import asyncio
import copy
import hmac
import hashlib
import io
import json
import base64
import random
import time

from PIL import Image
from pyee.asyncio import AsyncIOEventEmitter
import websockets
import requests

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

from pysxt import send_type, message_type

cookies = {
    "abRequestId": "bc611ec4-47ed-5223-bc91-0bbfba80cc65",
    "a1": "1959936162fv967jtfc0iv00099nyp3ohfs92rtao50000422162",
    "webId": "4eaef2bdf4d12a9eeacb5d2b8b2c669e",
    "web_session": "030037a0e619232603531b0bad204a70fd6028",
    "gid": "yj2jjqKJfDxfyj2jjqKyKiF9JihjKWV9iS83M0lh888jjj28IvUqlJ8884JJyKJ8Jj4yi2yS",
    "loadts": "1742032816314",
    "xsecappid": "walle-ad",
    "x-user-id-sxt.xiaohongshu.com": "67bc150804f0000000000003",
    "customerClientId": "153952729589630",
    "acw_tc": "0a42278617424791581381333e22c8828e8c247777984dd90e6bf914cccfcd",
    "websectiga": "cffd9dcea65962b05ab048ac76962acee933d26157113bb213105a116241fa6c",
    "sec_poison_id": "e3920332-888f-4d5f-8954-3383946cc262",
    "customer-sso-sid": "68c517483891070912775170jsprrcg0nptebruh",
    "access-token-sxt.xiaohongshu.com": "customer.sxt.AT-68c517483891070912775173wndbrtlvszckosbb"
}


# AES-ECB 加密
def aes_ecb_encrypt(key: str, plaintext: str) -> str:
    cipher = AES.new(key.encode(), AES.MODE_ECB)
    ciphertext = cipher.encrypt(pad(plaintext.encode(), AES.block_size))
    return base64.urlsafe_b64encode(ciphertext).decode()


class WebSocketClient:
    WS_URI = "wss://zelda.xiaohongshu.com/websocketV2"
    APP_NAME = "walle-ad"
    APP_VERSION = "0.7.1"

    def __init__(self, sxt, app_id, user_id, seller_id, token):
        self.sxt: SXT = sxt
        self.app_id = app_id
        self.user_id = user_id
        self.seller_id = seller_id
        self.token = token
        self.seq = 0
        self.lock = asyncio.Lock()
        self.websocket = None

    async def increase_seq(self) -> int:
        async with self.lock:
            self.seq += 1
            return self.seq

    async def ws_send(self, data):
        if self.websocket:
            data["seq"] = await self.increase_seq()
            await self.websocket.send(json.dumps(data))
            print(f"> Sent: {data}\n")

    async def handle_message(self, server_message):
        msg_type = server_message.get("type")

        match msg_type:
            case 2:  # 服务器要求 ACK
                await self.ws_send({"type": 130, "ack": server_message["seq"]})
                if server_message["data"]["type"] == "PUSH_SIXINTONG_MSG":
                    self.sxt.event_emitter.emit(server_message["data"]["payload"]["sixin_message"]["message_type"],
                                                self.sxt, server_message)
            case 129:  # 服务器返回 secureKey
                next_message = {
                    "type": 10,
                    "topic": aes_ecb_encrypt(server_message["secureKey"], self.user_id),
                    "encrypt": True
                }
                await self.ws_send(next_message)
            case 138:  # 服务器请求 userAgent & additionalInfo
                next_message = {
                    "type": 12,
                    "data": {
                        "userAgent": {"appName": self.APP_NAME, "appVersion": self.APP_VERSION},
                        "additionalInfo": {
                            "userId": self.user_id,
                            "sellerId": self.seller_id
                        }
                    }
                }
                await self.ws_send(next_message)
                await self.ws_send({"type": 4})  # 发送心跳
            case 132:  # 服务器心跳
                await asyncio.sleep(60)
                await self.ws_send({"type": 4})

    async def websocket_connect(self):
        while True:
            try:
                async with websockets.connect(self.WS_URI) as self.websocket:
                    print("[Connected] WebSocket connection established.")

                    await self.ws_send({
                        "type": 1,
                        "token": self.token,
                        "appId": self.app_id
                    })

                    while True:
                        response = await self.websocket.recv()
                        server_message = json.loads(response)
                        print(f"< Received: {server_message}\n")
                        asyncio.create_task(self.handle_message(server_message))

            except websockets.exceptions.ConnectionClosed:
                print("[Error] Connection closed, reconnecting in 3s...")
                await asyncio.sleep(3)
            except asyncio.CancelledError:
                print("[Cancelled] WebSocket task cancelled.")
                break


class SXT:

    def __init__(self, cookies):
        self.event_emitter = AsyncIOEventEmitter()
        self.headers = {
            # "authority": "sxt.xiaohongshu.com",
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "referer": "https://sxt.xiaohongshu.com/im/multiCustomerService?uba_pre=115.login..1742479176655&uba_ppre=115.home..1742479162478&uba_index=3",
            "sec-ch-ua": "\"Google Chrome\";v=\"107\", \"Chromium\";v=\"107\", \"Not=A?Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
            "x-subsystem": "sxt"
        }
        self.cookies = cookies
        self.user_id = self.cookies["x-user-id-sxt.xiaohongshu.com"]
        self.info = self.get_info()
        self.c_user_id = self.info["data"]["c_user_id"]
        self.platform = 1
        self.client = WebSocketClient(
            sxt=self,
            app_id="647e8f23d15d890d5cc02700",
            user_id="67bc150804f0000000000003",
            seller_id="6698b21b3289650015d6f4df",
            token="7f54749ef19aaf9966ed7a616982c016bda5dfba"
        )

    @classmethod
    def generate_uuid(cls):
        timestamp = int(time.time() * 1000)
        random_number = random.randint(10000000, 99999999)
        return f"{timestamp}-{random_number}"

    def get_info(self):
        url = "https://sxt.xiaohongshu.com/api-sxt/edith/ads/user/info"
        response = requests.get(url, headers=self.headers, cookies=self.cookies)
        return response.json()

    def get_chats(self, is_active="true", limit="80"):
        url = "https://sxt.xiaohongshu.com/api-sxt/edith/chatline/chat"
        params = {
            "porch_user_id": self.user_id,
            "limit": limit,
            "is_active": is_active
        }
        response = requests.get(url, headers=self.headers, cookies=self.cookies, params=params)
        return response.json()

    def get_chat_messages(self, customer_user_id, limit="20"):
        url = "https://sxt.xiaohongshu.com/api-sxt/edith/chatline/msg"
        params = {
            "porch_user_id": self.user_id,
            "customer_user_id": customer_user_id,
            "limit": limit
        }
        response = requests.get(url, headers=self.headers, cookies=self.cookies, params=params)
        return response.json()

    def read_chat(self, chat_user_id):
        url = "https://sxt.xiaohongshu.com/api-sxt/edith/chatline/chat/message/read"
        params = {
            "chat_user_id": chat_user_id
        }
        response = requests.get(url, headers=self.headers, cookies=self.cookies, params=params)
        return response.json()

    def send(self, receiver_id, content, message_type):
        url = "https://sxt.xiaohongshu.com/api-sxt/edith/chatline/msg"
        params = {
            "porch_user_id": self.user_id
        }
        data = {
            "sender_porch_id": self.user_id,
            "receiver_id": receiver_id,
            "content": content,
            "message_type": message_type,
            "uuid": self.generate_uuid(),
            "c_user_id": self.c_user_id,
            "platform": self.platform
        }
        response = requests.post(url, headers=self.headers, cookies=self.cookies, params=params, json=data)
        return response.json()

    def send_text(self, receiver_id, content):
        return self.send(receiver_id, content, send_type.TEXT)

    def hmac_sha1(self, key, content):
        return hmac.new(key.encode(), content.encode(), hashlib.sha1).hexdigest()

    def get_q_sign_time(self):
        pass

    def get_q_key_time(self):
        pass

    def get_C(self, m, r):
        return self.hmac_sha1(r, m)

    def get_q_signature(self, start_time, expire_time, file_id, file_size):
        C = self.hmac_sha1("null", f"{start_time};{expire_time}")
        print(C)
        x = hashlib.sha1(
            f'put\n/rimmatrix/{file_id}\n\ncontent-length={file_size}&host=ros-upload.xiaohongshu.com\n'.encode()).hexdigest()
        k = f"sha1\n{start_time};{expire_time}\n{x}\n"
        return self.hmac_sha1(C, k)

    def get_upload_token(self, biz_name, scene, file_count="1", version="1", source="web"):
        url = "https://sxt.xiaohongshu.com/api-sxt/edith/uploader/v3/token"
        params = {
            "biz_name": biz_name,
            "scene": scene,
            "file_count": file_count,
            "version": version,
            "source": source,
        }
        headers = copy.copy(self.headers)
        headers.update({
            "x-b3-traceid": "16f634575954c7e1",
            "x-subsystem": "sxt",
        })
        response = requests.get(url, headers=headers, cookies=cookies, params=params)
        return response.json()

    @classmethod
    def get_image_size(cls, image_data):
        with Image.open(io.BytesIO(image_data)) as img:
            return img.size

    def upload_file(self, file_path):
        biz_name = "cs"
        scene = "feeva_img"
        upload_token = self.get_upload_token(biz_name, scene)
        upload_temp_permit = upload_token["data"]["upload_temp_permits"][0]
        file_id = upload_temp_permit["file_ids"][0]
        expire_time = int(upload_temp_permit["expire_time"] / 1000)
        start_time = expire_time - 86400

        with open(file_path, "rb") as f:
            data = f.read()

        width, height = self.get_image_size(data)
        url = f"https://ros-upload.xiaohongshu.com/rimmatrix/{file_id}"
        C = self.hmac_sha1("null", f"{start_time};{expire_time}")
        x = hashlib.sha1(
            f'put\n/rimmatrix/{file_id}\n\ncontent-length={len(data)}&host=ros-upload.xiaohongshu.com\n'.encode()).hexdigest()
        k = f"sha1\n{start_time};{expire_time}\n{x}\n"
        headers = copy.copy(self.headers)
        headers.update({
            "content-type": "image/png",
            "content-length": str(len(data)),
            "host": "ros-upload.xiaohongshu.com",
            "authorization": f"q-sign-algorithm=sha1&q-ak=null&q-sign-time={start_time};{expire_time}&q-key-time={start_time};{expire_time}&q-header-list=content-length;host&q-url-param-list=&q-signature={self.hmac_sha1(C, k)}",
            "x-cos-security-token": upload_token["data"]["upload_temp_permits"][0]["token"]
        })
        response = requests.put(url, headers=headers, data=data)
        return {
            "link": {
                "cloudType": upload_temp_permit["cloud_type"], "bizName": biz_name, "scene": scene,
                "fileId": upload_temp_permit["file_ids"][0],
                "preViewUrl": response.headers["X-Ros-Preview-Url"]
            },
            "size": {
                "width": width,
                "height": height
            }
        }

    def send_image(self, receiver_id, file_path):
        data = self.upload_file(file_path)
        return self.send(receiver_id, json.dumps(data, separators=(",", ":"), ensure_ascii=False), send_type.IMAGE)

    def send_note(self, receiver_id, note_id):
        return self.send(receiver_id, note_id, send_type.NOTE)

    async def listen(self):
        await self.client.websocket_connect()

    def handle(self, message_type):
        def wrapper(f):
            self.event_emitter.on(message_type, f)

        return wrapper

    def run(self):
        asyncio.run(self.listen())

if __name__ == '__main__':
    sxt = SXT(cookies=cookies)
    print(sxt.get_q_signature("1743333182", "1743419581", "GfnQkNpBVb63ik9GJeSNKgy5VDYE3dZInaurqzjgaDM3Pns", 93238))

    # @sxt.handle(message_type.TEXT)
    # async def on_message(bot, event):
    #     print(event)
    #     # bot.send_text(event["data"]["payload"]["sixin_message"]["sender_id"], "你好")
    #     bot.send_image(event["data"]["payload"]["sixin_message"]["sender_id"], "test.png")
    #
    #
    # sxt.run()
