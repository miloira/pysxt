import asyncio
import hmac
import hashlib
import io
import json
import base64
import random
import time
import typing

import httpx
import websockets

from PIL import Image
from pyee.asyncio import AsyncIOEventEmitter
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

from . import send_type
from .logger import logger


def aes_ecb_encrypt(key: str, plaintext: str) -> str:
    cipher = AES.new(key.encode(), AES.MODE_ECB)
    ciphertext = cipher.encrypt(pad(plaintext.encode(), AES.block_size))
    return base64.urlsafe_b64encode(ciphertext).decode()


class SXTWebSocketClient:
    def __init__(
        self,
        user_id: str,
        seller_id: str,
        ws_uri: str = "wss://zelda.xiaohongshu.com/websocketV2",
        app_id: str = "647e8f23d15d890d5cc02700",
        token: str = "7f54749ef19aaf9966ed7a616982c016bda5dfba",
        app_name: str = "walle-ad",
        app_version: str = "0.9.1",
        connect_retry_interval: int = 3
    ):
        self.user_id = user_id
        self.seller_id = seller_id
        self.ws_uri = ws_uri
        self.app_id = app_id
        self.token = token
        self.app_name = app_name
        self.app_version = app_version
        self.connect_retry_interval = connect_retry_interval
        self.sxt = None
        self.websocket = None
        self.seq = 0

    def attach(self, sxt: "SXT") -> None:
        self.sxt = sxt

    async def increase_seq(self) -> int:
        self.seq += 1
        return self.seq

    async def ws_send(self, data: dict) -> None:
        if self.websocket:
            data["seq"] = await self.increase_seq()
            await self.websocket.send(json.dumps(data))
            logger.debug(f"> Sent: {data}")

    async def handle_message(self, server_message: dict) -> None:
        msg_type = server_message.get("type")

        match msg_type:
            case 2:  # 服务器要求 ACK
                await self.ws_send({"type": 130, "ack": server_message["seq"]})
                if self.sxt is not None:
                    if server_message["data"]["type"] == "PUSH_SIXINTONG_MSG":
                        self.sxt.event_emitter.emit(server_message["data"]["payload"]["sixin_message"]["message_type"],
                                                    self.sxt, server_message)
            case 4:
                await self.ws_send({"type": 132})
                await self.ws_send({"type": 4})
            case 129:  # 服务器返回 secureKey
                await self.ws_send({
                    "type": 10,
                    "topic": aes_ecb_encrypt(server_message["secureKey"], self.user_id),
                    "encrypt": True
                })
            case 132:  # 服务器心跳
                await asyncio.sleep(60)
                await self.ws_send({"type": 4})
            case 138:  # 服务器请求 userAgent & additionalInfo
                await self.ws_send({
                    "type": 12,
                    "data": {
                        "userAgent": {"appName": self.app_name, "appVersion": self.app_version},
                        "additionalInfo": {
                            "userId": self.user_id,
                            "sellerId": self.seller_id
                        }
                    }
                })
            case 140:
                await asyncio.sleep(30)
                await self.ws_send({"type": 4})
            case _:
                logger.warning(f"Unknown msg type: {msg_type} server message: {server_message}")

    async def connect(self) -> typing.NoReturn:
        while True:
            try:
                async with websockets.connect(self.ws_uri) as self.websocket:
                    logger.debug("[Connected] WebSocket connection established.")

                    await self.ws_send({
                        "type": 1,
                        "token": self.token,
                        "appId": self.app_id
                    })

                    while True:
                        response = await self.websocket.recv()
                        server_message = json.loads(response)
                        logger.debug(f"< Received: {server_message}")
                        asyncio.create_task(self.handle_message(server_message))

            except websockets.exceptions.ConnectionClosed:
                logger.debug("[Error] Connection closed, reconnecting in 3s...")
                self.seq = 0
                await asyncio.sleep(self.connect_retry_interval)

    async def close(self):
        await self.websocket.close()


class SXT:
    def __init__(
        self,
        cookies: dict,
        platform: int = 1,
        contact_way="octopus",
        timeout: int = 60,
        websocket_client_config: typing.Optional[dict] = None
    ):
        self.base_url = "https://sxt.xiaohongshu.com/api-sxt/edith"
        self.headers = {
            "authority": "sxt.xiaohongshu.com",
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
        self.platform = platform
        self.contact_way = contact_way
        self.timeout = timeout
        self.websocket_client_config = websocket_client_config or {}
        self.event_emitter = AsyncIOEventEmitter()
        self.client = httpx.AsyncClient(cookies=self.cookies, headers=self.headers, timeout=self.timeout)
        self.user_info = self.get_user_info()
        self.c_user_id = self.user_info["data"]["c_user_id"]
        self.b_user_id = self.user_info["data"]["b_user_id"]
        self.account_no = self.user_info["data"]["account_no"]
        self.user_detail = self.get_user_detail(self.account_no)
        self.seller_id = self.user_detail["data"]["flow_user"]["cs_provider_id"]
        self.websocket_client = SXTWebSocketClient(user_id=self.b_user_id, seller_id=self.seller_id,
                                                   **self.websocket_client_config)
        self.websocket_client.attach(self)

    @classmethod
    def generate_uuid(cls) -> str:
        timestamp = int(time.time() * 1000)
        random_number = random.randint(10000000, 99999999)
        return f"{timestamp}-{random_number}"

    @classmethod
    def get_image_size(cls, image_data: bytes) -> tuple[int, int]:
        with Image.open(io.BytesIO(image_data)) as img:
            return img.size

    def hmac_sha1(self, key: str, content: str) -> str:
        return hmac.new(key.encode(), content.encode(), hashlib.sha1).hexdigest()

    async def get_upload_token(self, biz_name: str, scene: str, file_count: str = "1", version: str = "1",
                               source: str = "web") -> dict:
        url = self.base_url + "/uploader/v3/token"
        params = {
            "biz_name": biz_name,
            "scene": scene,
            "file_count": file_count,
            "version": version,
            "source": source,
        }
        response = await self.client.get(url, params=params)
        return response.json()

    def make_q_signature(self, start_time: int, expire_time: int, file_id: str, file_size: int) -> str:
        C = self.hmac_sha1("null", f"{start_time};{expire_time}")
        x = hashlib.sha1(
            f'put\n/{file_id}\n\ncontent-length={file_size}&host=ros-upload.xiaohongshu.com\n'.encode()).hexdigest()
        k = f"sha1\n{start_time};{expire_time}\n{x}\n"
        return self.hmac_sha1(C, k)

    async def upload_file(self, file_path: str) -> dict:
        biz_name = "cs"
        scene = "feeva_img"
        upload_token = await self.get_upload_token(biz_name, scene)
        upload_temp_permit = upload_token["data"]["upload_temp_permits"][0]
        file_id = upload_temp_permit["file_ids"][0]
        expire_time = int(upload_temp_permit["expire_time"] / 1000)
        start_time = expire_time - 86400

        with open(file_path, "rb") as f:
            data = f.read()

        file_size = len(data)
        width, height = self.get_image_size(data)
        url = f"https://ros-upload.xiaohongshu.com/{file_id}"
        headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9",
            "authorization": f"q-sign-algorithm=sha1&q-ak=null&q-sign-time={start_time};{expire_time}&q-key-time={start_time};{expire_time}&q-header-list=content-length;host&q-url-param-list=&q-signature={self.make_q_signature(start_time, expire_time, file_id, file_size)}",
            "cache-control": "",
            "content-type": "image/png",
            "content-length": str(file_size),
            "origin": "https://sxt.xiaohongshu.com",
            "pragma": "no-cache",
            "priority": "u=1, i",
            "referer": "https://sxt.xiaohongshu.com/",
            "sec-ch-ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "x-cos-security-token": upload_token["data"]["upload_temp_permits"][0]["token"]
        }
        response = await self.client.put(url, headers=headers, content=data)
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

    def get_user_info(self) -> dict:
        url = self.base_url + "/ads/user/info"
        response = httpx.get(url, headers=self.headers, cookies=self.cookies)
        return response.json()

    def get_user_detail(self, account_no: str) -> dict:
        url = self.base_url + "/pc/flow/user/detail"
        params = {
            "account_no": account_no,
            "contact_way": self.contact_way
        }
        response = httpx.get(url, headers=self.headers, cookies=self.cookies, params=params)
        return response.json()

    async def search_chats(self, query: str) -> dict:
        url = self.base_url + "/chatline/chat/search"
        params = {
            "porch_user_id": self.b_user_id
        }
        data = {
            "query": query,
            "key_type": 2
        }
        response = await self.client.post(url, params=params, json=data)
        return response.json()

    async def get_chats(self, is_active: str = "true", limit: int = 80) -> dict:
        url = self.base_url + "/chatline/chat"
        params = {
            "porch_user_id": self.b_user_id,
            "limit": str(limit),
            "is_active": True if is_active == "true" else False
        }
        response = await self.client.get(url, params=params)
        return response.json()

    async def get_chat_messages(self, customer_user_id: str, limit: int = 20) -> dict:
        url = self.base_url + "/chatline/msg"
        params = {
            "porch_user_id": self.b_user_id,
            "customer_user_id": customer_user_id,
            "limit": str(limit)
        }
        response = await self.client.get(url, params=params)
        return response.json()

    async def read_chat(self, chat_user_id: str) -> dict:
        url = self.base_url + "/chatline/chat/message/read"
        params = {
            "chat_user_id": chat_user_id
        }
        response = await self.client.get(url, params=params)
        return response.json()

    async def switch_status(self, status: typing.Literal["online", "rest", "offline"]) -> dict:
        url = self.base_url + "/pc/chatline/user/switch_status"
        data = {
            "status": status,
            "account_no": self.account_no,
            "contact_way": self.contact_way
        }
        response = await self.client.post(url, json=data)
        return response.json()

    async def get_notification_settings(self) -> dict:
        url = self.base_url + "/ads/user/gray-scale/check"
        response = await self.client.post(url)
        return response.json()

    async def get_blacklist(self, start_time: typing.Optional[str] = None,
                            end_time: typing.Optional[str] = None,
                            customer_user_id: typing.Optional[str] = None,
                            page_index: int = 1,
                            page_size: int = 10) -> dict:
        url = self.base_url + "/chatline/blacklist/list"
        params = {
            "page_index": str(page_index),
            "page_size": str(page_size)
        }
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time
        if customer_user_id is not None:
            params["customer_user_id"] = customer_user_id
        response = await self.client.get(url, params=params)
        return response.json()

    async def add_blacklist(self, customer_user_id: str, black_type: str = "1") -> dict:
        url = self.base_url + "/chatline/blacklist/add"
        data = {
            "black_type": black_type,
            "customer_user_id": customer_user_id
        }
        response = await self.client.post(url, json=data)
        return response.json()

    async def get_session_list(self, state: typing.Literal["PROCESSING", "ENDED"],
                               source_user_id: typing.Optional[str] = "",
                               grantor_user_id: typing.Optional[str] = "",
                               customer_user_id: typing.Optional[str] = None,
                               session_id: typing.Optional[str] = None,
                               begin_time: typing.Optional[str] = None,
                               end_time: typing.Optional[str] = None,
                               seller_id: typing.Optional[str] = None,
                               page: int = 1,
                               limit: int = 10) -> dict:
        url = self.base_url + "/chatline/case-manage/list"
        params = {
            "state": state,
            "csa_no": self.account_no,
            "source_user_id": source_user_id,
            "grantor_user_id": grantor_user_id,
            "limit": str(limit),
            "page": str(page),
        }
        if customer_user_id is not None:
            params["customer_user_id"] = customer_user_id
        if session_id is not None:
            params["session_id"] = session_id
        if begin_time is not None:
            params["begin_time"] = begin_time
        if end_time is not None:
            params["end_time"] = end_time
        if seller_id is not None:
            params["seller_id"] = seller_id
        response = await self.client.get(url, params=params)
        return response.json()

    async def get_business_cards(self, page_num: int = 1, page_size: int = 10) -> dict:
        url = self.base_url + "/chatline/business_card/list"
        params = {
            "page_num": page_num,
            "page_size": page_size
        }
        response = await self.client.get(url, params=params)
        return response.json()

    async def search_notes(self, search_text: typing.Optional[str] = None, source: str = "1",
                           page_no: int = 1,
                           page_size: int = 10) -> dict:
        url = self.base_url + "/ads/note/list"
        params = {
            "seller_id": self.seller_id,
            "page_no": str(page_no),
            "page_size": str(page_size),
            "source": source
        }
        if search_text is not None:
            params["search_text"] = search_text
        response = await self.client.get(url, params=params)
        return response.json()

    async def send(self, receiver_id: str, content: str, message_type: str) -> dict:
        url = self.base_url + "/chatline/msg"
        params = {
            "porch_user_id": self.b_user_id
        }
        data = {
            "sender_porch_id": self.b_user_id,
            "receiver_id": receiver_id,
            "content": content,
            "message_type": message_type,
            "uuid": self.generate_uuid(),
            "c_user_id": self.c_user_id,
            "platform": self.platform
        }
        response = await self.client.post(url, params=params, json=data)
        return response.json()

    async def send_text(self, receiver_id: str, content: str) -> dict:
        return await self.send(receiver_id, content, send_type.TEXT)

    async def send_image(self, receiver_id: str, file_path: str) -> dict:
        return await self.send(receiver_id,
                               json.dumps(await self.upload_file(file_path), ensure_ascii=False),
                               send_type.IMAGE)

    async def send_note(self, receiver_id: str, note_id: str) -> dict:
        return await self.send(receiver_id, note_id, send_type.NOTE)

    async def send_card(self, receiver_id: str, card: str) -> dict:
        return await self.send(receiver_id, card, send_type.CARD)

    def handle(self, message_type: str) -> typing.Callable[[typing.Callable], None]:
        def wrapper(f):
            self.event_emitter.on(message_type, f)

        return wrapper

    async def listen(self) -> typing.NoReturn:
        logger.info(
            f'[{self.user_detail["data"]["flow_user"]["status"]}] {self.user_detail["data"]["flow_user"]["name"]}')
        logger.info("Message listening...")
        await self.websocket_client.connect()

    def run(self) -> typing.NoReturn:
        asyncio.run(self.listen())
