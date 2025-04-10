import json

from pysxt import SXT, message_type

bot = SXT(cookies={
    "access-token-sxt.xiaohongshu.com": "customer.sxt.AT-68c517491695954202880535minyl5brn9jn9srn"
})

@bot.handle(message_type.TEXT)
async def on_text_message(bot, event):
    business_cards = await bot.get_business_cards()
    print(await bot.send_card(event["data"]["payload"]["sixin_message"]["sender_id"], json.dumps({"type": "commercialBusinessCard", **business_cards["data"]["list"][0]})))

bot.run()