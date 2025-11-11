# pysxt
小红书私信通机器人开发框架
```python
from pysxt import SXT, message_type

bot = SXT(cookies={
    "access-token-sxt.xiaohongshu.com": "customer.sxt.AT-68c517491695954202880535minyl5brn9jn9srn"
})

@bot.handle(message_type.TEXT)
async def on_text_message(bot, event):
    print(event)

bot.run()
```
