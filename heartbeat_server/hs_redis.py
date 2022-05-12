import aioredis
import asyncio


class Redis:

    def __init__(self, params):
        self.sub: aioredis.commands.Redis = None
        self.pub: aioredis.commands.Redis = None
        self.password = params['password'] \
            if 'password' in params else None
        self.url = params['server_url']
        self.pub_channel = params['pub_channel']
        self.sub_channel = params['sub_channel']

    async def get_redis_pool(self):
        if self.password:
            self.pub = await aioredis.create_redis_pool(
                self.url, password=self.password, timeout=100)
            self.sub = await aioredis.create_redis_pool(
                self.url, password=self.password, timeout=100)
        else:
            self.pub = await aioredis.create_redis_pool(
                self.url, timeout=100)
            self.sub = await aioredis.create_redis_pool(
                self.url, timeout=100)

        res = await self.sub.subscribe(self.sub_channel)
        asyncio.ensure_future(self.reader(res[0]))

    async def reader(self, channel: aioredis.Channel):
        while await channel.wait_message():
            msg = await channel.get_json()
            asyncio.ensure_future(self.handle_msg(msg))

    async def handle_msg(self, msg):
        print('Got Message:', msg)

    async def close(self):
        self.pub.close()
        self.sub.close()

    async def publish(self, message):
        await self.pub.publish(channel=self.pub_channel,
                               message=message)
