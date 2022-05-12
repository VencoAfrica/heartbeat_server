import aioredis


class Redis:

    def __init__(self, params):
        self.sub = None
        self.pub = None
        self.password = params['password'] if params['password'] else None
        self.url = params['server_url']
        self.channel = params['channel']
        self.redis_lock = params['redis_lock']

    async def get_redis_pool(self):
        async with self.redis_lock:
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
            return self.pub, await self.sub.subscribe(self.channel)

    async def close(self):
        self.pub.close()
        self.sub.close()

    async def publish(self, message):
        await self.pub.publish(self.channel, message)
