import asyncio


class ResultStore:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.futures = {}

    async def put(self, idx, data):
        async with self.lock:
            future = self.futures.setdefault(idx, asyncio.Future())
            future.set_result(data)

    async def get(self, idx):
        async with self.lock:
            future = self.futures.setdefault(idx, asyncio.Future())
        data = await future
        async with self.lock:
            del self.futures[idx]
        return data
