import asyncio, websockets
async def test():
    async with websockets.connect('ws://127.0.0.1:8000/ws') as ws:
        print('connected')
        await ws.recv()
asyncio.run(test())
