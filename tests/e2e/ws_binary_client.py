"""WebSocket binary test client.

Standalone driver — NOT a pytest test despite the `test_*` filename.
Paired with `test_ws_binary_server.py`: run the server, then
`python test_ws_binary_client.py <port>` in another shell, with the port from the
server's "Listening on" line. Renamed the
async function to `main` and added a `__main__` guard so pytest can
safely import this module during collection (previous module-level
`asyncio.run(test())` spawned live WebSocket traffic on import).
"""
import asyncio
import sys

import websockets


async def main(port: int) -> None:
    async with websockets.connect(f"ws://127.0.0.1:{port}/echo") as ws:
        # Text message
        await ws.send("hello")
        r = await ws.recv()
        assert r == "echo: hello", f"text failed: {r}"
        print(f"  ✅ text: {r}")

        # Binary message
        data = bytes([0, 1, 2, 3, 255])
        await ws.send(data)
        r = await ws.recv()
        assert r == data, f"binary failed: {r!r}"
        print(f"  ✅ binary: {len(r)} bytes, content={list(r)}")

        await ws.close()
        print("  ✅ closed")


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1])))
