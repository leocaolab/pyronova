"""WebSocket binary test server."""
from pyronova import Pyronova

app = Pyronova()

@app.websocket("/echo")
def echo(ws):
    while True:
        msg = ws.recv_message()
        if msg is None:
            break
        if isinstance(msg, bytes):
            ws.send_bytes(msg)
        else:
            ws.send(f"echo: {msg}")

@app.get("/")
def index(req):
    return "ok"

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000)
