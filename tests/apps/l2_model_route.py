"""App for tests/test_layer2_m3.py::test_model_route_still_validates: a `model=` route.
Its own module: a worker serves one app per module."""

import pydantic

from pyronova import Pyronova


class Item(pydantic.BaseModel):
    name: str
    qty: int


app = Pyronova()


@app.post("/items", model=Item)
def create(req, item):
    return {"name": item.name, "qty": item.qty}
