"""App for tests/test_review_m1a.py::test_model_route_also_injects_path_params: `model=`
routes whose handlers also take path parameters. Its own module: a worker serves one
app per module."""

from pydantic import BaseModel

from pyronova import Pyronova


class Item(BaseModel):
    name: str


app = Pyronova()


@app.put("/items/{item_id}", model=Item)
def update(req, item: Item, item_id):
    return {"id": item_id, "name": item.name, "path": req.path}


@app.post("/tags/{tag}", model=Item)
async def tag(item: Item, tag):
    return {"tag": tag, "name": item.name}
