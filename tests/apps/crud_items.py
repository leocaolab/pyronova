"""`register_crud` over pyronova_crud_items — for tests/test_crud.py, whose fixture
creates the table before importing this module. Workers execute this module too, so it
only connects (idempotent, process-wide pool) and registers routes; no DDL here."""

import os

from pyronova import Pyronova
from pyronova.crud import register_crud
from pyronova.db import PgPool

pool = PgPool.connect(os.environ["PYRONOVA_TEST_PG_DSN"])

app = Pyronova()
register_crud(
    app, pool,
    prefix="/items",
    table="pyronova_crud_items",
    columns=["id", "name", "quantity"],
    id_column="id",
    id_type=int,
)


@app.get("/")
def root(req):
    return "ok"
