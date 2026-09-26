"""`register_crud` over pyronova_m1f_items — for tests/test_review_m1_followup.py, whose
fixture creates the table before importing this module. Workers execute this module
too, so it only connects (idempotent, process-wide pool) and registers routes."""

import os

from pyronova import Pyronova
from pyronova.crud import register_crud
from pyronova.db import PgPool

TABLE = "pyronova_m1f_items"

pool = PgPool.connect(os.environ["PYRONOVA_TEST_PG_DSN"])

app = Pyronova()
register_crud(
    app, pool,
    prefix="/items",
    table=TABLE,
    columns=["id", "name", "quantity"],
)
