"""tests/test_compression.py's app with compression on. Its own module: a worker serves
one app per module. Compression settings are process-wide, so the suite imports this
module only when its fixture runs."""

from pyronova import Pyronova

from tests.apps.compression_routes import add_routes

app = Pyronova()
add_routes(app)
app.enable_compression(min_size=256)
