from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer


if __name__ == "__main__":
    handler = partial(SimpleHTTPRequestHandler, directory="/data")
    ThreadingHTTPServer(("0.0.0.0", 8000), handler).serve_forever()
