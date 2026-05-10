"""Flask app exposing the social-stock-sentiment dashboard."""

from __future__ import annotations

import logging
import threading
import time

from flask import Flask, jsonify, render_template

import aggregator

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
LOG = logging.getLogger("app")

app = Flask(__name__)

CACHE_TTL_SEC = 5 * 60  # rescraping costs requests; cache 5 minutes
_cache: dict = {"data": None, "fetched_at": 0.0}
_lock = threading.Lock()


def _refresh_locked() -> dict:
    LOG.info("refreshing aggregated data")
    data = aggregator.run(top_n=20)
    _cache["data"] = data
    _cache["fetched_at"] = time.time()
    return data


def get_data(force: bool = False) -> dict:
    with _lock:
        if force or _cache["data"] is None or \
                (time.time() - _cache["fetched_at"]) > CACHE_TTL_SEC:
            return _refresh_locked()
        return _cache["data"]


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/trending")
def trending():
    data = get_data()
    return jsonify(data)


@app.route("/api/refresh", methods=["POST"])
def refresh():
    data = get_data(force=True)
    return jsonify(data)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
