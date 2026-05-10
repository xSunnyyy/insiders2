"""Vercel serverless entry point.

Vercel's @vercel/python builder picks up this file and wraps the WSGI
`app` object as a serverless function. The repo root is on sys.path
already (Vercel includes everything; tweak with includeFiles in
vercel.json if needed), so we can import the existing Flask app as-is.
"""

import os
import sys

# Make repo root importable so `from app import app` finds the top-level
# package even when Vercel runs us from /var/task.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app  # noqa: E402  -- Flask WSGI app

# Vercel looks for `app`, `application`, or `handler` at module level.
application = app
handler = app
