"""Put the repo root on sys.path so `dot_shared` imports resolve in tests.

The image gets this from PYTHONPATH=/app; tests run with this directory as cwd.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
