import os
from ._session import InferenceSession

API_ROOT = os.path.dirname(os.path.abspath(__file__))

__all__ = [
    'InferenceSession',
    'API_ROOT'
]
