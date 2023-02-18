from datetime import datetime, timedelta
from typing import Optional

from jose import jwt

from .config import SECRET_KEY, ALGORITHM


class TokenGen:
    """Access token generator.
    """
    DEFAULT_EXPIRE_MINUTES = 30

    def generate_token(self, data: dict,
                       expires_delta: Optional[timedelta] = None):

        to_encode = data.copy()
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire_timedelta = timedelta(minutes=self.DEFAULT_EXPIRE_MINUTES)
            expire = datetime.utcnow() + expire_timedelta
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        return encoded_jwt
