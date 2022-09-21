from typing import Optional

from pydantic import BaseModel


class Token(BaseModel):
    """Pydantic Model that will be used in the token endpoint for the response.
    """
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None
