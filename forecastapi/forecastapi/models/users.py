from typing import Optional

from pydantic import BaseModel


class User(BaseModel):
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    disabled: Optional[bool] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    s3_endpoint: Optional[str] = None


class UserInDB(User):
    hashed_password: str
