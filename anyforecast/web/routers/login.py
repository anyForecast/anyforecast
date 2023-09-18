from datetime import datetime, timedelta
from typing import Annotated, Optional

from fastapi import APIRouter, Depends
from fastapi.security import HTTPBasicCredentials
from fastapi.security import OAuth2PasswordRequestForm
from jose import jwt

from anyforecast.settings import conf
from ..auth import BasicAuth
from ..models import Token

router = APIRouter(prefix="/login", tags=["login"])


def create_access_token(
    data: dict, expires_delta: Optional[timedelta] = None
) -> str:
    """Returns access token.

    Parameters
    ----------
    data : dict
        Data to encode.

    expires_delta : timedelta, default=None
        Expiration time.
    """
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)

    to_encode.update({"exp": expire})

    token_settings = conf.get_token_settings()
    encoded_jwt = jwt.encode(to_encode, token_settings.key)
    return encoded_jwt


@router.post("/", response_model=Token)
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()]
):
    """User login."""
    credentials = HTTPBasicCredentials(
        username=form_data.username, password=form_data.password
    )

    user = BasicAuth().authenticate(credentials)
    access_token = create_access_token({"sub": user.username})

    return {"access_token": access_token, "token_type": "bearer"}
