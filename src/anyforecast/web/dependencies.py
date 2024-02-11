from typing import Annotated

from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer

from anyforecast.settings import AppInfo

from .auth import JWTAuth
from .models import User

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")


def get_app_info() -> AppInfo:
    """Returns application general information."""
    return AppInfo()


async def get_current_user(
    token: Annotated[str, Depends(oauth2_scheme)]
) -> User:
    """Authenticates token and returns user.

    Parameters
    ----------
    token : str
        JWT access token.

    Returns
    -------
    user : User
    """

    return JWTAuth().authenticate(token)


async def get_current_active_user(
    current_user: Annotated[User, Depends(get_current_user)]
) -> User:
    """Returns user only if is active.

    Raises
    ------
    HttpException if ``current_user`` is not active.

    Parameters
    ----------
    current_user : User

    Returns
    -------
    user : User
    """
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user
