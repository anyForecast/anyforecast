from fastapi import Depends, HTTPException, status, Header
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

from ._db_auth import get_user
from .jwt import SECRET_KEY, ALGORITHM
from ..models import User, TokenData

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")


async def jwt_auth(token: str = Depends(oauth2_scheme)):
    """Decode the received token, verify it, and return the current user.

    If the token is invalid, return an HTTP error right away.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = get_user(username=token_data.username)
    if user is None:
        raise credentials_exception
    return user


async def api_key_auth(api_key=Header(None)):
    return api_key


async def get_current_active_user(
        jwt: User = Depends(jwt_auth),
        api_key: User = Depends(api_key_auth)
):
    if not (jwt or api_key):
        raise HTTPException(401)

    current_user = jwt if jwt is not None else api_key
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user
