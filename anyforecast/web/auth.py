from abc import ABC, abstractmethod
from typing import Any, Optional

from fastapi import HTTPException, status
from fastapi.security import HTTPBasicCredentials
from jose import JWTError, jwt
from passlib.context import CryptContext

from anyforecast.settings import conf

from .models import UserInDB

fake_users_db = {
    "user": {
        "username": "user",
        "full_name": "John Doe",
        "email": "johndoe@example.com",
        "hashed_password": "$2b$12$O0mUAiZNpbR2A6Bk7Fq2TOT1ODcXyl.ahU9vIFDQ8p0.4Bzn6vEn6",
        "disabled": False,
    }
}


class UserProvider:
    def __init__(self):
        self.db = fake_users_db

    def from_username(self, username: str) -> UserInDB:
        if username in self.db:
            user_dict = self.db[username]
            return UserInDB(**user_dict)


class PasswordHelper:
    """Container for password utility methods."""

    def __init__(self):
        self._pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

    def verify(self, plain_password: str, hashed_password: str) -> bool:
        """Verifies password against its hashed version.

        Parameters
        ----------
        plain_password : str
            Password to verify.

        hashed_password : str
            Hashed password used to verify.
        """
        return self._pwd_context.verify(plain_password, hashed_password)

    def hash(self, password) -> str:
        """Returns hashed password.

        Parameters
        ----------
        password : str
            Password to hash
        """
        return self._pwd_context.hash(password)


class Authenticator(ABC):
    """Base class for authenticators.

    .. note::
        This class should not be used directly. Use derived classes instead.
    """

    def __init__(self):
        self._user_provider = UserProvider()

    def get_user(self, username: str) -> UserInDB:
        return self._user_provider.from_username(username)

    @abstractmethod
    def authenticate(self, data: Any) -> UserInDB:
        """Authenticates data and returns user."""
        pass

    def raise_exception(self, detail: Optional[str] = None) -> None:
        """Raises HTTPException.

        Parameters
        ----------
        detail : str
            Message to include in HTTPException.

        Raises
        ------
        HTTPException
        """
        if detail is None:
            detail = "Could not validate credentials"

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            headers={"WWW-Authenticate": "Bearer"},
            detail=detail,
        )


class JWTAuth(Authenticator):
    """Authenticates user from access token."""

    def __init__(self):
        super().__init__()

    def authenticate(self, token: str) -> UserInDB:
        """Authenticates user from access token.

        Raises
        ------
        HTTPException when
        * Decoded payload subject ("sub") is None.
        * Username from decoded payload does not exist.
        * Decoding failed (JWTError).

        Parameters
        ----------
        token : str
            Token to authenticate.
        """

        try:
            username = self.decode_username(token)
        except JWTError:
            raise self.raise_exception()

        user = self.get_user(username)
        if user is None:
            self.raise_exception()

        return user

    def decode_username(self, token: str) -> str:
        token_settings = conf.get_token_settings()
        payload = jwt.decode(
            token, token_settings.key, algorithms=[token_settings.algorithm]
        )

        username: str = payload.get("sub")
        if username is None:
            raise self.raise_exception()
        return username


class BasicAuth(Authenticator):
    """Basic username and password authentication."""

    def __init__(self):
        super().__init__()
        self._passhelper = PasswordHelper()

    def verify_password(
        self, plain_password: str, hashed_password: str
    ) -> bool:
        return self._passhelper.verify(plain_password, hashed_password)

    def authenticate(self, credentials: HTTPBasicCredentials) -> UserInDB:
        """Authenticates username and password.

        Password is verified against the stored hashed version. If the
        password does not match, returns an HTTP error right away.

        Raises
        ------
        HTTPException when
        * Username does not exist.
        * Passwords do not match.

        Parameters
        ----------
        credentials: HTTPBasicCredentials, default=None
            Credentials object.
        """
        detail = "Incorrect username or password."

        user = self.get_user(credentials.username)
        if user is None:
            self.raise_exception(detail)

        if not self.verify_password(credentials.password, user.hashed_password):
            raise self.raise_exception(detail)

        return user
