"""Users providers.

This module contains classes that implement an authentication procedure given
some user info, e.g. username and password, and return a user object of model
type :class:`User`.
"""

from abc import ABCMeta, abstractmethod

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext

from .config import SECRET_KEY, ALGORITHM
from ..models.auth import TokenData
from ..models.users import UserInDB, User

fake_users_db = {
    "johndoe": {
        "username": "user",
        "full_name": "John Doe",
        "email": "johndoe@example.com",
        "hashed_password": "$2b$12$RdpVTYRLlqBZWnSWBw3AXOKUyFsPZJfEiE8A9sSOOeQxSrMByY5Yq",
        "s3_endpoint": "minio:9000",
        "disabled": False,
    }
}


class UserProvider(metaclass=ABCMeta):
    """Base class for user providers.

    Parameters
    ----------
    source : dict
        Users source table.

    .. note::
        This class should not be used directly. Use derived classes instead.
    """

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    def __init__(self, source):
        self.source = source
        self.security_scheme = None

    @abstractmethod
    def load_user(self) -> User:
        pass

    def from_username(self, username: str, check_active: bool = True):
        if username in self.source:
            user_dict = self.source[username]
            user = UserInDB(**user_dict)
            if check_active:
                if user.disabled:
                    raise HTTPException(
                        status_code=400, detail="Inactive user")
                return user
            return user


class CredentialsProvider(UserProvider):
    """Credentials users provider.

    Provides user data from credentials (username and password).
    """

    def __init__(self):
        super().__init__(source=fake_users_db)

    def load_user(
            self,
            credentials: HTTPBasicCredentials = Depends(HTTPBasic())
    ):
        return self.authenticate(credentials)

    def authenticate(self, credentials: HTTPBasicCredentials):
        """Authenticates username and passwords.

        Password is verified against the stored hashed version. If the
        password does not match, returns an HTTP error right away.

        Parameters
        ----------
        credentials: HTTPBasicCredentials
            Credentials object.

        Returns
        -------
        user : User
        """
        hash_verifier = HashVerifier()
        user = self.from_username(credentials.username)
        if not hash_verifier.verify_password(
                credentials.password, user.hashed_password):
            raise self.credentials_exception
        return user


class TokenProvider(UserProvider):
    """Token users provider.

    Provide user data from access token.
    """

    def __init__(self):
        super().__init__(source=fake_users_db)

    def load_user(
            self,
            token: str = Depends(OAuth2PasswordBearer(tokenUrl="login"))
    ):
        return self.authenticate(token)

    def authenticate(self, token: str):
        """Decodes the received token, verifies it, and returns the user.

        If the token is invalid, return an HTTP error right away.

        Parameters
        ----------
        token: str
            Access token.

        Returns
        -------
        user : User
        """
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            username: str = payload.get("sub")
            if username is None:
                raise self.credentials_exception
            token_data = TokenData(username=username)
        except JWTError:
            raise self.credentials_exception
        user = self.from_username(username=token_data.username,
                                  check_active=True)
        if user is None:
            raise self.credentials_exception
        return user


class HashVerifier:
    def __init__(self):
        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

    def verify_password(self, plain_password, hashed_password):
        """Utility function to verify if a received password matches the hash
        stored.
        """
        return self.pwd_context.verify(plain_password, hashed_password)

    def get_password_hash(self, password):
        """Utility function to hash a password coming from the user.
        """
        return self.pwd_context.hash(password)
