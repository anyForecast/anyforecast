from passlib.context import CryptContext

from ..models import UserInDB

fake_users_db = {
    "johndoe": {
        "username": "johndoe",
        "full_name": "John Doe",
        "email": "johndoe@example.com",
        "hashed_password": "$2b$12$a4GcDvUhphOAMpuGNNpUIutn/u81gvEBMrdOEfHgrqK.rteDHa22y",
        "access_key": "johndoe",
        "secret_key": "password",
        "s3_endpoint": "minio:9000",
        "disabled": False,
    }
}

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def get_user(username: str):
    if username in fake_users_db:
        user_dict = fake_users_db[username]
        return UserInDB(**user_dict)


def authenticate_user(username: str, password: str):
    """Utility function to authenticate and return a user.
    """
    user = get_user(username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user


def verify_password(plain_password, hashed_password):
    """Utility function to verify if a received password matches the hash
    stored.
    """
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    """Utility function to hash a password coming from the user.
    """
    return pwd_context.hash(password)
