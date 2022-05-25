import jwt
from datetime import datetime, timedelta
from typing import Union, Any


def generate_token(username: Union[str, Any]):
    expire = datetime.utcnow() + timedelta(
        seconds=60 * 60 * 24 * 3
    )
    to_encode = {
        "exp": expire, "username": username
    }
    encoded_jwt = jwt.encode(to_encode, '123456', algorithm='HS256')
    return encoded_jwt