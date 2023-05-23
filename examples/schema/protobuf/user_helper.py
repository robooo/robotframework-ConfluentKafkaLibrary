from user_pb2 import User
from robot.api.deco import keyword

@keyword("Get Type")
def get_type():
    return User

@keyword("Create User")
def create_user(name, number: int, serialize :bool=False):
    new_user = User(name = name, number = number)

    if serialize:
        return new_user.SerializeToString()

    return new_user
