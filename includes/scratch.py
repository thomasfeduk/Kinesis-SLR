from includes.debug import *


def serialize(obj):
    if isinstance(obj, (str, int, float, bool)):
        return obj
    elif isinstance(obj, list):
        return [serialize(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: serialize(value) for key, value in obj.items()}
    elif hasattr(obj, '__dict__'):
        return serialize({k: v for k, v in vars(obj).items() if isinstance(v, property)})
    return str(obj)


#####################################################
#####################################################
#####################################################
#####################################################
#####################################################




def __json_encode__(self):
    return {"name": self.name, "age": self.age}


def to_json(self):
    return json.dumps(self, default=lambda o: o.__json_encode__())


def __str__(self):
    return self.to_json()

#####################################################
#####################################################
#####################################################
#####################################################
#####################################################


class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def __repr__(self):
        return f"Person({self.name}, {self.age})"

    def __eq__(self, other):
        if isinstance(other, Person):
            return self.name == other.name and self.age == other.age
        return False

    def __json_encode__(self):
        return {"name": self.name, "age": self.age}

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__json_encode__())

    def __str__(self):
        return self.to_json()

person = Person("Alice", 30)
json_string = str(person)
print(json_string)

die()

