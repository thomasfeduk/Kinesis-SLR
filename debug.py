from var_dump import var_dump


def pvd(data):
    var_dump(data)


def pvdd(data):
    var_dump(data)
    exit(0)


def die(msg: str = None):
    if msg is not None:
        print(msg)
    exit(0)
