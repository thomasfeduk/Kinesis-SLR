from var_dump import var_dump


def pvd(data):
    var_dump(data)
    # var_dump(strip_proprules_recursively(data))


def pvdd(data):
    pvd(data)
    exit(0)


def strip_proprules_recursively(data):
    data_stripped = data

    # Strip the extensive verbose _proprules properly output by var_dump
    try:
        if getattr(data_stripped, '_proprules'):
            del data_stripped._proprules
    except AttributeError:
        pass

    try:
        for i in data_stripped.__dict__:
            setattr(data_stripped, i, strip_proprules_recursively(i))
    except AttributeError:
        pass
    return data_stripped


def die(msg: str = None):
    if msg is not None:
        print(msg)
    exit(0)
