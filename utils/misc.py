import datetime


def is_list(i):
    return isinstance(i, list)


def is_int(i):
    return isinstance(i, int)


def is_str(i):
    return isinstance(i, str)


def is_float(i):
    return isinstance(i, float)


def is_number(i):
    return isinstance(i, int) or isinstance(i, float)


def is_date(i):
    return isinstance(i, datetime.date)


def is_dict(i):
    return isinstance(i, dict)


def is_bool(b):
    return isinstance(b, bool)


def coalesce(var, default):
    return var if var is not None else default


def round2_or_none(val):
    if val is not None:
        val = round(val, 2)
    return val
