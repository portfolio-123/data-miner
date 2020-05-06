import p123.data.validate as validation
import utils.misc as misc
import functools


MAIN = {
    'Operation': {
        'required': True
    },
    'On Error': {
        'isValid': functools.partial(validation.from_mapping, mapping=('stop', 'continue'))
    }
}

REQ_CONTEXT = {
    'Vendor': {
        'field': 'vendor',
        'isValid': functools.partial(validation.from_mapping, mapping=('factset', 'compustat'))
    },
    'PIT Method': {
        'field': 'pitMethod',
        'isValid': functools.partial(validation.from_mapping, mapping=('prelim', 'complete'))
    },
    'Rank Mon': {
        'field': 'rankMon',
        'isValid': misc.is_str
    }
}

SETTINGS = {
    'Type': {
        'isValid': functools.partial(validation.from_mapping, mapping=('stock', 'etf'))
    }
}
SETTINGS.update(REQ_CONTEXT)

ITERATIONS = {
    'Name': {
        'isValid': misc.is_str
    }
}
