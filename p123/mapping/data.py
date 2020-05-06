import p123.data.validate as validation
import p123.data.transform as transform
import p123.mapping.init as init
import p123.data.cons as cons
import utils.misc as misc
import functools


FREQ = {item['label']: item['value'] for item in cons.FREQ[1:]}
SETTINGS = {
    'Start Date': {
        'field': 'startDt',
        'isValid': misc.is_date,
        'transform': transform.date
    },
    'End Date': {
        'field': 'endDt',
        'isValid': misc.is_date,
        'transform': transform.date
    },
    'Frequency': {
        'field': 'frequency',
        'isValid': functools.partial(validation.from_mapping, mapping=FREQ),
        'transform': functools.partial(transform.from_mapping, mapping=FREQ)
    },
    'Mkt UIDs': {
        'field': 'mktUids',
        'isValid': validation.data_mkt_uids,
        'transform': transform.data_items
    },
    'Tickers': {
        'field': 'tickers',
        'isValid': validation.data_tickers_cusips,
        'transform': transform.data_items
    },
    'Cusips': {
        'field': 'cusips',
        'isValid': validation.data_tickers_cusips,
        'transform': transform.data_items
    }
}
SETTINGS.update(init.REQ_CONTEXT)

ITERATIONS = {
    'Formula': {
        'isValid': validation.data_formula,
        'required': True
    }
}
ITERATIONS.update(init.ITERATIONS)
