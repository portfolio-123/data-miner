import p123.data.validate as validation
import p123.data.transform as transform
import p123.mapping.init as init
import p123.data.cons as cons
import functools


FREQ = {item['label']: item['value'] for item in cons.FREQ[1:]}
SETTINGS = {
    'Start Date': {
        'field': 'startDt',
        'isValid': validation.date,
        'transform': transform.date,
        'required': True
    },
    'End Date': {
        'field': 'endDt',
        'isValid': validation.date,
        'transform': transform.date
    },
    'Frequency': {
        'field': 'frequency',
        'isValid': functools.partial(validation.from_mapping, mapping=FREQ),
        'transform': functools.partial(transform.from_mapping, mapping=FREQ)
    },
    'P123 UIDs': {
        'field': 'p123Uids',
        'isValid': validation.data_p123_uids,
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
SETTINGS.update(init.SETTINGS)

UNIVERSE_SETTINGS = {
    'As of Date': {
        'isValid': validation.data_univ_as_of_date,
        'required': True
    },
    'Universe': {
        'field': 'universe',
        'isValid': validation.universe,
        'transform': transform.universe,
        'required': True
    },
    'Formulas': {
        'isValid': validation.data_univ_formulas,
        'required': True
    }
}
UNIVERSE_SETTINGS.update(init.SETTINGS)

ITERATIONS = {
    'Formula': {
        'isValid': validation.data_formula,
        'required': True
    }
}
ITERATIONS.update(init.ITERATIONS)
