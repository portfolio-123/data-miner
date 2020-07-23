import p123.data.validate as validation
import p123.data.transform as transform
import p123.data.cons as cons
import p123.mapping.init as mapping_init
import utils.misc as misc
import functools


RANKS = {
    'Ranking System': {
        'field': 'rankingSystem',
        'isValid': validation.ranking_system,
        'transform': transform.screen_ranking,
        'required': True
    },
    'Universe': {
        'field': 'universe',
        'isValid': validation.universe,
        'transform': transform.universe
    },
    'As of Date': {
        'field': 'asOfDt',
        'isValid': validation.date,
        'transform': transform.date,
        'required': True
    },
    'Include Names': {
        'field': 'includeNames',
        'isValid': misc.is_bool
    },
    'Transaction Type': {
        'field': 'transType',
        'isValid': functools.partial(validation.from_mapping, mapping=cons.RANK_PERF_METHOD)
    },
    'Ranking Method': {
        'field': 'rankingMethod',
        'isValid': functools.partial(validation.from_mapping, mapping=cons.RANKING_METHOD),
        'transform': functools.partial(transform.from_mapping, mapping=cons.RANKING_METHOD)
    },
    # 'Industry': {
    #     'field': 'industry',
    #     'isValid': misc.is_str
    # },
    'Tickers': {
        'field': 'tickers',
        'isValid': misc.is_str
    },
    'Columns': {
        'isValid': functools.partial(validation.from_mapping, mapping=['ranks', 'composite', 'factor'])
    }
}
RANKS.update(mapping_init.SETTINGS)

RANKS_PERIOD = RANKS.copy()
del RANKS_PERIOD['As of Date'], RANKS_PERIOD['Columns']
RANKS_PERIOD['Start Date'] = {
    'isValid': validation.date,
    'transform': transform.date,
    'required': True
}
RANKS_PERIOD['End Date'] = {
    'isValid': validation.date,
    'transform': transform.date,
    'required': True
}
RANKS_PERIOD['Frequency'] = {
    'isValid': functools.partial(
        validation.from_mapping, mapping={item['label']: item['value'] for item in cons.FREQ[1:]}),
    'required': True
}


RANKS_MULTI_SETTINGS = RANKS.copy()
del RANKS_MULTI_SETTINGS['Ranking System'], RANKS_MULTI_SETTINGS['Ranking Method'], RANKS_MULTI_SETTINGS['Columns']
RANKS_MULTI_ITERATIONS = mapping_init.ITERATIONS.copy()
RANKS_MULTI_ITERATIONS['Ranking System'] = RANKS['Ranking System']
RANKS_MULTI_ITERATIONS['Ranking Method'] = RANKS['Ranking Method']
