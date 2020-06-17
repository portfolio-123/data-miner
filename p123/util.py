import logging
from p123api import Client
import statistics
import utils.misc as misc
import p123.mapping.init as mapping_init


def generate_params(*, data: dict, settings, api_client: Client, logger: logging.Logger):
    """
    Generate API compatible parameters dictionary from human readable ones
    :param data: data with meta info annotations
    :param settings: misc settings dictionary (like 'Type')
    :param api_client: API client (some transform functions may require it)
    :param logger:
    :return: dict
    """
    params = {}
    for entry in data.values():
        if not misc.is_dict(entry) or 'meta_info' not in entry:
            continue
        meta_info = entry['meta_info']
        value = entry['value']
        if 'transform' in meta_info:
            value = meta_info['transform'](value=entry['value'], settings=settings, api_client=api_client)
        if value is None:
            return
        target = params
        if meta_info.get('type') == 'screen':
            if 'screen' not in params:
                params['screen'] = {'type': settings['Type']}
            elif not misc.is_dict(params['screen']):
                logger.error('Invalid screen (mixing definition and ID)')
                return
            target = params['screen']
        target[meta_info['field']] = value
    return params


def update_iter_params(init_params: dict, iter_params: dict):
    params = init_params.copy()

    # make sure we don't change init screen param
    screen = params.get('screen')
    if isinstance(screen, dict):
        params['screen'] = screen.copy()

    # handle screen param and remove it
    screen = iter_params.get('screen')
    if 'screen' in iter_params:
        if isinstance(params.get('screen'), dict) and isinstance(screen, dict):
            params['screen'].update(screen)
        else:
            params['screen'] = screen
        del iter_params['screen']

    # update rest of params
    params.update(iter_params)

    return params


def process_screen_rolling_backtest_result(json: dict, start_dt, end_dt):
    length = len(json['rows'])
    data = [start_dt, end_dt, length]
    data.extend(round(val, 2) for val in json['average'][4:8])
    data.append(round(min(float(item[8]) for item in json['rows']), 2))
    data.append(round(max(float(item[9]) for item in json['rows']), 2))
    data.append(round(json['average'][10], 2))
    data.append(round(statistics.fmean(float(item[5]) for item in json['rows'][0:12]), 2) if length >= 12 else None)
    data.append(round(statistics.fmean(float(item[5]) for item in json['rows'][0:60]), 2) if length >= 60 else None)
    return data


def process_rank_perf_result(json: dict, rows: list, port_mode: bool = True):
    port_or_bench = 'port' if port_mode else 'bench'

    # one results iteration to rule them all
    per_outperform_cnt = 0
    ret_key = 8 if port_mode else 9
    max_loss = float(json['results']['rows'][0][ret_key])
    max_gain = float(json['results']['rows'][0][ret_key])
    max_loss_single_stock = float(json['results']['rows'][0][15])
    max_gain_single_stock = float(json['results']['rows'][0][16])
    for result_row in json['results']['rows']:
        per_outperform_cnt += 1 if float(result_row[10]) > 0 else 0
        max_loss = min(max_loss, float(result_row[ret_key]))
        max_gain = max(max_gain, float(result_row[ret_key]))
        max_loss_single_stock = min(max_loss_single_stock, float(result_row[15]))
        max_gain_single_stock = max(max_gain_single_stock, float(result_row[16]))

    idx = -1

    # Annualized return
    idx += 1
    rows[idx].append(misc.round2_or_none(json['stats'][port_or_bench]['annualized_return']))

    # Average excess return
    idx += 1
    rows[idx].append(
        misc.round2_or_none(json['stats']['port']['annualized_return'] - json['stats']['bench']['annualized_return'])
        if port_mode else None
    )

    # Total return
    idx += 1
    rows[idx].append(misc.round2_or_none(json['stats'][port_or_bench]['total_return']))

    # % of periods strategy outperforms
    idx += 1
    rows[idx].append(round(per_outperform_cnt / len(json['results']['rows']) * 100, 2) if port_mode else None)

    # Max gain
    idx += 1
    rows[idx].append(round(max_gain, 2))

    # Max loss
    idx += 1
    rows[idx].append(round(max_loss, 2))

    # Max gain single stock
    idx += 1
    rows[idx].append(round(max_gain_single_stock, 2) if port_mode else None)

    # Max loss single stock
    idx += 1
    rows[idx].append(round(max_loss_single_stock, 2) if port_mode else None)

    # Avg excess return in Up markets
    idx += 1
    rows[idx].append(misc.round2_or_none(json['results']['upMarkets'][10]) if port_mode else None)

    # Avg Excess Return in Down Markets
    idx += 1
    rows[idx].append(misc.round2_or_none(json['results']['downMarkets'][10]) if port_mode else None)

    # Sharpe
    idx += 1
    rows[idx].append(misc.round2_or_none(json['stats'][port_or_bench].get('sharpe_ratio')))

    # Sortino
    idx += 1
    rows[idx].append(misc.round2_or_none(json['stats'][port_or_bench].get('sortino_ratio')))

    # StdDev
    idx += 1
    rows[idx].append(misc.round2_or_none(json['stats'][port_or_bench].get('standard_dev')))

    # Max Drawdown
    idx += 1
    rows[idx].append(misc.round2_or_none(json['stats'][port_or_bench].get('max_drawdown')))

    # Beta
    idx += 1
    rows[idx].append(misc.round2_or_none(json['stats']['beta']) if port_mode else None)

    # Alpha
    idx += 1
    rows[idx].append(misc.round2_or_none(json['stats']['alpha']) if port_mode else None)

    # Avg Number of Positions
    idx += 1
    rows[idx].append(misc.round2_or_none(json['results']['average'][4]) if port_mode else None)


def validate_main(*, settings, logger: logging.Logger):
    for prop, meta_info in mapping_init.MAIN.items():
        if meta_info.get('required') and prop not in settings:
            logger.error(f'"Main" section does not contain the required property "{prop}"')
            return False
    return True


def validate_settings(*, operation, settings, logger: logging.Logger):
    for prop, meta_info in operation['mapping']['settings'].items():
        if meta_info.get('required') and prop not in settings:
            logger.error(f'"Default Settings" section does not contain the required property "{prop}"')
            return False
    return True


def validate_iteration(*, operation, iteration_idx, iteration_data, logger: logging.Logger):
    if 'iterations' in operation['mapping']:
        for prop, meta_info in operation['mapping']['iterations'].items():
            if meta_info.get('required') and prop not in iteration_data:
                logger.error(f'Iteration #{iteration_idx + 1} does not contain required property "{prop}"')
    return True


def validate_data_settings(settings, logger: logging.Logger):
    if 'Start Date' not in settings:
        logger.error('"Default Settings" section needs to contain the "Start Date" property')
        return False

    items_def_cnt = 0
    items_def_cnt += 1 if 'P123 UIDs' in settings else 0
    items_def_cnt += 1 if 'Tickers' in settings else 0
    items_def_cnt += 1 if 'Cusips' in settings else 0
    if not items_def_cnt:
        logger.error('"Default Settings" section needs to contain one of the following properties: '
                     '"P123 UIDs", "Tickers" or "Cusips"')
        return False
    if items_def_cnt > 1:
        logger.error('"Default Settings" section can only contain one of the following properties: '
                     '"P123 UIDs", "Tickers" or "Cusips"')
        return False

    if 'P123 UIDs' in settings:
        items = settings['P123 UIDs']
    elif 'Tickers' in settings:
        items = settings['Tickers']
    else:
        items = settings['Cusips']
    if misc.is_int(items):
        items = [items]
    elif misc.is_str(items):
        items = items.split(' ')
    items_cnt = len(items)
    if items_cnt > 50:
        logger.error('"Default Settings" can only contain at most 50 "P123 UIDs", "Tickers" or "Cusips"')
        return False
    return True
