RANKING_METHOD = {'nasnegative': 2, 'nasneutral': 4}
RANK_PERF_METRICS = (
    'Annualized return', 'Average excess return', 'Total return', '% of periods strategy outperforms',
    'Max gain', 'Max loss', 'Max gain single stock', 'Max loss single stock', 'Avg excess return in Up Markets',
    'Avg excess return in Down Markets', 'Sharpe', 'Sortino', 'StdDev', 'Max Drawdown', 'Beta', 'Alpha',
    'Avg # of positions'
)
RANK_PERF_METHOD = {'long': 'long', 'short': 'short'}
ROLLING_SCREEN_COLUMNS = [
    {'name': 'Name', 'justify': 'left'},
    {'name': 'Start', 'justify': 'left', 'length': 10},
    {'name': 'End', 'justify': 'left', 'length': 10},
    'Periods', 'Avg#Pos', 'AvgRet%', 'AvgBench%', 'AvgExcess%',
    'Min%NoSlip', 'Max%NoSlip', 'AvgStdDev', 'Top12AvgRet%', 'Top60AvgRet%'
]
FREQ = [
    {'label': '1day', 'value': 'Every Day', 'days': 1},
    {'label': '1week', 'value': 'Every Week', 'days': 7},
    {'label': '2weeks', 'value': 'Every 2 Weeks', 'days': 7 * 2},
    {'label': '3weeks', 'value': 'Every 3 Weeks', 'days': 7 * 3},
    {'label': '4weeks', 'value': 'Every 4 Weeks', 'days': 7 * 4},
    {'label': '6weeks', 'value': 'Every 6 Weeks', 'days': 7 * 6},
    {'label': '8weeks', 'value': 'Every 8 Weeks', 'days': 7 * 8},
    {'label': '13weeks', 'value': 'Every 13 Weeks', 'days': 7 * 13},
    {'label': '26weeks', 'value': 'Every 26 Weeks', 'days': 7 * 26},
    {'label': '52weeks', 'value': 'Every 52 Weeks', 'days': 7 * 52}
]
FREQ_BY_LABEL = {item['label']: item for item in FREQ}
SCREEN_METHOD = {'long': 'long', 'short': 'short', 'longshort': 'long/short', 'hedged': 'hedged'}
SCREEN_BACKTEST_RISK_STATS_FREQ = ('monthly', 'weekly', 'daily')
SCREEN_ROLLING_BACKTEST_FREQ = {'1week': FREQ_BY_LABEL['1week']['value'], '4weeks': FREQ_BY_LABEL['4weeks']['value']}
SCREEN_BACKTEST_FREQ = {item['label']: item['value'] for item in FREQ}
SCREEN_ROLLING_BACKTEST_TRANS_PRICE = {'open': 1, 'close': 4, 'avghilow': 3}