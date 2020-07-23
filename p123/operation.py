import logging
import p123.data.cons as data_cons
import utils.misc as misc
import tkinter as tk
import p123.data.transform as transform
from p123api import Client, ClientException
import p123.util as util
import datetime
import p123.mapping.init as mapping_init
import p123.mapping.data as mapping_data
import p123.mapping.rank as mapping_rank
import p123.mapping.screen as mapping_screen


class Operation:
    def __init__(self, *, api_client: Client, data, output, logger: logging.Logger):
        self._data = data
        self._logger = logger
        self._logger.info(f"Running ({self._data['Main']['Operation']})")

        self._result = []
        self._paused = False
        self._stopped = False
        self._finished = False
        self._api_client = api_client
        self._output = output
        self._continue_on_error = self._data['Main']['On Error'].lower() == 'continue' \
            if 'On Error' in self._data['Main'] else True

        self._init_default_params()
        if self.has_init_error():
            return
        self._init_header_row()
        self._init_col_setup()

    def _init_default_params(self):
        self._has_init_error = False
        try:
            self._default_params = util.generate_params(
                data=self._data['Default Settings'], settings=self._data['Default Settings'],
                api_client=self._api_client, logger=self._logger
            )
            precision = self._data['Main'].get('Precision')
            if precision is not None:
                self._default_params['precision'] = precision
        except ClientException as e:
            self._logger.error(e)
            self._has_init_error = True

    def _init_col_setup(self):
        self._col_setup = []
        for idx, column in enumerate(self._header_row):
            justify = 'right'
            if misc.is_dict(column):
                name = column['name']
                length = max(10, (column['length'] if 'length' in column else len(name)) + 2)
                if 'justify' in column:
                    justify = column['justify']
            else:
                name = column
                length = max(10, len(name) + 2)
            self._col_setup.append({'length': length, 'justify': justify})
            self._header_row[idx] = name

    def _init_header_row(self):
        self._header_row = []

    def has_init_error(self):
        return self._has_init_error

    def pause(self):
        self._paused = True

    def resume(self):
        self._paused = False

    def is_paused(self):
        return self._paused

    def stop(self):
        self._stopped = True

    def is_finished(self):
        return self._finished or self._stopped

    def _write_row_to_output(self, row, newline: bool = True):
        self._output.configure(state='normal')
        if newline:
            self._output.insert(tk.END, '\n')
        for idx, content in enumerate(row):
            if content is not None:
                content = f'{content:.2f}' if misc.is_float(content) else str(content)
            else:
                content = 'NA'
            length = self._col_setup[idx]['length']
            justify = self._col_setup[idx]['justify']
            self._output.insert(
                tk.END, content.rjust(length, ' ') if justify == 'right' else content.ljust(length, ' '))
        self._output.configure(state='disabled')

    def _write_value_to_output(self, value, newline: bool = True):
        self._output.configure(state='normal')
        if newline:
            self._output.insert(tk.END, '\n')
        self._output.insert(tk.END, str(value))
        self._output.configure(state='disabled')

    def run(self):
        try:
            run_outcome = self._run()
        except Exception as e:
            print(e)
            self._logger.error('Internal error')
            run_outcome = False
        if run_outcome is not None:
            self._finished = True
            if run_outcome:
                self._logger.info(f"Done ({self._data['Main']['Operation']})")

    def _run(self):
        """
        Actual operation run logic, this method should be overridden by implementing classes.
        :return:
            None - paused
            False - error encountered and On Error is set to Stop
            True - otherwise
        """

    def get_result(self):
        return self._result

    @staticmethod
    def init(*, api_client, data, output, logger: logging.Logger):
        operation = OPERATIONS.get(data['Main']['Operation'].lower())['class'](
            api_client=api_client, data=data, output=output, logger=logger
        )
        return operation if not operation.has_init_error() else None


class IterOperation(Operation):
    _api_item_change_checks = (
        ('universe', 'ApiUniverse', transform.universe),
        ('ranking', 'ApiRankingSystem', transform.screen_ranking)
    )

    def __init__(self, *, api_client, data, output, logger: logging.Logger):
        super().__init__(api_client=api_client, data=data, output=output, logger=logger)
        self._iter_idx = 0
        self._iter_cnt = len(data['Iterations'])
        self._api_item_changed = {}

    def _run_iter(self, *, iter_data, iter_params):
        pass

    def _check_api_item_change(self, iter_params):
        if self._default_params.get('screen'):
            for change in IterOperation._api_item_change_checks:
                if self._default_params['screen'].get(change[0]) == change[1]:
                    if iter_params.get('screen') and iter_params['screen'].get(change[0]) == change[1]:
                        self._api_item_changed[change[0]] = True
                    elif self._api_item_changed.get(change[0]):
                        change[2](
                            value=self._data['Default Settings'][change[0].capitalize()]['value'],
                            settings=self._data['Default Settings'], api_client=self._api_client
                        )
                        del self._api_item_changed[change[0]]

    def _run(self):
        while self._iter_idx < self._iter_cnt:
            if self.is_paused():
                return

            iter_data = self._data['Iterations'][self._iter_idx]
            iter_params = None
            try:
                iter_params = util.generate_params(
                    data=iter_data, settings=self._data['Default Settings'],
                    api_client=self._api_client, logger=self._logger
                )
            except ClientException as e:
                self._logger.error(e)

            try:
                if iter_params is None:
                    raise IterationFailedException
                self._check_api_item_change(iter_params)
                self._run_iter(iter_data=iter_data, iter_params=iter_params)
            except OperationPausedException:
                return
            except IterationFailedException:
                if not self._continue_on_error:
                    return False

            self._iter_idx += 1

        return True


class ScreenRollingBacktestOperation(IterOperation):
    def __init__(self, *, api_client, data, output, logger: logging.Logger):
        super().__init__(api_client=api_client, data=data, output=output, logger=logger)
        self._result.append(self._header_row)
        self._write_row_to_output(self._header_row, False)

    def _init_header_row(self):
        max_len = 0
        for iter_idx, iter_data in enumerate(self._data['Iterations']):
            name_len = len(iter_data['Name'] if 'Name' in iter_data else f'Iteration {iter_idx + 1}')
            if max_len < name_len:
                max_len = name_len
        self._header_row = data_cons.ROLLING_SCREEN_COLUMNS.copy()
        self._header_row[0] = self._header_row[0].copy()
        self._header_row[0]['length'] = max_len

    def _run_iter(self, *, iter_data, iter_params):
        try:
            params = util.update_iter_params(self._default_params, iter_params)
            json = self._api_client.screen_rolling_backtest(params)
            row = util.process_screen_rolling_backtest_result(
                json, params.get('startDt'), params.get('endDt'), params.get('precision'))
            row = [iter_data['Name'] if 'Name' in iter_data else 'Iteration ' + str(self._iter_idx + 1)] + row
            self._result.append(row)
            self._write_row_to_output(row)
            self._logger.info(f"Iteration {self._iter_idx + 1}/{self._iter_cnt}: success")
        except ClientException as e:
            self._logger.error(e)
            self._logger.warning(f"Iteration {self._iter_idx + 1}/{self._iter_cnt}: failed")
            raise IterationFailedException


class ScreenRunOperation(Operation):
    def __init__(self, *, api_client, data, output, logger: logging.Logger):
        super().__init__(api_client=api_client, data=data, output=output, logger=logger)

    def _init_header_row_custom(self):
        self._header_row = [{'name': 'P123 UID', 'justify': 'left', 'length': 10}]
        max_len = 0
        for row in self._result[:100]:
            max_len = max(max_len, len(row[1]))
        self._header_row.append({'name': 'Ticker', 'justify': 'left', 'length': max_len})
        max_len = 0
        for row in self._result[:100]:
            max_len = max(max_len, len(row[2]))
        self._header_row += [
            {'name': 'Name', 'justify': 'left', 'length': max_len},
            'Last'
        ]
        if len(self._result[0]) == 5:
            self._header_row.append('Rank')
        self._init_col_setup()
        self._result.insert(0, self._header_row)
        self._write_row_to_output(self._header_row, False)

    def _run(self):
        try:
            if 'screen' not in self._default_params:
                self._default_params['screen'] = {'type': self._data['Default Settings']['Type']}
            json = self._api_client.screen_run(self._default_params)
            self._result += json['rows']
            self._init_header_row_custom()
            for row in self._result[1:101]:
                self._write_row_to_output(row)
            if len(self._result) > 101:
                self._output.configure(state='normal')
                self._output.insert(tk.END, '\nOnly showing first 100 rows in preview.')
                self._output.configure(state='disabled')

        except ClientException as e:
            self._logger.error(e)
            return False

        return True


class ScreenBacktestOperation(Operation):
    def __init__(self, *, api_client, data, output, logger: logging.Logger):
        super().__init__(api_client=api_client, data=data, output=output, logger=logger)

    def _init_header_row_stats(self):
        self._header_row = [
            {'name': '', 'length': 8, 'justify': 'left'},
            'Tot Return', 'Ann Return', 'Max Dd', 'Sharpe', 'Sortino', 'StdDev', 'CorrelBench', 'R-Squared',
            'Beta', 'Alpha'
        ]
        self._init_col_setup()
        self._result.append(self._header_row)

    def _init_header_row_results(self, columns):
        for idx, row in enumerate(columns[0:4]):
            columns[idx] = {'name': row, 'length': 10 + (2 if idx == 0 else 0), 'justify': 'left'}
        self._header_row = columns
        self._init_col_setup()
        self._result.append(self._header_row)

    def _init_header_row_series(self):
        self._header_row = [
            {'name': 'Date', 'length': 10, 'justify': 'left'},
            'Screen Return %', 'Benchmark Return %', 'Turnover %', '# Positions'
        ]
        self._init_col_setup()
        self._result.append(self._header_row)

    def _run(self):
        try:
            if 'screen' not in self._default_params:
                self._default_params['screen'] = {'type': self._data['Default Settings']['Type']}
            json = self._api_client.screen_backtest(self._default_params)
            self._result.append(['Stats'])
            self._write_value_to_output('Stats', False)
            self._init_header_row_stats()
            stats = json['stats']
            item_stats = stats['port']
            self._result.append([
                'Screen', item_stats['total_return'], item_stats['annualized_return'], item_stats['max_drawdown'],
                item_stats['sharpe_ratio'], item_stats['sortino_ratio'], item_stats['standard_dev'],
                stats['correlation'], stats['r_squared'], stats['beta'], stats['alpha']
            ])
            item_stats = stats['bench']
            self._result.append([
                'Benchmark', item_stats['total_return'], item_stats['annualized_return'], item_stats['max_drawdown'],
                item_stats['sharpe_ratio'], item_stats['sortino_ratio'], item_stats['standard_dev']
            ])
            rows_to_write = self._result[1:100]
            for row in rows_to_write:
                self._write_row_to_output(row)
            rows_written_cnt = len(rows_to_write) + 1

            self._result.append([])
            self._result.append(['Results'])
            self._init_header_row_results(json['results']['columns'])
            self._result += json['results']['rows']
            data = json['results']['average']
            data[0] = 'Average'
            self._result.append(data)
            data = json['results']['upMarkets']
            data[0] = 'Up Markets'
            self._result.append(data)
            data = json['results']['downMarkets']
            data[0] = 'Down Markets'
            self._result.append(data)
            if rows_written_cnt < 100:
                rows_to_write = self._result[rows_written_cnt:100]
                for row in rows_to_write:
                    self._write_row_to_output(row)
                rows_written_cnt += len(rows_to_write)

            self._result.append([])
            self._result.append(['Time Series'])
            self._init_header_row_series()
            chart = json['chart']
            for idx, date in enumerate(chart['dates']):
                self._result.append([
                    date, chart['screenReturns'][idx], chart['benchReturns'][idx], chart['turnoverPct'][idx],
                    chart['positionCnt'][idx]
                ])
            if rows_written_cnt < 100:
                rows_to_write = self._result[rows_written_cnt:100]
                for row in rows_to_write:
                    self._write_row_to_output(row)

            if len(self._result) > 101:
                self._output.configure(state='normal')
                self._output.insert(tk.END, '\nOnly showing first 100 rows in preview.')
                self._output.configure(state='disabled')

        except ClientException as e:
            self._logger.error(e)
            return False

        return True


class DataOperation(IterOperation):
    def __init__(self, *, api_client, data, output, logger: logging.Logger):
        super().__init__(api_client=api_client, data=data, output=output, logger=logger)
        self._raw_result = {'items': {}}
        self._include_names = self._data['Default Settings'].get('Include Names')
        if self._include_names:
            self._include_names = self._include_names['value']
        self._include_cusips = self._data['Default Settings'].get('Cusips')

    def _init_header_row_custom(self):
        self._header_row = [
            {'name': 'Date', 'justify': 'left', 'length': 10},
            {'name': 'P123 UID', 'justify': 'left', 'length': 10}
        ]
        max_len_ticker = 6
        max_len_name = 12
        name_idx = 4 if self._include_cusips else 3
        for row in self._result[:100]:
            max_len_ticker = max(max_len_ticker, len(row[2]))
            if self._include_names:
                max_len_name = max(max_len_name, len(row[name_idx]))
        self._header_row.append({'name': 'Ticker', 'justify': 'left', 'length': max_len_ticker})
        if self._include_cusips:
            self._header_row.append({'name': 'Cusip', 'justify': 'left', 'length': 9})
        if self._include_names:
            self._header_row.append({'name': 'Company Name', 'justify': 'left', 'length': max_len_name})
        for idx, data in enumerate(self._data['Iterations']):
            name = misc.coalesce(data.get('Name'), data['Formula'])[:50]
            self._header_row.append({'name': name, 'length': max(len(name), 12)})
        self._init_col_setup()
        self._result.insert(0, self._header_row)
        self._write_row_to_output(self._header_row, False)

    def _run(self):
        run_outcome = super()._run()
        if run_outcome is not None and self._iter_idx > 0:
            for idx, date in enumerate(self._raw_result['dates']):
                for p123_uid, item in self._raw_result['items'].items():
                    row = [date, p123_uid, item['ticker']]
                    if self._include_cusips:
                        row.append(item['cusip'])
                    if self._include_names:
                        row.append(item['name'])
                    for series_data in item['series']:
                        row.append(series_data[idx])
                    self._result.append(row)
            self._init_header_row_custom()
            for row in self._result[1:101]:
                self._write_row_to_output(row)
            if len(self._result) > 101:
                self._output.configure(state='normal')
                self._output.insert(tk.END, '\nOnly showing first 100 rows in preview.')
                self._output.configure(state='disabled')
        return run_outcome

    def _run_iter(self, *, iter_data, iter_params):
        try:
            params = util.update_iter_params(self._default_params, iter_params)
            params['formulas'] = [iter_data['Formula']]
            json = self._api_client.data(params)
            if 'dates' not in self._raw_result:
                self._raw_result['dates'] = json['dates']
            for p123_uid, item in json['items'].items():
                if p123_uid not in self._raw_result['items']:
                    self._raw_result['items'][p123_uid] = item
                else:
                    self._raw_result['items'][p123_uid]['series'] += item['series']
            self._logger.info(f"Iteration {self._iter_idx + 1}/{self._iter_cnt}: success")
        except ClientException as e:
            self._logger.error(e)
            self._logger.warning(f"Iteration {self._iter_idx + 1}/{self._iter_cnt}: failed")
            raise IterationFailedException


class DataUniverseOperation(Operation):
    def __init__(self, *, api_client, data, output, logger: logging.Logger):
        super().__init__(api_client=api_client, data=data, output=output, logger=logger)
        self._include_names = self._data['Default Settings'].get('Include Names')
        if self._include_names:
            self._include_names = self._include_names['value']

    def _init_header_row_custom(self):
        self._header_row = [{'name': 'P123 UID', 'justify': 'left', 'length': 10}]
        max_len_ticker = 6
        max_len_name = 12
        for row in self._result[:100]:
            max_len_ticker = max(max_len_ticker, len(row[1]))
            if self._include_names:
                max_len_name = max(max_len_name, len(row[2]))
        self._header_row.append({'name': 'Ticker', 'justify': 'left', 'length': max_len_ticker})
        if self._include_names:
            self._header_row.append({'name': 'Company Name', 'justify': 'left', 'length': max_len_name})
        for formula in self._data['Default Settings']['Formulas']:
            name = str(list(formula.keys())[0] if misc.is_dict(formula) else formula)[:50]
            self._header_row.append({'name': name, 'length': max(len(name), 12)})
        self._init_col_setup()
        self._result.insert(0, self._header_row)
        self._write_row_to_output(self._header_row, False)

    def _run(self):
        try:
            self._default_params['formulas'] = list(map(
                lambda item: str(list(item.values())[0] if misc.is_dict(item) else item),
                self._data['Default Settings']['Formulas']
            ))
            as_of_dt = self._data['Default Settings']['As of Date']
            if misc.is_list(as_of_dt):
                as_of_dt = as_of_dt[0]
            self._default_params['asOfDt'] = transform.date(value=as_of_dt)
            json = self._api_client.data_universe(self._default_params)
            for idx, p123_uid in enumerate(json['p123Uids']):
                row = [p123_uid, json['tickers'][idx]]
                if self._include_names:
                    row.append(json['names'][idx])
                for data in json['data']:
                    row.append(data[idx])
                self._result.append(row)
            self._init_header_row_custom()
            for row in self._result[1:101]:
                self._write_row_to_output(row)
            if len(self._result) > 101:
                self._output.configure(state='normal')
                self._output.insert(tk.END, '\nOnly showing first 100 rows in preview.')
                self._output.configure(state='disabled')
        except ClientException as e:
            self._logger.error(e)
            return False

        return True


class RankPerfOperation(IterOperation):
    def __init__(self, *, api_client, data, output, logger: logging.Logger):
        self._buckets = data['Default Settings']['Buckets']
        super().__init__(api_client=api_client, data=data, output=output, logger=logger)
        self._run_idx = None
        self._run_rows = None

    def _init_default_params(self):
        super()._init_default_params()
        if self.has_init_error():
            return
        if 'screen' not in self._default_params:
            self._default_params['screen'] = {'type': self._data['Default Settings']['Type']}

    def _init_header_row(self):
        max_len = 0
        for name in data_cons.RANK_PERF_METRICS:
            name_len = len(name)
            if max_len < name_len:
                max_len = name_len
        self._header_row = [{'name': 'Metric', 'justify': 'left', 'length': max_len}]
        self._header_row.extend(f'Bucket {idx + 1}' for idx in range(self._buckets))
        self._header_row.append('Universe')
        self._header_row.append('Benchmark')

    def _run_iter(self, *, iter_data, iter_params):
        params = util.update_iter_params(self._default_params, iter_params)
        name = iter_data['Name'] if 'Name' in iter_data else 'Iteration ' + str(self._iter_idx + 1)

        if self._run_idx is None:
            self._run_idx = 0
            self._run_rows = []
            for metric in data_cons.RANK_PERF_METRICS:
                self._run_rows.append([metric])

        screen_rules = params['screen'].get('rules')
        params['transPrice'] = 4

        # add one more run to the number of buckets for the universe
        while self._run_idx < self._buckets + 1:
            if self.is_paused():
                raise OperationPausedException

            run_screen_rules = screen_rules.copy() if screen_rules else []
            if self._run_idx < self._buckets:
                start = round(100 / self._buckets * self._run_idx, 2)
                end = round(100 / self._buckets * (self._run_idx + 1), 2)
                formula =\
                    'Rank >= {} and Rank <{} {}'.format(start, '=' if self._run_idx == self._buckets - 1 else '', end)
                run_screen_rules.append({'formula': formula})
            if run_screen_rules:
                params['screen']['rules'] = run_screen_rules
            elif 'rules' in params['screen']:
                del params['screen']['rules']

            try:
                json = self._api_client.screen_backtest(params)
                util.process_rank_perf_result(json, self._run_rows, True, params.get('precision'))
                if self._run_idx == self._buckets:
                    util.process_rank_perf_result(json, self._run_rows, False, params.get('precision'))
                self._logger.info(
                    f"Iteration {self._iter_idx + 1}/{self._iter_cnt} "
                    f"run {self._run_idx + 1}/{self._buckets + 1}: success")
            except ClientException as e:
                self._logger.error(e)
                self._logger.warning(
                    f"Iteration {self._iter_idx + 1}/{self._iter_cnt} "
                    f"run {self._run_idx + 1}/{self._buckets + 1}: failed")
                if not self._continue_on_error:
                    raise IterationFailedException
                for row in self._run_rows:
                    row.append(None)

            self._run_idx += 1

        if self._iter_idx > 0:
            row = []
            self._result.append(row)
            self._write_row_to_output(row)
        row = [name]
        self._result.append(row)
        self._write_row_to_output(row, newline=self._iter_idx > 0)
        self._result.append(self._header_row)
        self._write_row_to_output(self._header_row)
        for row in self._run_rows:
            self._result.append(row)
            self._write_row_to_output(row)

        self._run_idx = None


class RankRanksOperation(Operation):
    def __init__(self, *, api_client, data, output, logger: logging.Logger):
        super().__init__(api_client=api_client, data=data, output=output, logger=logger)
        self._columns = misc.coalesce(self._data['Default Settings'].get('Columns'), 'ranks').lower()
        if self._columns != 'ranks':
            self._default_params['includeNodeDetails'] = True
        self._include_names = self._data['Default Settings'].get('Include Names')
        if self._include_names:
            self._include_names = self._include_names['value']

    def _init_header_row_custom(self, json):
        self._header_row = [{'name': 'P123 UID', 'justify': 'left', 'length': 10}]
        max_len = 0
        for ticker in json['tickers'][:100]:
            max_len = max(max_len, len(ticker))
        self._header_row.append({'name': 'Ticker', 'justify': 'left', 'length': max_len})
        if self._include_names:
            max_len = 0
            for name in json['names'][:100]:
                max_len = max(max_len, len(name))
            self._header_row.append({'name': 'Name', 'justify': 'left', 'length': max_len})
        self._header_row += [
            '#NAs',
            'Final Stmt',
            '100% rank'
        ]
        if self._columns != 'ranks':
            self._add_nodes_to_header_row(json)
        self._init_col_setup()
        self._result.append(self._header_row)
        self._write_row_to_output(self._header_row, False)

    def _add_nodes_to_header_row(self, json):
        for idx, name in enumerate(json['nodes']['names']):
            if idx == 0:
                continue
            node_type = json['nodes']['types'][idx]
            if self._columns == 'composite' and node_type == 1:
                continue
            # noinspection PyTypeChecker
            self._header_row.append(
                '{}% {} ({})'.format(json['nodes']['weights'][idx], name, json['nodes']['parents'][idx]))

    def _run(self):
        try:
            self._default_params['includeNaCnt'] = True
            self._default_params['includeFinalStmt'] = True
            json = self._api_client.rank_ranks(self._default_params)
            self._init_header_row_custom(json)
            node_rank_idxs = []
            if self._columns != 'ranks':
                for node_idx, node_type in enumerate(json['nodes']['types']):
                    if node_idx > 0 and (self._columns == 'factor' or node_type == 0):
                        node_rank_idxs.append(node_idx)
            for idx, p123_uid in enumerate(json['p123Uids']):
                row = [p123_uid, json['tickers'][idx]]
                if self._include_names:
                    row.append(json['names'][idx])
                row += [json['naCnt'][idx], 'Y' if json['finalStmt'][idx] else 'N', json['ranks'][idx]]
                if self._columns:
                    for node_idx in node_rank_idxs:
                        row.append(json['nodes']['ranks'][idx][node_idx])
                self._result.append(row)
            for row in self._result[1:101]:
                self._write_row_to_output(row)
            if len(self._result) > 101:
                self._output.configure(state='normal')
                self._output.insert(tk.END, '\nOnly showing first 100 rows in preview.')
                self._output.configure(state='disabled')

        except ClientException as e:
            self._logger.error(e)
            return False

        return True


class RankRanksPeriodOperation(Operation):
    def __init__(self, *, api_client, data, output, logger: logging.Logger):
        super().__init__(api_client=api_client, data=data, output=output, logger=logger)
        date = self._data['Default Settings']['Start Date']
        end_date = self._data['Default Settings']['End Date']
        today = datetime.date.today()
        if end_date > today:
            end_date = today
        self._dates = []
        days = data_cons.FREQ_BY_LABEL[data['Default Settings']['Frequency'].lower()]['days']
        while date < end_date:
            self._dates.append(date)
            date = date + datetime.timedelta(days=days)
        self._iter_idx = 0
        self._iter_cnt = len(self._dates)
        self._include_names = self._data['Default Settings'].get('Include Names')
        if self._include_names:
            self._include_names = self._include_names['value']
        self._ranks_by_p123_uid = {}

    def _init_header_row_custom(self):
        self._header_row = [{'name': 'P123 UID', 'justify': 'left', 'length': 10}]
        max_len = 0
        for row in self._result[:100]:
            max_len = max(max_len, len(row[1]))
        self._header_row.append({'name': 'Ticker', 'justify': 'left', 'length': max_len})
        if self._include_names:
            max_len = 0
            for row in self._result[:100]:
                max_len = max(max_len, len(row[2]))
            self._header_row.append({'name': 'Name', 'justify': 'left', 'length': max_len})
        for date in self._dates:
            self._header_row.append(str(date))
        self._init_col_setup()
        self._result.insert(0, self._header_row)
        self._write_row_to_output(self._header_row, False)

    def _run(self):
        while self._iter_idx < self._iter_cnt:
            if self.is_paused():
                return

            try:
                self._default_params['asOfDt'] = str(self._dates[self._iter_idx])
                json = self._api_client.rank_ranks(self._default_params)
                self._dates[self._iter_idx] = json['dt']
                for idx, p123_uid in enumerate(json['p123Uids']):
                    if p123_uid not in self._ranks_by_p123_uid:
                        row = [p123_uid, json['tickers'][idx]]
                        if self._include_names:
                            row.append(json['names'][idx])
                        self._result.append(row)
                        self._ranks_by_p123_uid[p123_uid] = [None] * self._iter_cnt
                    self._ranks_by_p123_uid[p123_uid][self._iter_idx] = json['ranks'][idx]
                self._logger.info(f'Iteration {self._iter_idx + 1}/{self._iter_cnt}: success')
            except ClientException as e:
                self._logger.error(e)
                self._logger.warning(f'Iteration {self._iter_idx + 1}/{self._iter_cnt}: failed')
                if not self._continue_on_error:
                    break

            self._iter_idx += 1

        if self._iter_idx > 0:
            for row in self._result:
                row += self._ranks_by_p123_uid[row[0]]
            self._init_header_row_custom()
            for row in self._result[1:101]:
                self._write_row_to_output(row)
            if len(self._result) > 101:
                self._output.configure(state='normal')
                self._output.insert(tk.END, '\nOnly showing first 100 rows in preview.')
                self._output.configure(state='disabled')

        return self._iter_idx == self._iter_cnt


class RankRanksMultiOperation(IterOperation):
    def __init__(self, *, api_client, data, output, logger: logging.Logger):
        super().__init__(api_client=api_client, data=data, output=output, logger=logger)
        self._include_names = self._data['Default Settings'].get('Include Names')
        if self._include_names:
            self._include_names = self._include_names['value']
        self._ranks_by_p123_uid = {}

    def _init_header_row_custom(self):
        self._header_row = [{'name': 'P123 UID', 'justify': 'left', 'length': 10}]
        max_len = 0
        for row in self._result[:100]:
            max_len = max(max_len, len(row[1]))
        self._header_row.append({'name': 'Ticker', 'justify': 'left', 'length': max_len})
        if self._include_names:
            max_len = 0
            for row in self._result[:100]:
                max_len = max(max_len, len(row[2]))
            self._header_row.append({'name': 'Name', 'justify': 'left', 'length': max_len})
        for iter_idx, iter_data in enumerate(self._data['Iterations']):
            name = iter_data.get('Name')
            if not name:
                ranking_system = iter_data['Ranking System']['value']
                name = ranking_system if misc.is_str(ranking_system) else f'Iteration {iter_idx + 1}'
            self._header_row.append(name)
        self._init_col_setup()
        self._result.insert(0, self._header_row)
        self._write_row_to_output(self._header_row, False)

    def _run(self):
        run_outcome = super()._run()
        if run_outcome is not None and self._iter_idx > 0:
            for row in self._result:
                row += self._ranks_by_p123_uid[row[0]]
            self._init_header_row_custom()
            for row in self._result[1:101]:
                self._write_row_to_output(row)
            if len(self._result) > 101:
                self._output.configure(state='normal')
                self._output.insert(tk.END, '\nOnly showing first 100 rows in preview.')
                self._output.configure(state='disabled')
        return run_outcome

    def _run_iter(self, *, iter_data, iter_params):
        try:
            params = util.update_iter_params(self._default_params, iter_params)
            json = self._api_client.rank_ranks(params)
            for idx, p123_uid in enumerate(json['p123Uids']):
                if p123_uid not in self._ranks_by_p123_uid:
                    row = [p123_uid, json['tickers'][idx]]
                    if self._include_names:
                        row.append(json['names'][idx])
                    self._result.append(row)
                    self._ranks_by_p123_uid[p123_uid] = [None] * self._iter_cnt
                self._ranks_by_p123_uid[p123_uid][self._iter_idx] = json['ranks'][idx]
            self._logger.info(f'Iteration {self._iter_idx + 1}/{self._iter_cnt}: success')
        except ClientException as e:
            self._logger.error(e)
            self._logger.warning(f'Iteration {self._iter_idx + 1}/{self._iter_cnt}: failed')
            raise IterationFailedException


class OperationPausedException(Exception):
    pass


class IterationFailedException(Exception):
    pass


def validate_property(*, section, iteration_idx=None, prop, value, meta_info: dict, logger: logging.Logger):
    """
    Checks if a certain property has a validation function defined in its meta info and attempts to
    validate against it
    :param section:
    :param iteration_idx:
    :param prop: the name of the property
    :param value: the value of the property
    :param meta_info: the meta info of the property
    :param logger:
    :return: bool
    """
    if 'isValid' in meta_info:
        ret = meta_info['isValid'](value)
        if misc.is_str(ret) or not ret:
            error = ': ' + ret if misc.is_str(ret) else ''
            if iteration_idx is None:
                logger.error(f'Invalid value for "{prop}" property in "{section}" section' + error)
            else:
                logger.error(f'Invalid value for "{prop}" property in iteration #{iteration_idx + 1}' + error)
            return False
    return True


def process_input_section(*, section, operation=None, data: dict, iteration_idx=None, logger: logging.Logger):
    """
    Parses, validates and annotates (meta info) a section's parameters
    :param operation:
    :param section:
    :param iteration_idx: Iteration idx
    :param data: section's data
    :param logger:
    :return: bool
    """
    if section == 'Main':
        mapping = mapping_init.MAIN
    elif section == 'Default Settings':
        mapping = operation['mapping']['settings']
    else:
        mapping = operation['mapping']['iterations']

    for prop, value in data.items():
        if prop in mapping:
            meta_info = mapping[prop]
            if not validate_property(
                    section=section, iteration_idx=iteration_idx, prop=prop, value=value, meta_info=meta_info,
                    logger=logger):
                return False

            if 'field' in meta_info:
                data[prop] = {'value': value, 'meta_info': meta_info}
        else:
            if section != 'Iterations':
                logger.error(f'Unrecognized property "{prop}" in "{section}" section')
            else:
                logger.error(f'Unrecognized property "{prop}" in iteration #{iteration_idx + 1}')
            return False
    return True


def process_input(*, data: dict, logger: logging.Logger):
    """
    Parse data input, stops and returns false on any validation error encountered.
    :param data: the data dictionary
    :param logger:
    :return: bool
    """
    if not misc.is_dict(data):
        logger.error('Input is not valid')
        return False

    if 'Main' not in data:
        logger.error('"Main" section not found')
        return False
    if 'Default Settings' not in data:
        logger.error('"Default Settings" section not found')
        return False

    main_data = data['Main']
    if not misc.is_dict(main_data):
        logger.error('"Main" section is not valid')
        return False
    if not process_input_section(section='Main', data=main_data, logger=logger):
        return False
    if not util.validate_main(settings=main_data, logger=logger):
        return False

    operation = OPERATIONS.get(data['Main']['Operation'].lower())
    if operation is None:
        logger.error(f'Invalid value for "Operation" property in "Main" section')
        return False
    operation_has_iterations = operation['mapping'].get('iterations') is not None

    if operation_has_iterations and 'Iterations' not in data:
        logger.error('"Iterations" section not found')
        return False
    if operation_has_iterations:
        if len(data) > 3:
            logger.error('Only "Main", "Default Settings" and "Iterations" sections expected')
            return False
    else:
        if len(data) > 2:
            logger.error('Only "Main" and "Default Settings" sections expected')
            return False

    settings_data = data['Default Settings']
    if not misc.is_dict(settings_data):
        logger.error('"Default Settings" section is not valid')
        return False
    if not process_input_section(section='Default Settings', operation=operation, data=settings_data, logger=logger):
        return False
    if not util.validate_settings(operation=operation, settings=data['Default Settings'], logger=logger):
        return False
    if 'validate_settings' in operation and not operation['validate_settings'](data['Default Settings'], logger):
        return False

    if 'Type' not in data['Default Settings']:
        data['Default Settings']['Type'] = 'Stock'

    if not operation_has_iterations:
        return True

    iterations = data['Iterations']
    if not misc.is_list(iterations):
        logger.error('"Iterations" section is not valid')
        return False
    for iteration_idx, iteration_data in enumerate(iterations):
        if not misc.is_dict(iteration_data):
            logger.error(f'Iteration #{iteration_idx + 1} is not valid')
            return False
        if not process_input_section(
                section='Iterations', operation=operation, iteration_idx=iteration_idx, data=iteration_data,
                logger=logger
        ):
            return False
        if not util.validate_iteration(
                operation=operation, iteration_idx=iteration_idx, iteration_data=iteration_data, logger=logger):
            return False

    return True


OPERATIONS = {
    'rollingscreen': {
        'class': ScreenRollingBacktestOperation,
        'mapping': {
            'settings': mapping_screen.ROLLING_SCREEN_SETTINGS,
            'iterations': mapping_screen.ROLLING_SCREEN_ITERATIONS
        }
    },
    'rankperformance': {
        'class': RankPerfOperation,
        'mapping': {'settings': mapping_screen.RANK_PERF_SETTINGS, 'iterations': mapping_screen.RANK_PERF_ITERATIONS}
    },
    'data': {
        'class': DataOperation,
        'mapping': {'settings': mapping_data.SETTINGS, 'iterations': mapping_data.ITERATIONS},
        'validate_settings': util.validate_data_settings
    },
    'ranks': {
        'class': RankRanksOperation,
        'mapping': {'settings': mapping_rank.RANKS}
    },
    'ranksperiod': {
        'class': RankRanksPeriodOperation,
        'mapping': {'settings': mapping_rank.RANKS_PERIOD}
    },
    'ranksmulti': {
        'class': RankRanksMultiOperation,
        'mapping': {'settings': mapping_rank.RANKS_MULTI_SETTINGS, 'iterations': mapping_rank.RANKS_MULTI_ITERATIONS}
    },
    'screenrun': {
        'class': ScreenRunOperation,
        'mapping': {'settings': mapping_screen.SCREEN_RUN_SETTINGS}
    },
    'screenbacktest': {
        'class': ScreenBacktestOperation,
        'mapping': {'settings': mapping_screen.SCREEN_BACKTEST_SETTINGS}
    },
    'datauniverse': {
        'class': DataUniverseOperation,
        'mapping': {'settings': mapping_data.UNIVERSE_SETTINGS}
    }
}
