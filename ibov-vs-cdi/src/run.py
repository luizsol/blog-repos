import time

import numpy as np
import pandas as pd

import fetch_and_parse

start_time = time.time()

returns = fetch_and_parse.load_and_merge_data(
    ibov_path='../data/ibov-data.csv', cdi_path='../data/cdi-data.csv')

log_data = np.log(returns)
max_window_size = len(returns) + 1

windowed_returns = pd.DataFrame(
    data={'window_size': [], 'type': [], 'return': []})

dataframes = []

for window_size in range(1, max_window_size):
    print(f'Creating cumulative returns for window size {window_size}'
          f'/{max_window_size} ({100 * window_size / max_window_size:.2f}%)')
    windows = np.exp(log_data.rolling(window_size).sum())

    window_return_records = []

    for _, row in windows.iterrows():
        window_return_records.append({
            'type': 'cdi',
            'window_size': window_size,
            'return': row['cdi'],
        })

        window_return_records.append({
            'type': 'ibov',
            'window_size': window_size,
            'return': row['ibov_adj'],
        })

    dataframes.append(pd.DataFrame(window_return_records))


windowed_returns = windowed_returns.append(
    dataframes, ignore_index=True, sort=False)

windowed_returns.to_csv('../data/raw_windowed.csv')

end_time = time.time()

print(f'Total time: {(end_time - start_time) / 60:.1f} minutes')
