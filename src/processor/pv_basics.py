# Standard
import numpy as np

def process1min(data):
    '''write a function to iterate all names in one date folder
    Main functionality to generate features
    '''
    column_names = ['code', 'open', 'close', 'pre_close',
                    'low', 'high', 'open_std',
                    'close_std', 'total_volume', 'total_turnover', 'volume_std', 'turnover_std']
    # TODO: sprecial handling for 0 value??
    open = data.iloc[0].open
    close = data.iloc[-1].close
    pre_close = data.iloc[0].pre_close
    low = data.low.min()
    high = data.high.max()
    # 1 min return volatility
    open_std = data.open.pct_change().replace([np.inf, -np.inf], np.nan).dropna().std()
    close_std = data.close.pct_change().replace([np.inf, -np.inf], np.nan).dropna().std()
    total_volume = data.iloc[-1].accvolume
    total_turnover = data.iloc[-1].accturover
    volume_std = data.volume.std()
    turnover_std = data.turover.std()  # typo here?
    return [data.iloc[0].code, open, close, pre_close, low, high, open_std, close_std, total_volume, total_turnover, volume_std, turnover_std], column_names

def processConsistentVolume(data):
    '''Function to generate the consistent volume feature'''
    a = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    columns = ['code'] + [f'consistent_volume_{i}' for i in a]
    consistent_volumes = []
    for i in a:
        consistent_volumes.append((data.volume * ((data.open - data.close).abs()
                                                < (data.high - data.low).abs() * i) * (data.time <= 1457)).sum())
    return [data.iloc[0].code] + consistent_volumes, columns

def processConsistentBuySell(data):
    '''Function to generate the consistent volume feature'''
    a = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    columns = ['code'] + [f'consistent_buy_{i}' for i in a] + [f'consistent_sell_{i}' for i in a]
    consistent_volumes = []
    for i in a:
        consistent_volumes.append((data.volume * ((data.open - data.close).abs()
                                                  < (data.high - data.low).abs() * i) * (data.time <= 1457) * (data.close > data.open)).sum())
    for i in a:
        consistent_volumes.append((data.volume * ((data.open - data.close).abs()
                                                  < (data.high - data.low).abs() * i) * (data.time <= 1457) * (data.close < data.open)).sum())
    return [data.iloc[0].code] + consistent_volumes, columns

def processBuySellVolume(data):
    pass

def processBuySellTurnover(data):
    columns = ['code'] + ['buy_turnover'] + ['sell_turnover']
    turnovers = []
    turnovers.append((data.turover * (data.time <= 1457) * (data.close > data.open)).sum())
    turnovers.append((data.turover * (data.time <= 1457) * (data.close < data.open)).sum())
    return [data.iloc[0].code] + turnovers, columns

def processConsistentBuySellTurnover(data):
    '''Function to generate the consistent turnover feature'''
    a = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    columns = ['code'] + [f'consistent_buy_trv_{i}' for i in a] + [f'consistent_sell_trv_{i}' for i in a]
    consistent_turnover = []
    for i in a:
        consistent_turnover.append((data.turover * ((data.open - data.close).abs()
                                                  < (data.high - data.low).abs() * i) * (data.time <= 1457) * (data.close > data.open)).sum())
    for i in a:
        consistent_turnover.append((data.turover * ((data.open - data.close).abs()
                                                  < (data.high - data.low).abs() * i) * (data.time <= 1457) * (data.close < data.open)).sum())
    return [data.iloc[0].code] + consistent_turnover, columns

def processConsistentTurnover(data):
    '''Function to generate the consistent volume feature'''
    a = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    columns = ['code'] + [f'consistent_turnover_{i}' for i in a]
    consistent_turnover = []
    for i in a:
        consistent_turnover.append((data.turover * ((data.open - data.close).abs()
                                                  < (data.high - data.low).abs() * i) * (data.time <= 1457)).sum())
    return [data.iloc[0].code] + consistent_turnover, columns

def processintradayTurnover(data):
    am_open = data.iloc[0].turover # 09:30
    pm_close = data.loc[data.time >= 1457].turover.sum() # close auction
    
    am_close = data.loc[data.time == 1129].turover.sum() # 11:29
    pm_open = data.loc[data.time == 1300].turover.sum() # 13:00
    
    am_open_5min = data.loc[(data.time >= 931) & (data.time <= 935)].turover.sum() # excluding first minute, 09:31 - 09:35
    am_close_5min = data.loc[(data.time >= 1125) & (data.time <= 1129)].turover.sum() # 11:25 - 11:29
    pm_open_5min = data.loc[(data.time >= 1300) & (data.time <= 1304)].turover.sum() # 13:00 - 13:04
    pm_close_5min = data.loc[(data.time >= 1452) & (data.time <= 1456)].turover.sum() # excluding close auction, 09:31 - 09:35

    high = data.turover.max()
    low = min(data.loc[data.time < 1457].turover.min(), pm_close)

    am_high = data.loc[data.time < 1300].turover.max()
    am_low = data.loc[data.time < 1300].turover.min()
    
    pm_high = max(data.loc[(data.time < 1457) & (data.time >= 1300)].turover.max(), pm_close)
    pm_low = min(data.loc[(data.time < 1457) & (data.time >= 1300)].turover.min(), pm_close)
    
    columns = ['code', 'am_open_trv', 'pm_close_trv', 'am_close_trv', 'pm_open_trv',
               'am_open_5min_trv', 'am_close_5min_trv', 'pm_open_5min_trv', 'pm_close_5min_trv',
               'high_trv', 'low_trv', 'am_high_trv', 'am_low_trv', 'pm_high_trv', 'pm_low_trv']

    return [data.iloc[0].code] + [am_open, pm_close, am_close, pm_open, am_open_5min, am_close_5min, pm_open_5min, pm_close_5min, high, low, am_high, am_low, pm_high, pm_low], columns


def processIlliquidity(data):
    illiquidity1_hl_mean = ((data.high - data.low) / data.turover).replace([np.inf, -np.inf], 0).mean()
    illiquidity1_hl_std = ((data.high - data.low) / data.turover).replace([np.inf, -np.inf], 0).std()
    illiquidity1_hl_range = ((data.high - data.low) / data.turover).replace([np.inf, -np.inf], 0).max() - ((data.high - data.low) / data.turover).replace([np.inf, -np.inf], 0).min()
    
    illiquidity1_ho_mean = ((data.high - data.open) / data.turover).replace([np.inf, -np.inf], 0).mean()
    illiquidity1_ho_std = ((data.high - data.open) / data.turover).replace([np.inf, -np.inf], 0).std()
    illiquidity1_ho_range = ((data.high - data.open) / data.turover).replace([np.inf, -np.inf], 0).max() - ((data.high - data.open) / data.turover).replace([np.inf, -np.inf], 0).min()
    
    illiquidity1_co_mean = ((data.close - data.open) / data.turover).replace([np.inf, -np.inf], 0).mean()
    illiquidity1_co_std = ((data.close - data.open) / data.turover).replace([np.inf, -np.inf], 0).std()
    illiquidity1_co_range = ((data.close - data.open) / data.turover).replace([np.inf, -np.inf], 0).max() - ((data.close - data.open) / data.turover).replace([np.inf, -np.inf], 0).min()
    
    columns = ['code', 'illiquidity1_hl_mean', 'illiquidity1_hl_std', 'illiquidity1_hl_range', 'illiquidity1_ho_mean',
               'illiquidity1_ho_std', 'illiquidity1_ho_range', 'illiquidity1_co_mean', 'illiquidity1_co_std', 'illiquidity1_co_range']

    return [data.iloc[0].code] + [illiquidity1_hl_mean, illiquidity1_hl_std, illiquidity1_hl_range, illiquidity1_ho_mean, illiquidity1_ho_std, illiquidity1_ho_range, illiquidity1_co_mean, illiquidity1_co_std, illiquidity1_co_range], columns


def processMoneyflow(data):
    column_names = ['code', 'mega_inflow_volume', 'mega_outflow_volume', 'mega_inflow_trv', 'mega_outflow_trv', 'mega_ret',
                     'large_inflow_volume', 'large_outflow_volume', 'large_inflow_trv', 'large_outflow_trv', 'large_ret',
                     'mid_inflow_volume', 'mid_outflow_volume', 'mid_inflow_trv', 'mid_outflow_trv', ',mid_ret', 
                     'small_inflow_volume', 'small_outflow_volume', 'small_inflow_trv', 'small_outflow_trv', 'small_ret']
    
    data['min_ret'] = (np.log(data.close) - np.log(data.open)).replace([np.inf, -np.inf, np.nan], 0)

    mega_inflow_volume = ((data.loc[(data.volume>= 500000) | (data.turover >= 1000000)].volume) * (data.close > data.open)).sum()
    mega_outflow_volume = ((data.loc[(data.volume>= 500000) | (data.turover >= 1000000)].volume) * (data.close < data.open)).sum()
    mega_inflow_trv = ((data.loc[(data.volume>= 500000) | (data.turover >= 1000000)].turover) * (data.close > data.open)).sum()
    mega_outflow_trv = ((data.loc[(data.volume>= 500000) | (data.turover >= 1000000)].turover) * (data.close < data.open)).sum()
    mega_ret = ((data.loc[(data.volume>= 500000) | (data.turover >= 1000000)].min_ret)).sum()
    
    large_subset = data.loc[((5000000 >= data.volume) & (data.volume >= 100000)) | ((1000000 >= data.turover) & (data.turover >= 200000))]
    large_inflow_volume = (large_subset.volume * (data.close > data.open)).sum()
    large_outflow_volume = (large_subset.volume * (data.close < data.open)).sum()
    large_inflow_trv = (large_subset.turover * (data.close > data.open)).sum()
    large_outflow_trv = (large_subset.turover * (data.close < data.open)).sum()
    large_ret = large_subset.min_ret.sum()
    
    mid_subset = data.loc[((100000 >= data.volume) & (data.volume >= 20000)) | ((200000 >= data.turover) & (data.turover >= 40000))]
    mid_inflow_volume = (mid_subset.volume * (data.close > data.open)).sum()
    mid_outflow_volume = (mid_subset.volume * (data.close < data.open)).sum()
    mid_inflow_trv = (mid_subset.turover * (data.close > data.open)).sum()
    mid_outflow_trv = (mid_subset.turover * (data.close < data.open)).sum()
    mid_ret = mid_subset.min_ret.sum()

    small_subset = data.loc[(data.volume <= 20000) | (data.turover <= 40000)]
    small_inflow_volume = (small_subset.volume * (data.close > data.open)).sum()
    small_outflow_volume = (small_subset.volume * (data.close < data.open)).sum()
    small_inflow_trv = (small_subset.turover * (data.close > data.open)).sum()
    small_outflow_trv = (small_subset.turover * (data.close < data.open)).sum()
    small_ret = small_subset.min_ret.sum()

    return ([data.iloc[0].code] + [mega_inflow_volume, mega_outflow_volume, mega_inflow_trv, mega_outflow_trv, mega_ret, 
                                  large_inflow_volume, large_outflow_volume, large_inflow_trv, large_outflow_trv, large_ret,
                                   mid_inflow_volume, mid_outflow_volume, mid_inflow_trv, mid_outflow_trv, mid_ret,
                                   small_inflow_volume, small_outflow_volume, small_inflow_trv, small_outflow_trv, small_ret], column_names)


def processMoneyflow2(data):
    column_names = ['code', 'mega_inflow_volume', 'mega_outflow_volume', 'mega_inflow_trv', 'mega_outflow_trv', 'mega_inflow_ret', 'mega_outflow_ret',
                     'large_inflow_volume', 'large_outflow_volume', 'large_inflow_trv', 'large_outflow_trv', 'large_inflow_ret', 'large_outflow_ret',
                     'mid_inflow_volume', 'mid_outflow_volume', 'mid_inflow_trv', 'mid_outflow_trv', ',mid_inflow_ret', 'mid_outflow_ret',
                     'small_inflow_volume', 'small_outflow_volume', 'small_inflow_trv', 'small_outflow_trv', 'small_inflow_ret', 'small_outflow_ret']
    
    data['min_inflow_ret'] = (np.log(data.high) - np.log(data.low)) * (data.close > data.open) + (np.log(data.high) - np.log(data.open) + np.log(data.close) - np.log(data.low)) * (data.close <= data.open)
    data['min_outflow_ret'] = (np.log(data.high) - np.log(data.low)) * (data.close <= data.open) + (np.log(data.open) - np.log(data.low) + np.log(data.high) - np.log(data.close)) * (data.close > data.open)
    data['min_inflow_ret']  = data['min_inflow_ret'].replace([np.inf, -np.inf, np.nan], 0)
    data['min_outflow_ret'] = data['min_outflow_ret'].replace([np.inf, -np.inf, np.nan], 0)
    
    data['price_range'] = ((data.open - data.low) + (data.high - data.low) + (data.high - data.close)) * (data.close > data.open) + ((data.high - data.open) + (data.high - data.low) + (data.close - data.low)) * (data.close <= data.open)
    data['inflow_ratio'] = (data.high - data.low) / data.price_range * (data.close > data.open) + (data.high - data.open + data.close - data.low) / data.price_range * (data.close <= data.open) 
    data['outflow_ratio'] = (data.high - data.low) / data.price_range * (data.close <= data.open) + (data.open - data.low + data.high - data.close) / data.price_range * (data.close > data.open) 
    data['inflow_ratio']  = data['inflow_ratio'].replace([np.inf, -np.inf, np.nan], 0)
    data['outflow_ratio'] = data['outflow_ratio'].replace([np.inf, -np.inf, np.nan], 0)
    
    mega_inflow_volume = ((data.loc[(data.volume>= 500000) | (data.turover >= 1000000)].volume) * data.inflow_ratio).sum()
    mega_outflow_volume = ((data.loc[(data.volume>= 500000) | (data.turover >= 1000000)].volume) * data.outflow_ratio).sum()
    mega_inflow_trv = ((data.loc[(data.volume>= 500000) | (data.turover >= 1000000)].turover) *  data.inflow_ratio).sum()
    mega_outflow_trv = ((data.loc[(data.volume>= 500000) | (data.turover >= 1000000)].turover) * data.outflow_ratio).sum()
    mega_inflow_ret = ((data.loc[(data.volume>= 500000) | (data.turover >= 1000000)].min_inflow_ret)).sum()
    mega_outflow_ret = ((data.loc[(data.volume>= 500000) | (data.turover >= 1000000)].min_outflow_ret)).sum()
    
    large_subset = data.loc[((5000000 >= data.volume) & (data.volume >= 100000)) | ((1000000 >= data.turover) & (data.turover >= 200000))]
    large_inflow_volume = (large_subset.volume * data.inflow_ratio).sum()
    large_outflow_volume = (large_subset.volume * data.outflow_ratio).sum()
    large_inflow_trv = (large_subset.turover * data.inflow_ratio).sum()
    large_outflow_trv = (large_subset.turover * data.outflow_ratio).sum()
    large_inflow_ret = large_subset.min_inflow_ret.sum()
    large_outflow_ret = large_subset.min_outflow_ret.sum()
    
    mid_subset = data.loc[((100000 >= data.volume) & (data.volume >= 20000)) | ((200000 >= data.turover) & (data.turover >= 40000))]
    mid_inflow_volume = (mid_subset.volume * data.inflow_ratio).sum()
    mid_outflow_volume = (mid_subset.volume * data.outflow_ratio).sum()
    mid_inflow_trv = (mid_subset.turover * data.inflow_ratio).sum()
    mid_outflow_trv = (mid_subset.turover * data.outflow_ratio).sum()
    mid_inflow_ret = mid_subset.min_inflow_ret.sum()
    mid_outflow_ret = mid_subset.min_outflow_ret.sum()

    small_subset = data.loc[(data.volume <= 20000) | (data.turover <= 40000)]
    small_inflow_volume = (small_subset.volume * data.inflow_ratio).sum()
    small_outflow_volume = (small_subset.volume * (data.close < data.open)).sum()
    small_inflow_trv = (small_subset.turover * data.inflow_ratio).sum()
    small_outflow_trv = (small_subset.turover * (data.close < data.open)).sum()
    small_inflow_ret = small_subset.min_inflow_ret.sum()
    small_outflow_ret = small_subset.min_outflow_ret.sum()

    return ([data.iloc[0].code] + [mega_inflow_volume, mega_outflow_volume, mega_inflow_trv, mega_outflow_trv, mega_inflow_ret, mega_outflow_ret,
                                   large_inflow_volume, large_outflow_volume, large_inflow_trv, large_outflow_trv, large_inflow_ret, large_outflow_ret,
                                   mid_inflow_volume, mid_outflow_volume, mid_inflow_trv, mid_outflow_trv, mid_inflow_ret, mid_outflow_ret,
                                   small_inflow_volume, small_outflow_volume, small_inflow_trv, small_outflow_trv, small_inflow_ret, small_outflow_ret], column_names)
