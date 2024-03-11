import matplotlib.pyplot as plt
plt.style.use('ggplot')

import numpy as np
import pandas as pd

from loader import dataloader
from pathmgmt import pathmgmt as myPath

INDEX_MAPPING = {'hs300': '沪深300',
                 'zz500': '中证500',
                 'zz800': '中证800',
                 'zz1000': '中证1000', }


def compute_matrics(positions, tab_name, start, end, universe, transform, neutralize, holding):
    # print(positions.reset_index().groupby('time').pnl.sum().to_frame())
    daily_pnl = positions.reset_index().groupby('time').pnl.sum().to_frame()
    
    long_only = positions.reset_index().groupby('time').long_pnl.sum().to_frame()['long_pnl']
    daily_pnl = daily_pnl.reset_index().merge(long_only.reset_index(),
                                        on=['time'], how='left').set_index('time')

    # daily_pnl['long_pnl'] = positions.reset_index().groupby(
    #     'time').long_pnl.sum().to_frame()['long_pnl']
    
    # load benchmark price
    if universe in INDEX_MAPPING:
        universe = INDEX_MAPPING[universe]
        benchmark = dataloader.loading(
            tab_name="Index", start=start, end=end, fields=['code', 'name', 'close'])
        benchmark = benchmark.loc[daily_pnl.index[1]:]
        daily_pnl['benchmark_ret'] = benchmark.loc[benchmark.name == universe].close.transform(lambda x: np.log(x.shift(-1)) - np.log(x))
    else: # e.g., zz9999
        benchmark = pd.read_csv(myPath.DATA_DIR/'customidx'/f'{universe}_ret.csv')
        benchmark['time'] = benchmark['time'].astype("string")
        benchmark.set_index(['time'], inplace=True)
        benchmark.index = pd.to_datetime(benchmark.index)
        benchmark = benchmark.loc[daily_pnl.index[1]:]
        daily_pnl['benchmark_ret'] = benchmark.mkt_weighted_ret

    daily_pnl['benchmark_ret'] = daily_pnl['benchmark_ret'].fillna(0)
    # print(daily_pnl)
    
    # cumulative pnl
    daily_pnl['cum_pnl'] = daily_pnl['pnl'].cumsum()
    daily_pnl['cum_long_pnl'] = daily_pnl['long_pnl'].cumsum()
    daily_pnl['cum_benchmark_ret'] = daily_pnl['benchmark_ret'].cumsum()
    # print(daily_pnl)
    
    # excess return 
    daily_pnl['excess_ret'] = daily_pnl['long_pnl'] - daily_pnl['benchmark_ret']
    daily_pnl['cum_excess_ret'] = daily_pnl['cum_long_pnl'] - daily_pnl['cum_benchmark_ret']
    
    # annualized return 
    annual_ret = daily_pnl.pnl.mean() * 252
    # annualized excess return
    annual_excess_ret = daily_pnl.excess_ret.mean() * 252
    # information ratio, long only
    ir2 = daily_pnl.excess_ret.mean() / daily_pnl.excess_ret.std() * np.sqrt(252)
    # information raio, not compared to benchmark
    ir1 = daily_pnl.pnl.mean() / daily_pnl.pnl.std() * np.sqrt(252)
    # information coefficient
    # position weights vs 1d return 
    ic = positions.reset_index().groupby('time')[[f'fut_ret_1d', 'weights']].corr().iloc[0::2, -1].mean()
    # max drawdown
    max_drawdown = -(daily_pnl.cum_pnl - daily_pnl.cum_pnl.cummax()).min()
    max_drawdown_excess = -(daily_pnl.cum_excess_ret - daily_pnl.cum_excess_ret.cummax()).min()
    # total turnover
    positions['turnover'] = positions.groupby('code').weights.diff().abs() / 2
    turnover = positions.reset_index().groupby('time').turnover.sum().mean() 
    daily_pnl['turnover'] = positions.reset_index().groupby('time').turnover.sum()
    
    statistics = {'Annualized Return': annual_ret, 
                  'Annualized Excess Return': annual_excess_ret, 
                  'IR': ir1, 
                  'IR long only': ir2,
                  'IC': ic, 
                  'Max Drawdown': max_drawdown,
                  'Max Drawdown long only': max_drawdown_excess,
                  'daily Turnover': turnover}
    
    # plot the performance metrics
    plot(tab_name, daily_pnl, statistics, start, end, transform, neutralize, holding)
    
    # return daily_pnl, statistics
    

def plot(tab_name, daily_pnl, statistics, start, end, transform, neutralize, holding):
    # pnl plot
    fig, ax = plt.subplots(1, 1, figsize=(16, 12))
    fig.autofmt_xdate(rotation=45)
    ax.plot(daily_pnl[['cum_pnl', 'cum_benchmark_ret', 'cum_excess_ret']], label=[
            'cumulative return', 'benchmark return', 'cumulative excess return'])
    ax.legend()
    ax.set_title(f'PnL for {tab_name} from {start} to {end} with holding period {holding} days')
    plt.figtext(.95, .49, pd.DataFrame(
        data=["{:.3%}".format(v) if k not in ['IR', 'IR long only'] else "{:.3}".format(v)
            for k, v in statistics.items()],
        index=statistics.keys())[0].to_string(),
        {'multialignment': 'right', 'fontsize': 12})
    (myPath.PLOT_DIR/tab_name/'PnL_plot').mkdir(parents=True, exist_ok=True)
    plt.savefig(myPath.PLOT_DIR/tab_name/'PnL_plot'/
                f'{start}-{end}-{transform}-{neutralize}-holding{holding}days-PnL.png', bbox_inches='tight')
    # turnover plot
    fig, ax = plt.subplots(1, 1, figsize=(16, 12))
    fig.autofmt_xdate(rotation=45)
    ax.plot(daily_pnl['turnover'], label='daily turnover')
    ax.legend()
    ax.set_title(f'Daily Turnover for {tab_name} from {start} to {end} with holding period {holding} days')
    (myPath.PLOT_DIR/tab_name/'Turnover_plot').mkdir(parents=True, exist_ok=True)
    plt.savefig(myPath.PLOT_DIR/tab_name/'Turnover_plot'/
                f'{start}-{end}-{transform}-{neutralize}-holding{holding}days-Turnover.png', bbox_inches='tight')

    # save daily pnl
    (myPath.PLOT_DIR/tab_name/'PnL_results').mkdir(parents=True, exist_ok=True)
    daily_pnl.to_csv(myPath.PLOT_DIR/tab_name/'PnL_results'/
                     f'{start}-{end}-{transform}-{neutralize}-holding{holding}days-dailyPnL.csv')

    # save statistics to file
    (myPath.PLOT_DIR/tab_name/'statistics').mkdir(parents=True, exist_ok=True)
    pd.DataFrame(data=statistics, index=[0]).to_csv(myPath.PLOT_DIR/tab_name/'statistics'/
                                                      f'{start}-{end}-{transform}-{neutralize}-holding{holding}days-statistics.csv', index=False)
    
def stratified_model(positions, tab_name, start, end, universe, transform, neutralize, holding):
    alphas_copy = positions[['code', 'name', 'alpha', 'fut_ret_1d']].copy().dropna().reset_index()
    alphas_copy['decile'] = alphas_copy.groupby('time')['alpha'].transform(
        lambda x: pd.qcut(x, q=10, labels=range(1, 11)))
    # equally weighted within each bucket
    daily_pnl_stratified = alphas_copy.groupby(['decile', 'time']).fut_ret_1d.mean(
        ).to_frame().reset_index().set_index('time').pivot(columns='decile', values='fut_ret_1d')
    # save
    # daily_pnl_stratified
    (myPath.PLOT_DIR/tab_name/'stratified_results').mkdir(parents=True, exist_ok=True)
    daily_pnl_stratified.to_csv(myPath.PLOT_DIR/tab_name/'stratified_results'/
                                f'{start}-{end}-{transform}-{neutralize}-holding{holding}days-stratifiedPnL.csv')
    # plot
    daily_pnl_stratified = daily_pnl_stratified.cumsum()
    fig, ax = plt.subplots(1, 1, figsize=(16, 12))
    cmap = plt.get_cmap('tab10')
    for i, col in enumerate(daily_pnl_stratified.columns):
        ax.plot(daily_pnl_stratified[col],
                color=cmap(i), label=f'decile {col}')
    plt.legend()
    ax.set_title(f'Stratified PnL for {tab_name} from {start} to {end} with holding period {holding} days')
    (myPath.PLOT_DIR/tab_name/'stratified_plot').mkdir(parents=True, exist_ok=True)
    plt.savefig(myPath.PLOT_DIR/tab_name/'stratified_plot'/
                f'{start}-{end}-{transform}-{neutralize}-holding{holding}days-Stratified.png')


def concentration_plot(positions, tab_name, start, end, universe, transform, neutralize, holding):
    '''concentration is defined as the top 5% long poistion and 5% short position
    '''
    alphas_copy = positions[['code', 'name', 'weights', 'fut_ret_1d']].copy().dropna().reset_index()
    # Calculate the threshold values for each date
    thresholds = alphas_copy.groupby('time')['weights'].quantile([0.05, 0.95], interpolation='nearest').unstack()
    
    alphas_copy = alphas_copy.merge(thresholds.reset_index(), on=['time'], how='left')

    top_short = alphas_copy[alphas_copy['weights'] <= alphas_copy[0.05]]
    top_long = alphas_copy[alphas_copy['weights'] >= alphas_copy[0.95]]

    top_short_sum = top_short.groupby('time')['weights'].sum()
    top_long_sum = top_long.groupby('time')['weights'].sum()
    
    # Create a new DataFrame to combine the results
    concentration = pd.DataFrame({'Top 5% short position': top_short_sum, 'Top 5% long position': top_long_sum})

    fig, ax = plt.subplots(1, 1, figsize=(16, 12))
    fig.autofmt_xdate(rotation=45)
    ax.plot(concentration[['Top 5% short position', 'Top 5% long position']], label=['Top 5% short position', 'Top 5% long position'])
    ax.legend()
    ax.set_title(f'Concentration analysis for {tab_name} from {start} to {end} with holding period {holding} days')
    (myPath.PLOT_DIR/tab_name/'concentration_plot').mkdir(parents=True, exist_ok=True)
    plt.savefig(myPath.PLOT_DIR/tab_name/'concentration_plot'/f'{start}-{end}-{transform}-{neutralize}-holding{holding}days-concentration.png', bbox_inches='tight')
