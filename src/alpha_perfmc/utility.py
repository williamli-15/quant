from datetime import datetime
import logging

import numpy as np
import pandas as pd

from loader import dataloader
from pathmgmt import pathmgmt as myPath
from logger import logger as myLogger
from utils.calendar import CALENDAR, START, END

logger = myLogger.Logger(__name__)
logger.init(console_handler=True)

def transform(alphas, method='None'):
    if method == "None":
        return alphas
    elif method == 'Rank':
        alphas['alpha'] = alphas.groupby('time').alpha.transform(
            lambda x: x.rank(ascending=True, method="average"))
        return alphas
    else:
        logger().exception(f"Not Implemented for transformation method {method}")
        
def neutralize(alphas, neutralize_method):
    if neutralize_method == 'None':
        return simple_demean(alphas)
    elif neutralize_method == 'Industry':
        return neutralization_sector_only(alphas)
    elif neutralize_method == 'Industry_with_weighted_cap':
        return neutralization_sector_and_marketcap(alphas)
    else:
        logger().exception(f"Not Implemented for neutralization method {neutralize_method}")
        
def simple_demean(alphas):
    alphas['weights'] = alphas.groupby(
        'time').alpha.transform(lambda x: x - x.mean())
    alphas['weights'] = alphas.groupby(
        'time').weights.transform(lambda x: x / x.abs().sum() * 2)
    return alphas

def neutralization_sector_only(alphas):
    '''demean by sector,  where mean is equally weighted mean
    '''
    logger().debug("Loading industry group info...")
    industry = dataloader.loading(
        tab_name="Sector", start=START, end=END, fields=['code', 'sw1', 'sw2', 'sw3'])
    logger().debug("Loading industry group info...Done!")
    
    alphas = pd.merge(alphas.reset_index(), industry.reset_index(), left_on=[
        'time', 'code'], right_on=['time', 'code'], how='left')
    def helper(df):
        df['sector_alpha_mean'] = df.groupby(
            'sw1').alpha.transform(lambda x: x.mean())
        df['weights'] = df['alpha'] - df['sector_alpha_mean']
        df['weights'] = df['weights'] / df.weights.abs().sum() * 2
        return df
    alphas = alphas.groupby('time').apply(helper).set_index('time')
    return alphas

def neutralization_sector_and_marketcap(alphas):
    '''demean by sector,  where mean is market value weighted mean
    TODO: long or short not summing up to 1 using this method, how to reconcile?
    TODO: square roots of market value to reduce concentration risk?
    '''
    logger().debug("Loading market cap info...")
    market_value = dataloader.loading(
        tab_name="Market Value", start=START, end=END, fields=['code', 'neg_mkt_val', 'mkt_val', 'neg_shares', 'shares'])
    logger().debug("Loading market cap info...Done!")

    logger().debug("Loading industry group info...")
    industry = dataloader.loading(
        tab_name="Sector", start=START, end=END, fields=['code', 'sw1', 'sw2', 'sw3'])
    logger().debug("Loading industry group info...Done!")

    alphas = pd.merge(alphas.reset_index(), market_value.reset_index(), left_on=[
        'time', 'code'], right_on=['time', 'code'], how='left')
    alphas = pd.merge(alphas.reset_index(), industry.reset_index(), left_on=[
        'time', 'code'], right_on=['time', 'code'], how='left')
    alphas['weighted_alpha'] = alphas['alpha'] * alphas['mkt_val']
    def helper(df):
        df['sector_market_value'] = df.groupby(
            'sw1').mkt_val.transform(lambda x: x.sum())
        df['sector_weighted_alpha'] = df.groupby(
            'sw1').weighted_alpha.transform(lambda x: x.sum())
        df['sector_weighted_zscores'] = df['sector_weighted_alpha'] / \
            df['sector_market_value']
        df['weights'] = df['alpha'] - df['sector_weighted_zscores']
        df['weights'] = df['weights'] / df.weights.abs().sum() * 2
        return df
    alphas = alphas.groupby('time').apply(helper).set_index('time')
    return alphas

def halt_adjustment(alphas):
    logger().debug("Loading trading halts info...")
    halt_info = dataloader.loading(
        tab_name="Trading Halt", start=START, end=END, fields=['date', 'code', 'name'])
    logger().debug("Loading trading halts info...Done!")

    alphas['prev_weights'] = alphas.groupby('code').weights.shift(1)
    alphas[['weights', 'prev_weights']] = alphas[[
        'weights', 'prev_weights']].fillna(value=0)
    alphas = pd.merge(alphas.reset_index(), halt_info.reset_index(), left_on=[
                    'time', 'code', 'name'], right_on=['date', 'code', 'name'], how='left')
    alphas = alphas.groupby('time').apply(halt_adjustment_helper).set_index('time')
    # check after adjustment all positive weights sum to 1
    # alphas.groupby('time').apply(
    #     lambda x: x.loc[x.weights > 0, 'weights'].sum()).value_counts()
    return alphas
    
def halt_adjustment_helper(df):
    '''ensure that \sum w_i = 1, for all w_i > 0; \sum w_i = -1, for all w_i = -1
    consider trading halts, freeze w_i for each stock i that halts on the day
    then we need \sum w_i = 1 - x, for all w_i > 0, where x is the sum of all positive weights from previous date that halt today
    \sum w_i = -1 - y, for all w_i < 0, where y is the sum of all negative weights from previous date that halt today'''
    # before freezing
    x0 = df.loc[(~df.date.isna()) & (df.weights > 0), 'weights'].sum()
    y0 = df.loc[(~df.date.isna()) & (df.weights < 0), 'weights'].sum()
    # freeze weights to previous date
    df.loc[~df.date.isna(), 'weights'] = df.loc[~df.date.isna(), 'prev_weights']
    # after freezing
    x1 = df.loc[(~df.date.isna()) & (df.weights > 0), 'weights'].sum()
    y1 = df.loc[(~df.date.isna()) & (df.weights < 0), 'weights'].sum()
    # make the adjustment for the non freezing weights
    df.loc[(df.date.isna()) & (df.weights > 0), 'weights'] = df.loc[(
        df.date.isna()) & (df.weights > 0), 'weights'] / (1 - x0) * (1 - x1)
    df.loc[(df.date.isna()) & (df.weights < 0), 'weights'] = df.loc[(
        df.date.isna()) & (df.weights < 0), 'weights'] / (1 + y0) * (1 + y1)
    return df

def alpha_to_weight(alphas, transform_method='None', neutralize_method='None'):
    logger().debug("preprocessing alphas...")
    alphas = transform(alphas, transform_method)
    logger().debug("preprocessing alphas...Done!")
    
    logger().debug("neutralizing alphas...")
    alphas = neutralize(alphas, neutralize_method)
    logger().debug("neutralizing alphas...Done!")
    
    logger().debug("applying halt adjustments...")
    alphas = halt_adjustment(alphas)
    logger().debug("applying halt adjustments...Done!")
    return alphas

def rolling(alphas, holding=1):
    rolling = alphas.groupby('code').weights.rolling(holding, min_periods=1).sum().reset_index()
    rolling['weights'] = rolling['weights'] / holding
    rolling.rename(columns={'weights': 'rolling_weights'}, inplace=True)
    alphas = alphas.reset_index().merge(rolling.reset_index(),on=['time', 'code'], how='left').set_index('time')
    alphas.drop(columns=['weights'], inplace=True)
    alphas.rename(columns={'rolling_weights': 'weights'}, inplace=True)
    return alphas
