from datetime import datetime
import logging
from multiprocessing import Pool

import numpy as np
import pandas as pd

from loader import dataloader
from pathmgmt import pathmgmt as myPath
from . import metrics
from . import utility
from logger import logger as myLogger

logger = myLogger.Logger(__name__)
logger.init(console_handler=True)

class PnLCalculator():
    def __init__(self, args_dict):
        self.__name = args_dict['table_name']
        self.__universe = self.__name.split('-')[-1]
        self.__start = args_dict['start_date']
        self.__end = args_dict['end_date']
        self.__alpha = self.__load_alpha()
        self.__transform = args_dict['transform']
        self.__neutralize = args_dict['neutralize']
        self.__holding = args_dict['holding']
        
    def __load_alpha(self):
        logger().debug("loading computed alphas...")
        alphas = dataloader.loading(
            tab_name=self.__name, start=self.__start, end=self.__end, fields=['code', 'name', 'alpha', 'fut_ret_1d'])
        # # purging periods where weights are NA
        # alphas.dropna(subset=['weights'], inplace=True)
        logger().debug("loading computed alphas...Done!")
        return alphas

    def compute_pnl(self):
        # convert alphas into positions
        self.__alpha = utility.alpha_to_weight(self.__alpha, self.__transform, self.__neutralize)
        # compute rolling alphas for holding period larger than 1
        self.__alpha = utility.rolling(self.__alpha, self.__holding)
        # compute pnl
        self.__alpha['pnl'] = self.__alpha['weights'] * self.__alpha['fut_ret_1d']
        ## long only pnl
        self.__alpha['long_pnl'] = self.__alpha['weights'] * \
            self.__alpha['fut_ret_1d'] * (self.__alpha['weights'] > 0)
        # compute performance metrics
        # here we assume benchmark is the universe
        logger().debug("computing metrics and recording...")
        metrics.compute_matrics(
            self.__alpha, self.__name, self.__start, self.__end, self.__universe, self.__transform, self.__neutralize, self.__holding)
        logger().debug("computing metrics and recording...Done!")
        
        logger().debug("computing stratified PnL and recording...")
        try:
            metrics.stratified_model(
                self.__alpha, self.__name, self.__start, self.__end, self.__universe, self.__transform, self.__neutralize, self.__holding)
        except:
            pass
        logger().debug("computing stratified PnL and recording...Done!")
        
        logger().debug("computing concentration analysis and recording...")
        try:
            metrics.concentration_plot(
                self.__alpha, self.__name, self.__start, self.__end, self.__universe, self.__transform, self.__neutralize, self.__holding)
        except:
            pass
        logger().debug("computing concentration analysis and recording......Done!")

        
        # daily_pnl, statistics = metrics.compute_matrics(
        #     positions, tab_name, start, end, universe, holding_period)

        # return positions, daily_pnl,  statistics
