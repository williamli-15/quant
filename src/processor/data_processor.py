# Standard library import 
from .pv_basics import process1min, processConsistentVolume, processConsistentBuySell, processBuySellTurnover, processConsistentBuySellTurnover, processConsistentTurnover, processintradayTurnover, processIlliquidity, processMoneyflow, processMoneyflow2
from datetime import datetime
from functools import partial
import glob
import json
import logging
from multiprocessing import Pool
import os
from pathlib import Path 
import warnings
import zipfile
warnings.filterwarnings('ignore')

# Third party imports
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

# Local Library imports
from pathmgmt import pathmgmt as myPath
from utils.calendar import CALENDAR

logFormatter = logging.Formatter(
    '%(asctime)s:%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

LogPath = myPath.makeLogPath(datetime.now(), __name__)

fileHandler = logging.FileHandler(LogPath)
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)
logger.setLevel(logging.DEBUG)

def process1minFiles(input, output, func, refresh=False):
    '''Here we would define the information we wanted to extract from raw 1min data
    TODO: we can add more columns/features at later stage
    '''
    logger.debug("start processing process1minFiles")
    # create the folder/table the first time if note exists
    (myPath.DATA_DIR/output).mkdir(parents=True, exist_ok=True)
    with Pool() as pool:                        
        pool.map(partial(process1minOneFile, output, func, refresh), (myPath.DATA_DIR/input).glob('*'))
    
    # for name in tqdm((myPath.DATA_DIR/input).glob('*')):
    #     process1minOneFile(name, output, refresh)


def process1minOneFile(output, func, refresh, name):
    date = str(name).split('\\')[-1].split('.')[0] + '.csv'
    outputfile = myPath.DATA_DIR/output/date
    if outputfile.exists() and not refresh:
        return
    try:
        table = []
        with zipfile.ZipFile(name) as z:
            for filename in z.namelist():
                data = pd.read_csv(z.open(filename))
                # row, column_names = process1min(data)
                row, column_names = func(data)
                table.append(row)
        df = pd.DataFrame(data=table, columns=column_names)
        # we save each date to a single file
        df.to_csv(outputfile, index=False)
        logger.debug(f"Done processing {name}")
    except:
        logger.error(f"Exeption when processing {name}")
        # error_list.append(name)
    
    
def preprocessST(inputfile, outputfile):
    '''fill the gap between two status:
    for example, if the name has a status id 2 on 20180102, and another status id 1 on 20180202, 
    we should fill every trading day between 20180102 and 20180201 inclusively with a status id 2
    '''
    st_date = pd.read_csv(myPath.DATA_DIR/'date'/inputfile)
    st_date = st_date.sort_values(by=['code', 'eff_date'])
    st_date['eff_date'] = st_date['eff_date'].astype('string')
    st_date['status_id'] = st_date['status_id'].astype('string')

    # we print out the status transition for each stock
    st_trainsition = st_date.sort_values(by=['code', 'eff_date']).groupby(
        'code').status_id.apply(lambda x: '->'.join(x))

    # print(st_trainsition.value_counts())
    # 2->1                                  312
    # 2                                     111
    # 2->1->2->1                             83
    # 2->4->3->1                             48
    # 2->4->5                                47
    # 2->1->2                                44
    # 5                                      36
    # 2->1->2->1->2->1                       18
    # 2->4->1->3                             12
    # 2->1->2->1->2                          10
    # 2->1->2->4->3->1                        8
    # 2->1->6->7->5                           6
    # 4->6->7->5                              6
    # 2->4->3->1->2->1                        6
    # 2->1->5                                 6
    # 2->4->1->6->7->5                        5
    # 2->4->6->1->7->5                        4
    # 2->4->3->1->2                           4
    # 2->1->2->4->1->3                        4
    # 2->5                                    4
    # 2->1->2->1->2->4->1->6->7->5            3
    # 2->6->1->7->5                           3
    # 2->4->3->1->2->1->2                     3
    # 2->1->2->6->1->7->5                     3
    # 2->4->3->1->5                           3
    # 2->1->2->1->2->1->2->1                  3
    # 2->1->2->4->1->6->7->5                  3
    # 6->7->5                                 3
    # 2->1->2->4->6->1->7->5                  2
    # 2->1->2->4->5                           2
    # 2->1->2->1->5                           2
    # 2->4->3->1->2->4->1->6->7->5            2
    # 2->1->2->4->3->1->2                     2
    # 2->4->2->3->1                           2
    # 2->1->2->1->2->1->2->4->3->1            2
    # 2->1->2->1->6->7->5                     2
    # 2->4->3->1->2->4->1->3                  1
    # 2->1->2->4->3->1->2->1->2               1
    # 2->4->3->1->2->4->6->1->7->5            1
    # 2->4->3->4->3->1->2->1                  1
    # 2->4->1->3->2->1->2->1->2               1
    # 2->4->3->1->2->1->2->4->1->6->7->5      1
    # 2->1->2->4->3->1->2->1                  1
    # 2->4->3->1->2->4->3->1->2               1
    # 2->4->3->4->3->1                        1
    # 2->4->1->3->2->4->6->1->7->5            1
    # 2->4->3->1->2->4->3->1                  1
    # 2->4->2->3->1->2                        1
    # 2->4                                    1
    # 2->1->2->1->2->5                        1
    # 2->1->2->4->3->1->2->1->2->4            1
    # 2->1->2->1->2->6->1->7->5               1
    # 2->4->3->2->1->2->1->2                  1
    # 2->4->3->1->2->1->2->1->2               1
    # 2->4->2->3->1->2->1                     1
    # 2->4->3->2->1->2->1                     1
    # 1                                       1
    # Name: status_id, dtype: int64

    st_date['transition'] = st_date.sort_values(by=['code', 'eff_date']).groupby(
        'code').status_id.transform(lambda x: '->'.join(x))

    CALENDAR_set = set(CALENDAR)

    def resample(df):
        df.drop_duplicates(subset=['code', 'eff_date'], inplace=True)
        df['eff_date'] = pd.to_datetime(df['eff_date'])
        df = df.set_index('eff_date').groupby(
            'code').apply(lambda x: x.resample('D').ffill()).reset_index(1).reset_index(drop=True)
        df['date'] = df.eff_date.dt.strftime("%Y%m%d")
        # only keep the actively trading days
        df = df.loc[df.date.transform(lambda x: x in CALENDAR_set)]
        df.drop(columns=['eff_date'], inplace=True)
        return df

    # we treat two cases separately
    # the final staus is 1 or 3
    def case1(df):
        df = df.loc[df.transition.str.endswith(
            '1') | df.transition.str.endswith('3')]
        df = resample(df)
        return df
    st_date1 = case1(st_date)

    # the final staus is 2 or 4 or 5
    def case2(df):
        max_date = df.eff_date.max()
        df = df.loc[df.transition.str.endswith('2')
                    | df.transition.str.endswith('4')
                    | df.transition.str.endswith('5')]
        # we get last status for each stock
        df_last_day = df.drop_duplicates(
            subset=['code'], keep='last').copy()
        # we add a row here which indicates that the status lasts to at least the largest date in the table
        df_last_day['eff_date'] = max_date
        df = pd.concat([df, df_last_day]
                       ).drop_duplicates().sort_values(by=['code', 'eff_date'])

        df = resample(df)
        return df
    st_date2 = case2(st_date)

    outFolder = myPath.DATA_DIR/'dateProcess'
    outFolder.mkdir(parents=True, exist_ok=True)
    output = pd.concat([st_date1, st_date2]).sort_values(by=['date'])
    output.to_csv(outFolder/outputfile, index=False)

def preprocessHalt(inputfile, outputfile):
    '''we create table from raw data where we list every stocks that experiences halts on a single day, 
    either for a full day or just part of the day
    '''
    halt_date = pd.read_csv(myPath.DATA_DIR/'date'/inputfile)
    result = []
    for row in halt_date.values:
        code = row[0]
        name = row[1]
        start = str(row[2])
        end = str(row[4])
        dates = pd.date_range(start, end)
        
        if row[5] == 93000:
            dates = dates[:-1]
        dates = [d.strftime("%Y%m%d") for d in dates]
        
        for d in dates:
            result.append([d, code, name])
            
    CALENDAR_set = set(CALENDAR)
            
    df = pd.DataFrame(data=result, columns=['date', 'code', 'name'])
    df = df.sort_values(by=['date', 'code']).drop_duplicates()
    # only keep the actively trading days
    df = df.loc[df.date.transform(lambda x: x in CALENDAR_set)]
    
    outFolder = myPath.DATA_DIR/'dateProcess'
    outFolder.mkdir(parents=True, exist_ok=True)
    df.to_csv(outFolder/outputfile, index=False)


if __name__ == "__main__":
    # run below to preprocess st dates
    # preprocessST('st_date.csv', 'ST_date.csv')
    
    # run below to preprocess halt dates
    # preprocessHalt('halt_date.csv', 'halt_date.csv')
    
    # run below to preprocess 1min raw data
    # error_list = process1minFiles('qishi_1min', '1minProcess', process1min)
    # error_list = process1minFiles('qishi_1min', '1minConsistentVolume', processConsistentVolume)
    # error_list = process1minFiles('qishi_1min', '1minConsistentBuySell', processConsistentBuySell)
    # error_list = process1minFiles('qishi_1min', '1minBuySellTurnover', processBuySellTurnover)
    # error_list = process1minFiles('qishi_1min', '1minConsistentBuySellTurnover', processConsistentBuySellTurnover)
    # error_list = process1minFiles('qishi_1min', '1minConsistentTurnover', processConsistentTurnover)
    # error_list = process1minFiles('qishi_1min', 'intradayTurnover', processintradayTurnover)
    # error_list = process1minFiles('qishi_1min', 'Illiquidity', processIlliquidity)
    # error_list = process1minFiles('qishi_1min', 'MoneyFlow', processMoneyflow)
    error_list = process1minFiles('qishi_1min', 'MoneyFlow2', processMoneyflow2)
