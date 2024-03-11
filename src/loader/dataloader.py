# Standard
import bisect

# 
import pandas as pd

# Local Library imports
from utils.calendar import CALENDAR
from pathmgmt import pathmgmt as myPath

def loading(tab_name, start, end=None, N=None, fields=None):
    if end:
        return loadingRange(tab_name, start, end, fields)
    elif N:
        return loadingRolling(tab_name, start, N, fields)

def loadingRange(tab_name, start, end, fields):
    """Read data from start to end
    tab_name: table name
    start: start date
    end: end date
    fields: columns of interest
    """
    dates = [d.strftime("%Y%m%d")
             for d in pd.date_range(start, end)]
    if tab_name in ["Trading Halt", "ST Stocks"]:
        output = fromSingleFile(tab_name, dates, fields)
    else:
        output = fromMultiFiles(tab_name, dates, fields)
    return output

def loadingRolling(tab_name, start, N, fields):
    """Read N-days data, starting from start. 
    N can be negative, meaning reading backwardly.
    tab_name: table name
    start: start date
    N: offset, can be both positive or negative
    fields: columns of interest
    """
    def getDates(N, date):
        idx = bisect.bisect_left(CALENDAR, date)
        if N >= 0:
            dates = CALENDAR[idx: idx + N]
        else:
            if date == CALENDAR[idx]:
                dates = CALENDAR[(idx + N + 1): (idx + 1)]
            else:
                dates = CALENDAR[(idx + N): idx]
        return dates
    dates = getDates(N, start)
    if tab_name in ["Trading Halt", "ST Stocks"]:
        output = fromSingleFile(tab_name, dates, fields)
    else:
        output = fromMultiFiles(tab_name, dates, fields)
    return output

def fromMultiFiles(tab_name, dates, fields):
    '''For data stored as multiple files, each as a single date
    "stk_1min",
    "adj_factor",
    "index",
    "limits",
    "mkt_value",
    "industry",
    "universe",
    '''
    output = pd.DataFrame()
    for date in dates:
        filepath = myPath.getfilePath(
            tab_name, date=date, indexName=fields) if tab_name == "Universe" else myPath.getfilePath(tab_name, date=date)
        if filepath.exists():
            current = pd.read_csv(
                filepath) if tab_name == "Universe" else pd.read_csv(
                    filepath, usecols=fields)
            current['time'] = date
            output = pd.concat([output, current])
    output.set_index(['time'], inplace=True)
    output.index = pd.to_datetime(output.index)
    return output

def fromSingleFile(tab_name, dates, fields):
    """For data stored as a single file with multiple dates
    "halt_date",
    "ST_date",
    """
    filepath = myPath.getfilePath(tab_name)
    if 'date' not in fields:
        fields = fields + ['date']
    output = pd.read_csv(filepath, usecols=fields)
    dates_set = set(dates)
    output = output.loc[output.date.transform(lambda x: str(x) in dates_set)]
    output['date'] = output['date'].astype("string")
    output.set_index(['date'], inplace=True)
    output.index = pd.to_datetime(output.index)
    return output
    
