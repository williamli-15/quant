from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR/'data'
LOG_DIR = BASE_DIR/'log'
ALPHA_DIR = DATA_DIR/'alphas'
PLOT_DIR = BASE_DIR/'plot'

DATA_MAPPING = {"PV Basics": '1minProcess',
           "Cum Adj Factor": "adj_fct",
           "Index": 'idx',
           "Limits": "lmt",
           "Market Value": "mkt_val",
           "Sector": "sw",
           "Universe": "univ",
           "Trading Halt": 'halt_date',
           "ST Stocks": 'ST_date',
           "Consistent Volume": '1minConsistentVolume',
           "Consistent Buy Sell": '1minConsistentBuySell',
           "Turnover Buy Sell": '1minBuySellTurnover',
           "Consistent Turnover": '1minConsistentTurnover',
           "Consistent Buy Sell Turnover": '1minConsistentBuySellTurnover',
            "Intraday Turnover": 'intradayTurnover',
            "Illiquidity": 'Illiquidity',
            'MoneyFlow': 'MoneyFlow',
            'MoneyFlow2': 'MoneyFlow2'
           }

def getfilePath(tab_name, **kwargs):
    if tab_name in ["Index", "Cum Adj Factor", "Limits", "Market Value", "Sector"]:
        return DATA_DIR/DATA_MAPPING[tab_name]/kwargs['date'][:4]/(kwargs['date']+'.csv')
    elif tab_name == "Universe":
        return DATA_DIR/DATA_MAPPING[tab_name]/kwargs['indexName']/kwargs['date'][:4]/(kwargs['date']+'.csv')
    elif tab_name in ["PV Basics", "Consistent Volume", "Consistent Buy Sell", "Turnover Buy Sell", "Consistent Turnover", "Consistent Buy Sell Turnover", "Intraday Turnover",  "Illiquidity", "MoneyFlow", "MoneyFlow2"]:
        return DATA_DIR/DATA_MAPPING[tab_name]/(kwargs['date']+'.csv')
    elif tab_name in ["Trading Halt", "ST Stocks"]:
        return DATA_DIR/'dateProcess'/(DATA_MAPPING[tab_name] + '.csv')
    elif 'alpha' in tab_name:
        return ALPHA_DIR/tab_name.split('.')[-1]/(kwargs['date'] + '.csv')

def makeLogPath(dt_string, name):
    date = dt_string.strftime("%Y%m%d")
    timestamp = dt_string.strftime("%Y%m%d%H%M%S")
    (LOG_DIR/date/name).mkdir(parents=True, exist_ok=True)
    return LOG_DIR/date/name/(timestamp+".log")
