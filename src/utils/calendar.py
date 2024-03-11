
# Standard
import json

#Third party
import pandas as pd

# Local
from pathmgmt import pathmgmt as myPath

def createTradingCalendar(inputfile, outputfile):
    '''from trd_date, we can create a trading calendar for all trading days'''
    CALENDAR = pd.read_csv(myPath.DATA_DIR/'date'/inputfile)
    CALENDAR = [str(d)
                for d in CALENDAR.loc[CALENDAR.is_open == 1].date.tolist()]
    outFolder = myPath.DATA_DIR/'dateProcess'
    outFolder.mkdir(parents=True, exist_ok=True)
    with open(outFolder/outputfile, 'w') as f:
        json.dump(CALENDAR, f)


def readCalendar():
    '''create the calendar if not exists;
    read from jsom file
    '''
    file = myPath.DATA_DIR/'dateProcess'/'calendar.json'
    if not file.exists():
        createTradingCalendar('trd_date.csv', 'calendar.json')
    f = open(file, "r")
    data = json.loads(f.read())
    return data


CALENDAR = readCalendar()
START = '20180101'
END = '20201231'