# A program to collect products price data from supermarkets
'''
    ---------------------
    Parameters:
    Uses:
    =====================
    Return:
    Side effects:
'''
# http://prices.shufersal.co.il/ # for shufersal, no passcode
# http://matrixcatalog.co.il/NBCompetitionRegulations.aspx # victory, no passcode
# http://publishprice.ybitan.co.il/ # yenot bitan, no passcode
# https://url.retail.publishedprices.co.il/login # TivTaam, yohananof, osherad,
# Keshet Teamim, RamiLevi

# Categories
# Price, PriceFull, Promo, PromoFull, Stores

import tracemalloc
import timeit
import time
import datetime

from loguru import logger

from Shufersal import Shufersal
from RamiLevy import RamiLevy
from Store import Store
from DBConn import DB

_fn = "./data/Shufersal/PriceFull7290027600007-001-202306070300.xml"
_storefn = "./data/Shufersal/Stores7290027600007-000-202306110201.xml"
_targetManu = "קטיף."

def SAX_obtain(fn, targetManu):
    '''
        Obtain wanted items from XML file by picking the items while parsing
        ---------------------
        Parameters:
            fn - file name
            targetManu - which manufacturer's items to get
        =====================
        Return:
            list of Item objects
        Side effects:
    '''
    context = ET.iterparse(fn, events=("start", "end"))
    _, root = next(context)
    items = None
    currItem = None
    manuIdentified = False
    relItems = []

    for event, elem in context:
        if items is None:
            if event == "start" and elem.tag == "Items":
                items = elem
                root.clear()
        else:
            if currItem is None:
                if event == "start" and elem.tag == "Item":
                    currItem = elem
            elif not manuIdentified:
                if event == "end" and \
                elem.tag == "ManufacturerName" and \
                elem.text == targetManu:
                        manuIdentified = True
            else:
                if event == "end" and elem.tag == "Item":
                    relItems.append(Item(currItem))
                    currItem = None
                    manuIdentified = False
                    items.clear()
    return(relItems)

def timing_tests(fn, targetManu, n=10):
    '''
        Time different filterting methods
        ---------------------
        Parameters:
            fn - file name
            targetManu - which manufacturer's items to get
        =====================
        Return:
            list of Item objects
        Side effects:
    '''
    res1 = SAX_obtain(fn, targetManu)
    res2 = obtain_items(fn, targetManu)
    print(len(res1), len(res2))
    t1 = timeit.timeit(lambda: SAX_obtain(fn, targetManu), number=n)
    t2 = timeit.timeit(lambda: obtain_items(fn, targetManu), number=n)
    print(t1, t2)

@logger.catch
def main():
    dbc = DB()
    dbc.dbStruct()
    chains = init_chains(dbc)
    # TODO Parallel this
    while True:
        start = datetime.date.today()
        logger.info(f"Scanning for day {start}")
        nextDay = start + datetime.timedelta(1)
        targetTime = datetime.datetime(nextDay.year, nextDay.month, nextDay.day, 4)
        for chain in chains:
            chain.scanStores()
        if datetime.datetime.now() > targetTime:
            pass
        else:
            diff = int((targetTime - datetime.datetime.now()).total_seconds())
            logger.info(f"Sleeping for {diff} seconds up to the next store update")
            time.sleep(diff)

def init_chains(db):
    chains = []
    shufersal = Shufersal(db)
    chains.append(shufersal)
    return chains

@logger.catch
def testing():
    dbc = DB()
    ramiLevy = RamiLevy(dbc)
    ramiLevy.scanStores()
    # ramiLevy.login()
    # ramiLevy.download()

if __name__ == '__main__':
    logger.add("./logs/scanning_{time}.log", rotation="03:00", compression="zip", enqueue=True, filter=lambda record: record["level"].no < 30, format="{time:YYYY-MM-DD HH:mm:ss.SSS}| {message}", level="INFO")
    logger.add("./logs/crash.log", backtrace=True, diagnose=True, level="WARNING")
    logger.info("Starting")
    # main()
    testing()
    logger.info("stopped")
