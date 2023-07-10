# A program to collect products price data from supermarkets
'''
    ---------------------
    Parameters:
    Uses:
    =====================
    Return:
    Side effects:
'''
# http://matrixcatalog.co.il/NBCompetitionRegulations.aspx # victory, no passcode
# http://publishprice.ybitan.co.il/ # yenot bitan, no passcode

import tracemalloc
import timeit
import time
import datetime

from loguru import logger

from Shufersal import Shufersal
from MegaChain import YBitan, Mega, MegaMarket
from BinaChain import KingStore, ShukHayir, ShefaBirkat, ZolVeBegadol
from CerberusChain import RamiLevy, Yohananof, Dabach, DorAlon, HaziHinam, Keshet, OsherAd, StopMarket, TivTaam, Yohananof
from MatrixChain import Victory, HaShuk
from DBConn import DB

TESTING = False

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
def main(targetTime=0):
    dbc = DB()
    dbc.dbStruct()
    chains = init_chains(dbc)
    # TODO Parallel this
    while True:
        if datetime.datetime.now() < targetTime:
            diff = int((targetTime - datetime.datetime.now()).total_seconds())
            logger.info(f"Sleeping for {diff} seconds up to the next store update")
            time.sleep(diff)
        start = datetime.date.today()
        logger.info(f"Scanning for day {start}")
        nextDay = start + datetime.timedelta(1)
        targetTime = datetime.datetime(nextDay.year, nextDay.month, nextDay.day, 4)
        for chain in chains:
            chain.scanStores()

@logger.catch
# patch to re-read YBitan data
def patch_YBitan():
    dbc = DB()
    dbc.dbStruct()
    chains = init_chains(dbc)
    logger.info(f"Scanning for YBitan")
    ybitan = YBitan(db)
    ybitan.scanStores(newDay=False)
    logger.info(f"Finished YBitan patching")
    main(datetime.datetime(2023, 7, 11, 4))

def init_chains(db):
    chains = []
# from MegaChain import YBitan, Mega, MegaMarket
# from BinaChain import KingStore, ShukHayir, ShefaBirkat, ZolVeBegadol
# from CerberusChain import RamiLevy, Yohananof, Dabach, DorAlon, HaziHinam, Keshet, OsherAd, StopMarket, TivTaam, Yohananof
# from MatrixChain import Victory, HaShuk
    chains.append(Shufersal(db))
    chains.append(RamiLevy(db))
    chains.append(Yohananof(db))
    chains.append(KingStore(db))
    chains.append(YBitan(db))
    chains.append(Victory(db))
    return chains

@logger.catch
def testing():
    dbc = DB()
    yBitan = YBitan(dbc)
    yBitan.scanStores()
    # shukHayir = ShukHayir(dbc)
    # shukHayir.scanStores()
    # victory = Victory(dbc)
    # tab = victory._getInfoTable(None)

if __name__ == '__main__':
    if not TESTING:
        logger.remove()
    logger.add("./logs/scanning_{time}.log", rotation="03:00", compression="zip", enqueue=True, filter=lambda record: record["level"].no < 30, format="{time:YYYY-MM-DD HH:mm:ss.SSS}| {message}", level="INFO")
    logger.add("./logs/crash.log", backtrace=True, diagnose=True, level="WARNING")
    logger.info("Starting")
    if TESTING:
        testing()
    else:
        main()
    logger.info("stopped")
