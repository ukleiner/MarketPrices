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
import xml.etree.ElementTree as ET

from Item import Item



_fn = "./data/Shufersal/PriceFull7290027600007-001-202306070300.xml"
_targetManu = "קטיף."

def obtain_items(fn, targetManu):
    '''
        Obtain wanted items from XML file by parsing the whole file and extracting the items
        ---------------------
        Parameters:
            fn - file name
            targetManu - which manufacturer's items to get
        =====================
        Return:
            list of Item objects
        Side effects:
    '''
    context = ET.parse(fn)
    search_path = f'Items/Item/ManufacturerName[.="{targetManu}"]...'
    xmlItems = context.findall(search_path)
    return([Item(xmlItem) for xmlItem in xmlItems])

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
    t1 = timeit.timeit(lambda: ip(fn), number=n)
    t2 = timeit.timeit(lambda: dp(fn), number=n)
    print(t1, t2)
