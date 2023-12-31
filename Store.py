import re
import gzip
from zipfile import ZipFile
import datetime
import xml.etree.ElementTree as ET

from loguru import logger

from CustomExceptions import WrongChainFileException, WrongStoreFileException, NoStoreException
from Item import Item

dateR = re.compile('-(\d{12})')
class Store:
    def __init__(self, db, fn, targetManu, itemCodes, codeCategoryR, chainId):
        '''
        Initialize a store inside a chain
        ---------------------
        Parameters:
            db -  handle to DB
            fn - filename
            targetManu - manufacturer to target it's products
            itemCodes - specific item codes to search for
            chainId - chain external ID
            chain - chain internal ID
        =====================
        Return:
            Store object
        '''
        self.db = db
        self.fn = fn
        self.manu = targetManu
        self.itemCodes = itemCodes
        self.codeCategoryR = codeCategoryR
        self.chainId = chainId
        logger.info(f"Start store for chain {self.chainId} using file {self.fn}")
        date = dateR.search(fn).group(1)
        self.datetime = datetime.datetime(int(date[0:4]), int(date[4:6]), int(date[6:8]), int(date[8:10]), int(date[10:12]))


        with gzip.open(fn, 'rt') as f:
            data = f.read()
            self.context = ET.fromstring(data)

        try:
            self._storeDetails(self.context)
        except AttributeError:
            logger.info(f"File {fn} not an xml Store file")
            raise WrongStoreFileException
        self._log(f"Inited")


    def getStore(self):
        '''
            get internal store id, throws if no store
            ---------------------
            Parameters:
            Uses:
            =====================
            Return:
                db store id
            Side effects:
                throws if store not set
        '''
        con = self.db.getConn()
        cur = con.cursor()
        query = "SELECT id FROM store WHERE store = ?"
        cur.execute(query, (self.storeId,))
        sid, = cur.fetchone()
        return sid

    def obtainItems(self):
        '''
            Obtain wanted items from XML file by parsing the whole file and extracting the items
            ---------------------
            Parameters:
            =====================
            Return:
                list of Item objects
            Side effects:
        '''
        manuItems = []
        codeItems = []
        lenItems = []
        catItems = []
        # Check if MatrixChain
        if self.context.tag == 'Prices':
            self._log('Handling MatrixChain type')
            manuSearch = "Products/Product/ManufactureName"
            itemSearch = "Products/Product"
        else:
            manuSearch = "Items/Item/ManufacturerName"
            itemSearch = "Items/Item"
        if self.manu is not None:
            self._log(f"Obtaining items from manufactuer {self.manu}")
            manuSearchPath = f'{manuSearch}[.="{self.manu}"]...'
            manuXmlItems = self.context.findall(manuSearchPath)
            manuItems = [Item(self.chainId, self.store, self.datetime, xmlItem) for xmlItem in manuXmlItems]
            self._log(f"Found {len(manuItems)} manufacturer items")

        if self.itemCodes is not None:
            for code in self.itemCodes:
                self._log(f"Obtaining item with code {code}")
                _items = self.context.findall(f'{itemSearch}/ItemCode[.="{code}"]...')
                if len(_items) > 0:
                    xmlItem = _items[0]
                    codeItems.append(Item(self.chainId, self.store, self.datetime, xmlItem))
            self._log(f"Found {len(codeItems)} items with code {code}")

        if self.codeCategoryR is not None:
            self._log(f"Obtaining item matching code category {self.codeCategoryR}")
            # Check if MatrixChain
            _catItems = self.context.findall(itemSearch)
            for _item in _catItems:
                code = _item.find('ItemCode').text
                if self.codeCategoryR.search(code) is not None:
                    catItems.append(Item(self.chainId, self.store, self.datetime, _item))
            self._log(f"Found {len(catItems)} code category items")


        items = manuItems + codeItems + lenItems + catItems
        return items

    def getPrices(self, items):
        '''
            Get list of item prices ready for insert in db
            ---------------------
            Parameters:
                items - list of Item objects
            Uses:
            =====================
            Return:
                list of price (list of data ready for db insert)
            Side effects:
        '''
        con = self.db.getConn()
        cur = con.cursor()
        itemsObj = {item.code: item for item in items}
        self._log(f"got {len(itemsObj)} items")
        itemCodes = list(itemsObj.keys())
        ids_codes = self._getItemIds(itemCodes)
        try:
            ids, codes = zip(*ids_codes)
        except ValueError:
            codes = []
        missing_codes = list(set(itemCodes)-set(codes))

        if len(missing_codes) > 0:
            self._log(f"Missing {len(itemsObj)} chain items")
            missing_items = [itemsObj[code] for code in missing_codes]
            self._insertChainItems(missing_items)
            ids_codes = self._getItemIds(itemCodes)
        self._log(f"got {len(ids_codes)} id codes")
        prices = [itemsObj[code].getPriceItem(iid) for iid, code in ids_codes]
        return(prices)

    def insertPrices(self, prices):
        '''
            Insert prices to db
            ---------------------
            Parameters:
                prices - list of price list
            Uses:
            =====================
            Return:
            Side effects:
                update db
        '''
        # prices should be with verified item internal id
        # No try except  for fast fail
        con = self.db.getConn()
        cur = con.cursor()
        query = '''INSERT INTO
        price (`filedate`, `store`, `item`,`update_date`,`price`)
        VALUES(?,?,?,?,?)'''
        cur.executemany(query, prices)
        try:
            con.commit()
        except sqlite3.IntegrityError as e:
            logger.exception(f"Store {self.storeId}@{self.chainId}: {mes}")
        insPrices = cur.rowcount
        self._log(f"Logged {insPrices} item prices")

    # ===========PRIVATE=========
    def _storeDetails(self, context):
        '''
            Get store details from a store prices file
            ---------------------
            Parameters:
                context - xml object
            =====================
            Return:
                Nothing
            Side effects:
                sets the store values in the object
        '''
        try:
            fileChain = int(context.find('ChainID').text)
            self.subChain = int(context.find('SubChainID').text)
            self.storeId = int(context.find('StoreID').text)
        except AttributeError:
            fileChain = int(context.find('ChainId').text)
            self.subChain = int(context.find('SubChainId').text)
            self.storeId = int(context.find('StoreId').text)
        if fileChain != self.chainId:
            raise WrongChainFileException
        try:
            self.store = self.getStore()
        except TypeError:
            raise NoStoreException

    def _getItemIds(self, itemCodes):
        '''
            get internal item ids for itemsCodes for a specific store
            ---------------------
            Parameters:
                context - xml object
            =====================
            Return:
                Nothing
            Side effects:
                sets the store values in the object
        '''
        con = self.db.getConn()
        cur = con.cursor()
        query = f"SELECT id, code FROM chainItem WHERE chain = ? AND code IN ({','.join(['?']*len(itemCodes))})"
        cur.execute(query, [self.chainId] + itemCodes)
        return cur.fetchall()

    def _insertChainItems(self, items):
        '''
            Insert missing items to db
            ---------------------
            Parameters:
                items - list of Item objects to insert
            Uses:
            =====================
            Return:
            Side effects:
                update db
        '''
        itemsList = [item.getChainItem() for item in items]
        con = self.db.getConn()
        cur = con.cursor()
        query = '''
        INSERT INTO
        chainItem (`chain`, `code`, `name`, `manufacturer`, `units`)
        VALUES (?,?,?,?,?)
        '''
        cur.executemany(query, itemsList)
        con.commit()
        self._log(f"Inserted {cur.rowcount} store items")

    def _log(self, mes):
        logger.info(f"Store {self.storeId}@{self.chainId}: {mes}")
