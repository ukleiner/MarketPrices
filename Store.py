import gzip
import xml.etree.ElementTree as ET

from loguru import logger

from CustomExceptions import WrongChainFileException, NoStoreException
from Item import Item

class Store:
    def __init__(self, db, fn, targetManu, chainId, chain):
        '''
        Initialize a store inside a chain
        ---------------------
        Parameters:
            db -  handle to DB
            fn - filename
            targetManu - manufacturer to taret it's products
            chainId - chain external ID
            chain - chain internal ID
        =====================
        Return:
            Store object
        '''
        self.db = db
        self.fn = fn
        self.manu = targetManu
        self.chainId = chainId
        self.chain = chain
        logger.info(f"Start store for chain {self.chainId} using file {self.fn}")

        with gzip.open(fn, 'rt') as f:
            data = f.read()
            self.context = ET.fromstring(data)
        self._storeDetails(self.context)
        logger.info(f"Store {self.storeId}/{self.subchain}/{self.chainId}")


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
        logger.info(f"Obtaining items for store {self.storeId}@{self.chainId} from manufactuer {self.manu}")
        search_path = f'Items/Item/ManufacturerName[.="{self.manu}"]...'
        xmlItems = self.context.findall(search_path)
        return([Item(self.chain, xmlItem) for xmlItem in xmlItems])

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
        logger.info(f"got {len(itemsObj)} items from {self.storeId}@{self.chainId}")
        itemCodes = list(itemsObj.keys())
        ids_codes = self._getItemIds(itemCodes)
        ids, codes = zip(*ids_codes)
        missing_codes = list(set(itemCodes)-set(codes))

        if len(missing_codes) > 0:
            missing_items = [itemsObj[code] for code in missing_codes]
            self._insertChainItems(missing_items)
            ids_codes = self._getItemCodes(itemCodes)
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
        price (`item`,`update_date`,`price`)
        VALUES(?,?,?)'''
        cur.executemany(query, prices)
        con.commit()
        insPrices = cur.rowcount
        logger.info(f"Logged {insPrices} item prices for store {self.storeId}@{self.chainId}")



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
        fileChain = int(context.find('ChainId').text)
        if fileChain != self.chainId:
            raise WrongChainFileException
        self.subChain = int(context.find('SubChainId').text)
        self.storeId = int(context.find('StoreId').text)
        # TODO raise special error if store missing to tirgger store update
        try:
            self.store = self.getStore()
        except TypeError:
            raise NoStoreException

    def _getItemIds(self, itemsCodes):
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
        query = f"SELECT id, code FROM chainItem WHERE chain = ? AND code IN ({','.join(['?']*len(itemCodes))})"
        cur.execute(query, (self.chain, itemCodes))
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
        itemsList = [item.getChainItem(self.chain) for item in items]
        con = self.db.getConn()
        cur = con.cursor()
        query = '''
        INSERT INTO
        storeItem (`chain`, `code`, `name`, `manufacturer`, `units`)
        VALUES (?,?,?,?,?)
        '''
        cur.executemany(query, itemsList)
        con.commit()
        logger.info(f"Inserted {cur.rowcount} store items for {self.storeId}@{self.chainId}")
