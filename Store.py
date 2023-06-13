import gzip
import xml.etree.ElementTree as ET

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

        with gzip.open(fn, 'rt') as f:
            data = f.read()
            self.context = ET.fromstring(data)
        self._storeDetails(self.context)


    def checkStoreExists(self):
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
        search_path = f'Items/Item/ManufacturerName[.="{self.manu}"]...'
        xmlItems = self.context.findall(search_path)
        return([Item(self.chain, xmlItem) for xmlItem in xmlItems])

    def getPrices(self, items):
        con = self.db.getConn()
        cur = con.cursor()
        itemsObj = {item.code: item for item in items}
        itemCodes = list(itemsObj.keys())
        ids_codes = self._getItemIds(itemCodes)
        ids, codes = zip(*ids_codes)
        missing_codes = list(set(itemCodes)-set(codes))

        if len(missing_codes) > 0:
            missing_items = [itemsObj[code] for code in missing_codes]
            self._createChainItems(missing_items)
            ids_codes = self._getItemCodes(itemCodes)
        prices = [itemsObj[code].getPriceItem(iid) for iid, code in ids_codes]
        return(prices)

    def logPrices(self, prices):
        pass


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
            self.store = self.checkStoreExists()
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
        cur.execute(query, (self.chain, itemCodes)
        return cur.fetchall()

    def _createChainItems(self, missing_codes, missing_items):
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
