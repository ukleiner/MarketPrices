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
        self.context = ET.parse(fn)
        self._store_details(self.context)


    def _store_details(self, context):
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
        self.store = int(context.find('StoreId').text)
        # TODO raise special error if store missing to tirgger store update
        try:
            self.checkStoreExists()
        except TypeError:
            raise NoStoreException

    def checkStoreExists(self):
        con = self.db.getConn()
        cur = con.cursor()
        query = "SELECT id FROM store WHERE store = ?"
        cur.execute(query, (self.store,))
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
        search_path = f'Items/Item/ManufacturerName[.="{self.targetManu}"]...'
        xmlItems = self.context.findall(search_path)
        return([Item(xmlItem) for xmlItem in xmlItems])

