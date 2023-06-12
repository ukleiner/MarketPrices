import xml.etree.ElementTree as ET
from Item import Item

class Store:
    def __init__(self, db, fn, targetManu):
        '''
        Initialize a store inside a chain
        ---------------------
        Parameters:
            db -  handle to DB
            fn - filename
            targetManu - manufacturer to taret it's products
        =====================
        Return:
            Store object
        '''
        self.db = db
        self.fn = fn
        self.manu = targetManu
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
        root = context.getroot()
        self.chain = int(root.find('ChainId').text)
        self.subChain = int(root.find('SubChainId').text)
        self.store = int(root.find('StoreId').text)

    def check_subchain_exists(self):
        cur = self.db.getCursor()
        query = "SELECT id FROM subchain WHERE subchainId = ?"
        cur.execute(query, (self.subChain,))
        res = cur.fetchone()
        return res.id

    def check_store_exists(self):
        cur = self.db.getCursor()
        query = "SELECT id FROM store WHERE store = ?"
        cur.execute(query, (self.subChain,))
        res = cur.fetchone()
        return res.id

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
        search_path = f'Items/Item/ManufacturerName[.="{targetManu}"]...'
        root = context.getroot()
        xmlItems = root.findall(search_path)
        return([Item(xmlItem) for xmlItem in xmlItems])

