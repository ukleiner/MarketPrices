import xml.etree.ElementTree as ET
from Item import Item

class Store:
    def __init__(self, fn, targetManu):
        self.fn = fn
        self.manu = targetManu
        self.context = ET.parse(fn)
        self._store_details(self.context)


    def _store_details(self, context):
        self.chain = int(context.find('ChainId').text)
        self.subChain = int(context.find('SubChainId').text)
        self.store = int(context.find('StoreId').text)

    def check_chain_exists(self):
        pass
    def check_subchain_exists(self):
        pass
    def check_store_exists(self):
        pass

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
        xmlItems = context.findall(search_path)
        return([Item(xmlItem) for xmlItem in xmlItems])

