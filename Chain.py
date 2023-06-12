import xml.etree.ElementTree as ET

class Chain:
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db, url, username, password, name):
        self.db = db
        self.name = name
        pass

    def get():
        pass

    def getChain(self, chain):
        cur = self.db.getCursor()
        query = "SELECT id FROM chain WHERE chainId = ?"
        cur.execute(query, (chain,))
        res = cur.fetchone()
        return res.id

    def insertChain(self, chain):
        cur = self.db.getCursor()
        query = "INSERT INTO chain (`chainId`, `chainName`) VALUES(?, ?)"
        cur.execute(query, (chain, self.name))
        return cur.lastrowid

    def getSubChains(self, chain):
        '''
            Fetch subchains internal Id conversion from chain Id
            ---------------------
            Parameters:
                chain - chain internal ID
            =====================
            Return:
               dict of subchain (external) to subchain (internal)
        '''
        cur = self.db.getCursor()
        query = "SELECT id,subchainId FROM subchain WHERE chain = ?"
        cur.execute(query, (chain,))
        return({sc.subchainId: sc.id for sc in cur.fetchall()})

    def createSubchain(self, chain, subchain, name):
        cur = self.db.getCursor()
        query = "INSERT INTO subchain (`chain`, `subchainId`, `name`) VALUES(?,?,?)"
        cur.execute(query, (chain, subchain, name))
        return cur.lastrowid

    def getStores(self, chain):
        '''
            Fetch storeinternal Id conversion from chain Id
            ---------------------
            Parameters:
                chain - internal chain id
            =====================
            Return:
            dict store (external) to store (internal)
        '''
        cur = self.db.getCursor()
        query = "SELECT id, store FROM store WHERE chain = ?"
        cur.execute(query, (chain,))
        return({store.store: store.id for store in cur.fetchall()})

    def createStores(self, stores, storeLinks):
        cur = self.db.getCursor()
        storeQuery = "INSERT INTO store (`chain`, `store`, `name`, city`) VALUES(?,?,?,?)"
        cur.executemany(query, stores)
        newStoresQ = "SELECT id, store FROM store WHERE store IN ?"
        cur.execute(newStoresQ, list(stores.keys()))
        storeIds = { s.store: s.id for s in cur.fetchall() }
        realLinks = [[subchain, storeIds[store]] for store, subhcain in storeLinks.items()]
        linkQ = "INSERT INTO store_link (`subchain`,`store`) VALUES(?,?)"
        cur.executemany(linkQ, realLinks)

    def obtainStores(self, fn):
        '''
            Obtain chain stores
            ---------------------
            Parameters:
                fn - file name
            =====================
            Return:
                list of Item objects
        '''
        context = ET.parse(fn)
        print(context.find(".//CHAINID"))
        c = 0
        chainId = int(context.find('.//CHAINID').text)
        try:
            chain = self.getChain(chainId)
        except AttributeError:
            chain = self.insertChain(chainId)

        subchains = self.getSubchains(chain)
        stores = self.getStores(chain)

        storesElem = context.find('.//STORES')
        storesIns = {}
        for store in storesElem.iter():
            storeId = store.find("STOREID")
            if storeId in stores:
                continue

            subchainId = store.find('SUBCHAINID')
            if subchainId not in subchains:
                scname = store.find('SUBCHAINNAME')
                subchain = self.createSubchain(chain, subchainId, name)
                subchains[subchainId] = subchain

            subchain = subchains[subchainId]
            storeName = store.find("STORENAME")
            city = store.find("CITY")

            storesIns[storeId] = [chain, storeId, storeName, city]
            storeLinks[storeId] = subchain

        self.creteStores(storesIns, storeLinks)
