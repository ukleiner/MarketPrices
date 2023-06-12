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
        con = self.db.getConn()
        cur = con.cursor()
        query = "SELECT id FROM chain WHERE chainId = ?"
        cur.execute(query, (chain,))
        res = cur.fetchone()
        return res.id

    def insertChain(self, chain):
        con = self.db.getConn()
        cur = con.cursor()
        query = "INSERT INTO chain (`chainId`, `chainName`) VALUES(?, ?)"
        cur.execute(query, (chain, self.name))
        con.commit()
        return cur.lastrowid

    def getSubchains(self, chain):
        '''
            Fetch subchains internal Id conversion from chain Id
            ---------------------
            Parameters:
                chain - chain internal ID
            =====================
            Return:
               dict of subchain (external) to subchain (internal)
        '''
        con = self.db.getConn()
        cur = con.cursor()
        query = "SELECT id,subchainId FROM subchain WHERE chain = ?"
        cur.execute(query, (chain,))
        return({sc.subchainId: sc.id for sc in cur.fetchall()})

    def createSubchain(self, chain, subchain, name):
        con = self.db.getConn()
        cur = con.cursor()
        query = "INSERT INTO subchain (`chain`, `subchainId`, `name`) VALUES(?,?,?)"
        cur.execute(query, (chain, subchain, name))
        con.commit()
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
        con = self.db.getConn()
        cur = con.cursor()
        query = "SELECT id, store FROM store WHERE chain = ?"
        cur.execute(query, (chain,))
        return({store.store: store.id for store in cur.fetchall()})

    def createStores(self, stores, storeLinks):
        con = self.db.getConn()
        cur = con.cursor()
        storeQuery = "INSERT INTO store (`chain`, `store`, `name`, `city`) VALUES(?,?,?,?)"
        cur.executemany(storeQuery, stores.values())
        con.commit()

        newStoresQ = f"SELECT id, store FROM store WHERE store IN ({','.join(['?']*len(stores))})"
        cur.execute(newStoresQ, list(stores.keys()))
        storeIds = { store: sid for sid, store in cur.fetchall() }
        realLinks = [[subchain, storeIds[store]] for store, subchain in storeLinks.items()]

        linkQ = "INSERT INTO store_link (`subchain`,`store`) VALUES(?,?)"
        cur.executemany(linkQ, realLinks)
        con.commit()

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
        storeLinks = {}
        for store in storesElem:
            if store.tag == "STORES":
                continue

            storeId = int(store.find("STOREID").text)
            if storeId in stores:
                continue

            subchainId = store.find('SUBCHAINID').text
            if subchainId not in subchains:
                scname = store.find('SUBCHAINNAME').text
                subchain = self.createSubchain(chain, subchainId, scname)
                subchains[subchainId] = subchain

            subchain = subchains[subchainId]
            storeName = store.find("STORENAME").text
            city = store.find("CITY").text

            storesIns[storeId] = [chain, storeId, storeName, city]
            storeLinks[storeId] = subchain

        self.createStores(storesIns, storeLinks)
