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

    def obtainStores(fn):
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
        chainId = int(context.find('CHAINID').text)
        try:
            chain = self.getChain(chainId)
        except AttributeError:
            chain = self.insertChain(chainId)

        storesElem = context.find('STORES')
        subchains = {}
        stores = []
        for storein storesElem.iter():
            subchain = store.find('SUBCHAINID')
            if subchain not in subchains:
                subchains[subchain] = [
                        subchin
                        ]
