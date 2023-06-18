import sqlite3

from loguru import logger

class DB:
    def __init__(self):
        self.version = 1
        self.con = sqlite3.connect('agriItems.db')

    def getConn(self):
        return self.con

    def dbStruct(self):
        self.cur = self.con.cursor()
        self.createChains()
        self.createSubchains()
        self.createStores()
        self.linkSubChainStore()
        self.createChainItems()
        self.createPrices()
        self.createItems()
        self.createItemLinker()
        self.con.commit()
        logger.info(f"Created db version {self.version}")

    def createChains(self):
        query = '''CREATE TABLE IF NOT EXISTS chain (
        id INTEGER PRIMARY KEY,
        Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        chainId INTEGER UNIQUE,
        chainName TEXT
        )
        '''
        self.cur.execute(query)

    def createSubchains(self):
        '''
        type is a column I added to describe what type of chain it is
        '''
        query = '''CREATE TABLE IF NOT EXISTS subchain (
        id INTEGER PRIMARY KEY,
        Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        chain INTEGER NOT NULL,
        subchainId INTEGER,
        name TEXT,
        type INTEGER,
        FOREIGN KEY(chain) REFERENCES chain(id),
        UNIQUE(chain, subchainId)
        )
        '''
        self.cur.execute(query)

    def createStores(self):
        query = '''CREATE TABLE IF NOT EXISTS store (
        id INTEGER PRIMARY KEY,
        Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        chain INTEGER NOT NULL,
        store INTEGER NOT NULL,
        name TEXT NOT NULL,
        city TEXT,
        FOREIGN KEY(chain) REFERENCES chain(id),
        UNIQUE(chain, store)
        )'''
        self.cur.execute(query)

    def linkSubChainStore(self):
        query = '''CREATE TABLE IF NOT EXISTS store_link (
        id INTEGER PRIMARY KEY,
        Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        subchain INTEGER NOT NULL,
        store INTEGER NOT NULL,
        FOREIGN KEY(subchain) REFERENCES subchain(id)
        FOREIGN KEY(store) REFERENCES store(id),
        UNIQUE(subchain, store)
        )'''
        self.cur.execute(query)


    def createChainItems(self):
        query = '''CREATE TABLE IF NOT EXISTS chainItem (
        id INTEGER PRIMARY KEY,
        Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        chain INTEGER NOT NULL,
        code INTEGER NOT NULL,
        name TEXT NOT NULL,
        manufacturer TEXT NOT NULL,
        units TEXT,
        FOREIGN KEY(chain) REFERENCES chain(id),
        UNIQUE(chain, code)
        )'''
        self.cur.execute(query)

    def createPrices(self):
        query = '''CREATE TABLE IF NOT EXISTS price (
        id INTEGER PRIMARY KEY,
        Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        filedate DATETIME NOT NULL,
        store INTEGER NOT NULL,
        item INTEGER NOT NULL,
        update_date DATETIME NOT NULL,
        price REAL,
        FOREIGN KEY(store) REFERENCES store(id)
        FOREIGN KEY(item) REFERENCES chainItem(id),
        UNIQUE(filedate, store, item)
        )'''
        self.cur.execute(query)

    def createItems(self):
        query = '''CREATE TABLE IF NOT EXISTS item(
        id INTEGER PRIMARY KEY,
        Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        item_name INTEGER NOT NULL UNIQUE
        )'''
        self.cur.execute(query)

    def createItemLinker(self):
        query = '''CREATE TABLE IF NOT EXISTS item_link(
        id INTEGER PRIMARY KEY,
        item INTEGER NOT NULL,
        chainItem INTEGER NOT NULL,
        FOREIGN KEY(item) REFERENCES item(id)
        FOREIGN KEY(chainItem) REFERENCES chainItem(id),
        UNIQUE(item, chainItem)
        )'''
        self.cur.execute(query)

