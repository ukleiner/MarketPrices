import sqlite3

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
        self.logger(f"Created db version {self.version}")

    def createChains(self):
        query = '''CREATE TABLE IF NOT EXISTS chain (
        id INTEGER PRIMARY KEY,
        Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        chainId INTEGER,
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
        FOREIGN KEY(chain) REFERENCES chain(id)
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
        FOREIGN KEY(chain) REFERENCES chain(id)
        )'''
        self.cur.execute(query)

    def linkSubChainStore(self):
        query = '''CREATE TABLE IF NOT EXISTS store_link (
        id INTEGER PRIMARY KEY,
        Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        subchain INTEGER NOT NULL,
        store INTEGER NOT NULL,
        FOREIGN KEY(subchain) REFERENCES subchain(id)
        FOREIGN KEY(store) REFERENCES store(id)
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
        FOREIGN KEY(chain) REFERENCES chain(id)
        )'''
        self.cur.execute(query)

    def createPrices(self):
        query = '''CREATE TABLE IF NOT EXISTS price (
        id INTEGER PRIMARY KEY,
        Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        store INTEGER NOT NULL,
        item INTEGER NOT NULL,
        update_date DATETIME NOT NULL,
        price REAL,
        FOREIGN KEY(store) REFERENCES store(id)
        FOREIGN KEY(item) REFERENCES storeItem(id)
        )'''
        self.cur.execute(query)

    def createItems(self):
        query = '''CREATE TABLE IF NOT EXISTS item(
        id INTEGER PRIMARY KEY,
        Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        item_name INTEGER NOT NULL
        )'''
        self.cur.execute(query)

    def createItemLinker(self):
        query = '''CREATE TABLE IF NOT EXISTS item_link(
        id INTEGER PRIMARY KEY,
        item INTEGER NOT NULL,
        chainItem INTEGER NOT NULL,
        FOREIGN KEY(item) REFERENCES item(id)
        FOREIGN KEY(chainItem) REFERENCES chainItem(id)
        )'''
        self.cur.execute(query)

