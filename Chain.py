import os
import re
import datetime
import gzip
import xml.etree.ElementTree as ET

from lxml import etree
import requests

from CustomExceptions import WrongChainFileException, NoStoreException
from Store import Store

class Chain:
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db, url, username, password, name, chainId):
        self.db = db
        self.name = name
        self.chainId = chainId
        self.dirname = f"./data/{name}"
        self.url = url
        self.username = username
        self.password = password

        self.priceR = re.compile('^PriceFull')
        self.storeR = re.compile('^Stores')
        self.dateR = re.compile('-(\d{8})\d{4}')

        try:
            self.setChain()
        except TypeError:
            # TODO alert the user in some way about this
            # so it can trigger obtainStores
            self.updateChain()

    def download(self):
        url =f'http://{self.url}/FileObject/UpdateCategory/?catID=2&storeId=0&sort=Time&sortdir=ASC'
        updateDate = self._getLatestDate()
        '''
        r = requests.get(url)
        res = r.text
        with open('shufres.html', 'w') as f:
            f.write(res)
        '''
        with open('shufres.html', 'r') as f:
            res = f.read()
        html = etree.HTML(res)
        table = html.find("body/div/table/tbody")
        links = []
        link = None
        priceFileName = None
        skip = False
        for elem in table.iter():
            if elem.tag == "tr":
                link = None
                priceFileName = None
                skip = False
            elif skip:
                continue
            elif elem.tag == "td":
                if elem.text is None:
                    a_elem = elem.find('a')
                    if a_elem is None:
                        continue
                    link = a_elem.get('href')
                    link = "".join(link.split())
                else:
                    if self.priceR.search(elem.text):
                        fileDate = self._todatetime(self.dateR.search(elem.text).group(1))
                        if fileDate <= updateDate:
                            skip = True
                            continue
                        priceFileName = elem.text

                if priceFileName is not None and link is not None:
                    links.append({'link': link, 'name': priceFileName})
                    skip = True
        downloaded_files = [self._download_gz(item['name'], item['link']) for item in links]
        print(downloaded_files)

    def getStoreFile(self):
        url =f'http://{self.url}/FileObject/UpdateCategory?catID=5'

        r = requests.get(url)
        html = r.text
        table = etree.HTML(html).find("body/div/table/tbody")
        storeFileName = None
        link = None

        for elem in table.iter():
            if elem.tag == "td":
                if elem.text is None:
                    link = elem.find('a').get('href')
                    link = "".join(link.split())
                else:
                    if self.storeR.search(elem.text):
                        storeFileName = elem.text
                if storeFileName is not None and link is not None:
                    break
        return(self._download_gz(storeFileName, link))

    def _download_gz(self, fn, link):
        data = requests.get(link)
        filename = f'{self.dirname}/{fn}.gz'
        with open(filename, 'wb') as f:
            f.write(data.content)
        return filename

    def updateChain(self):
        storeFile = self.getStoreFile()
        self.obtainStores(storeFile)

    def setChain(self):
        self.chain = self.getChain(self.chainId)

    def fileList(self):
        updateDate = self.getLatestDate()
        filenames = next(os.walk(self.dirname), (None, None, []))[2]
        if updateDate is None:
            priceFiles = [f for f in filenames if self.priceR.match(f)]
        else:
            matchPrice = {self._todatetime(self.dateR.search(f).group(1)): f for f in priceFiles}
            return [file for key, file in matchPrice if key > updateDate]

    def scanStores(self):
        repeat = True
        files = self.fileList()
        for fn in files:
            try:
                store = Store(self.db, fn, self.targetManu, self.chainId)
                items = store.obtainItems()
            except NoStoreException:
                # TODO handle this
                pass

    def getChain(self, chain):
        con = self.db.getConn()
        cur = con.cursor()
        query = "SELECT id FROM chain WHERE chainId = ?"
        cur.execute(query, (chain,))
        cid, = cur.fetchone()
        return cid

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
        with gzip.open(fn, 'rt') as f:
            data = f.read()
            context = ET.parse(data)
        chainId = int(context.find('.//CHAINID').text)
        if chainId != self.chain:
            # chainId in file should be like setup
            raise WrongChainFileException
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

    def insertChain(self, chain):
        con = self.db.getConn()
        cur = con.cursor()
        query = "INSERT INTO chain (`chainId`, `chainName`) VALUES(?, ?)"
        cur.execute(query, (chain, self.name))
        con.commit()
        return cur.lastrowid

    def createSubchain(self, chain, subchain, name):
        con = self.db.getConn()
        cur = con.cursor()
        query = "INSERT INTO subchain (`chain`, `subchainId`, `name`) VALUES(?,?,?)"
        cur.execute(query, (chain, subchain, name))
        con.commit()
        return cur.lastrowid

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


     # ========== PRIVATE ==========
    def _todatetime(self, date):
        return datetime.datetime(int(date[0:4]), int(date[4:6]), int(date[6:8]))

    def _getLatestDate(self):
        con = self.db.getConn()
        cur = con.cursor()
        query = '''SELECT price.update_date
        FROM price
        INNER JOIN item on price.item = item.id
        INNER JOIN item_link on item.id = item_link.item
        INNER JOIN storeItem ON item_link.storeItem = storeItem.id
        INNER JOIN store ON storeItem.store = store.id
        WHERE store.chain = ?
        ORDER BY price.update_date DESC
        LIMIT 1
        '''
        cur.execute(query, (self.chain,))
        try:
            updateDate, = cur.fetchone()
            # TODO filter used files
        except TypeError:
            updateDate = self._todatetime("19700101")
        return updateDate

