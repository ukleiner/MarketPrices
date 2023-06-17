import os
import re
import datetime
import gzip
import xml.etree.ElementTree as ET

import requests
from lxml import etree
from loguru import logger

from CustomExceptions import WrongChainFileException, NoStoreException, NoSuchStoreException
from Store import Store

class Chain:
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db, url, username, password, name, chainId, manu):
        self.db = db
        self.name = name
        self.chainId = chainId
        self.chain = None
        self.targetManu = manu
        self.dirname = f"./data/{name}"
        self.url = url
        self.username = username
        self.password = password

        self.priceR = re.compile('^PriceFull')
        self.storeR = re.compile('^Stores')
        self.dateR = re.compile('-(\d{8})\d{4}')

        self._log(f"Construing {self.name} chain with {self.username}:{self.password}@{self.url}, searching for products from {self.targetManu}")

        try:
            self._setChain()
        except TypeError:
            # TODO alert the user in some way about this
            # so it can trigger obtainStores
            self.updateChain()

    def download(self):
        '''
            Download new data files
            Implemented by each Chain class separately
            ---------------------
            Parameters:
            Uses:
            =====================
            Return:
                downloaded - files downloaded
            Side effects:
                downloads files to dirname
        '''
        page = 0
        downloaded = []
        continuePaging = True
        firstOfLast = None
        updateDate = self._getLatestDate()
        self._log(f"looking at date after {updateDate}")
        while continuePaging:
            page = page + 1
            links, continuePaging = self.download_page(page, firstOfLast)
            if len(links) == 0:
                firstOfLast = None
            else:
                firstOfLast = firstLink['name']
            downloaded_files = [self._download_gz(item['name'], item['link']) for item in links]
            downloaded = downloaded + downloaded_files
        return(downloaded)

    def download_page(self, page, updateDate=None, firstOfLast=None):
        continuePaging=True
        if updateDate is None:
            updateDate = self._getLatestDate()
        table = self._getInfoTable(f"FileObject/UpdateCategory/?catID=2&storeId=0&sort=Time&sortdir=DESC&page={page}")
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
                        print(elem.tag, elem.text, firstOfLast)
                        fileDate = self._todatetime(self.dateR.search(elem.text).group(1))
                        if fileDate <= updateDate or firstOfLast == elem.text:
                            continuePaging = False
                            self._log(f"Stop paging, reached fileDate: {fileDate}, repeated fetched: {firstOfLast == elem.text}")
                            break
                        priceFileName = elem.text

                if priceFileName is not None and link is not None:
                    links.append({'link': link, 'name': priceFileName})
                    logger.info(f"Found price file {priceFileName}")
                    skip = True
        return links, continuePaging


    def fileList(self):
        '''
            Returns a list of price files with newer dates than the latest price in the db
            ---------------------
            Parameters:
            Uses:
            =====================
            Return:
                list of filenames to scan
            Side effects:
        '''
        updateDate = self._getLatestDate()
        filenames = next(os.walk(self.dirname), (None, None, []))[2]
        if updateDate is None:
            priceFiles = [f for f in filenames if self.priceR.match(f)]
            self._log("No last update time, using all files in folder")
        else:
            matchPrice = {self._todatetime(self.dateR.search(f).group(1)): f for f in filenames}
            priceFiles = [file for key, file in matchPrice.items() if key > updateDate]
            self._log(f"last update date {updateDate}, fetching files after that date")
        self._log(f"Fetching {len(priceFiles)} files")
        return priceFiles

    def scanStores(self):
        '''
            Main entry point
            Scan prices files from stores and inserts prices to db
            ---------------------
            Parameters:
            Uses:
            =====================
            Return:
            Side effects:
                updates db
        '''
        newFiles = self.download()
        files = self.fileList()
        for fn in files:
            try:
                store = Store(self.db, fn, self.targetManu, self.chainId, self.chain)
            except NoStoreException:
                self._log(f"Missing store from file {fn}")
                try:
                    self.updateChain()
                    # store that was missing hasn't initiated, recap
                    store = Store(self.db, fn, self.targetManu, self.chainId, self.chain)
                except NoSuchStoreException:
                    self._log(f"Store in file {fn} missing from latest stores file")
                    # removed store, continue
                    continue
            finally:
                items = store.obtainItems()
                prices = store.getPrices(items)
                store.logPrices(prices)

    def getStoreFile(self):
        '''
            Get file with chain stores for updating
            ---------------------
            Parameters:
            Uses:
            =====================
            Return:
                location of stored file
            Side effects:
                Download file with stores data
        '''
        table = self._getInfoTable("FileObject/UpdateCategory?catID=5")

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
        if os.path.exists(f"{self.dirname}/{storeFileName}"):
            raise NoSuchStoreException

        return(self._download_gz(storeFileName, link))

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
        self._log(f"Obtaining stores from {fn}")
        with gzip.open(fn, 'rt') as f:
            data = f.read()
            context = ET.fromstring(data)
        chainId = int(context.find('.//CHAINID').text)
        if self.chain is not None and chainId != self.chain:
            # chainId in file should be like setup
            logger.error(f"Chain {self.chainId}: file with wrong chain Id supplied {fn}")
            raise WrongChainFileException
        try:
            chain = self._getChain(chainId)
        except TypeError:
            chain = self._insertChain(chainId)

        subchains = self._getSubchains(chain)
        stores = self._getStores(chain)

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
                subchain = self._insertSubchain(chain, subchainId, scname)
                subchains[subchainId] = subchain

            subchain = subchains[subchainId]
            storeName = store.find("STORENAME").text
            city = store.find("CITY").text

            storesIns[storeId] = [chain, storeId, storeName, city]
            storeLinks[storeId] = subchain

        self._insertStores(storesIns, storeLinks)

    def updateChain(self):
        '''
            Updates chain's stores with new file
            ---------------------
            Parameters:
            Uses:
            =====================
            Return:
                Void
            Side effects:
                downloads file and updates db
        '''
        storeFile = self.getStoreFile()
        self.obtainStores(storeFile)
     # ========== PRIVATE ==========
    def _getChain(self, chain):
        '''
            get internal chain id, throws if no chain
            ---------------------
            Parameters:
            Uses:
            =====================
            Return:
                db chain id
            Side effects:
                throws if chain not set
        '''
        con = self.db.getConn()
        cur = con.cursor()
        query = "SELECT id FROM chain WHERE chainId = ?"
        cur.execute(query, (chain,))
        cid, = cur.fetchone()
        return cid

    def _getSubchains(self, chain):
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

    def _getStores(self, chain):
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

    def _getInfoTable(self, local_path):
        url =f'http://{self.url}/{local_path}'
        self._log(f"searching for table {url}")
        r = requests.get(url)
        res = r.text
        html = etree.HTML(res)
        table = html.find("body/div/table/tbody")
        return(table)

    def _download_gz(self, fn, link):
        '''
            Download a gzip file
            ---------------------
            Parameters:
               fn - name to save
               link - where to download from
            Uses:
            =====================
            Return:
                path to file
            Side effects:
                downloads file
        '''
        self._log(f"Downloading file {link}")
        data = requests.get(link)
        filename = f'{self.dirname}/{fn}.gz'
        with open(filename, 'wb') as f:
            f.write(data.content)
            self._log(f"Saved to {filename}")
        return filename


    def _setChain(self):
        self.chain = self._getChain(self.chainId)

    def _insertChain(self, chain):
        '''
            Inserts the chain to the db
            ---------------------
            Parameters:
            Uses:
            =====================
            Return:
                new chain internal id
            Side effects:
                updates db
        '''
        con = self.db.getConn()
        cur = con.cursor()
        query = "INSERT INTO chain (`chainId`, `chainName`) VALUES(?, ?)"
        cur.execute(query, (chain, self.name))
        con.commit()
        self._log(f"Created new chain in db, {cur.lastrowid}")
        return cur.lastrowid

    def _insertSubchain(self, chain, subchain, name):
        '''
            Inserts subchain to db
            ---------------------
            Parameters:
            Uses:
            =====================
            Return:
                subchain internal id
            Side effects:
                updates db
        '''
        con = self.db.getConn()
        cur = con.cursor()
        query = "INSERT INTO subchain (`chain`, `subchainId`, `name`) VALUES(?,?,?)"
        cur.execute(query, (chain, subchain, name))
        con.commit()
        self._log(f"Created new subchain in db, {cur.lastrowid}")
        return cur.lastrowid

    def _todatetime(self, date):
        return datetime.datetime(int(date[0:4]), int(date[4:6]), int(date[6:8]))

    def _getLatestDate(self):
        con = self.db.getConn()
        cur = con.cursor()
        query = '''SELECT price.update_date
        FROM price
        INNER JOIN item on price.item = item.id
        INNER JOIN item_link on item.id = item_link.item
        INNER JOIN chainItem ON item_link.chainItem = chainItem.id
        WHERE chainItem.chain = ?
        ORDER BY price.update_date DESC
        LIMIT 1
        '''
        cur.execute(query, (self.chain,))
        try:
            updateDate, = cur.fetchone()
            # TODO filter used files
        except TypeError:
            updateDate = self._todatetime("20230618")
        return updateDate

    def _insertStores(self, stores, storeLinks):
        '''
            Inserts stores to db
            ---------------------
            Parameters:
            Uses:
            =====================
            Return:
                None
            Side effects:
                updates db
        '''
        con = self.db.getConn()
        cur = con.cursor()
        storeQuery = "INSERT INTO store (`chain`, `store`, `name`, `city`) VALUES(?,?,?,?)"
        cur.executemany(storeQuery, stores.values())
        con.commit()
        self._log(f"Logged {cur.rowcount} new stores")

        newStoresQ = f"SELECT id, store FROM store WHERE store IN ({','.join(['?']*len(stores))})"
        cur.execute(newStoresQ, list(stores.keys()))
        storeIds = { store: sid for sid, store in cur.fetchall() }
        realLinks = [[subchain, storeIds[store]] for store, subchain in storeLinks.items()]

        linkQ = "INSERT INTO store_link (`subchain`,`store`) VALUES(?,?)"
        cur.executemany(linkQ, realLinks)
        con.commit()

    def _log(self, mes):
        logger.info(f"Chain {self.chainId}: {mes}")


