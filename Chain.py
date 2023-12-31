import time
import os
import io
import re
import datetime
from zipfile import ZipFile
import gzip

import xml.etree.ElementTree as ET

import requests
from lxml import etree
from loguru import logger

from CustomExceptions import WrongChainFileException, WrongStoreFileException, NoStoreException, NoSuchStoreException
from Store import Store

MAX_SLEEPS = 3
SLEEP_SECS = 30
GZIP_MAGIC_NUMBER = b'\x1f\x8b'
ZIP_MAGIC_NUMBER = b'PK'
class Chain:
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db, url, username, password, name, chainId, manu=None, itemCodes = None, codeCategoryR=None):
        self.db = db
        self.name = name
        self.chainId = chainId
        self.targetManu = manu
        self.itemCodes = itemCodes
        self.codeCategoryR = codeCategoryR
        self.dirname = f"./data/{name}"
        self.url = url
        self.username = username
        self.password = password

        self.priceR = re.compile('^PriceFull')
        self.storeR = re.compile('^Stores')
        self.dateR = re.compile('-(\d{8})\d{4}')

        if not os.path.exists(self.dirname):
            os.makedirs(self.dirname)
            self._log("Data folder created")
        self._log(f"Construing {self.name} chain with {self.username}:{self.password}@{self.url}, searching for products from {self.targetManu}")

        self.session = self.login()
        try:
           self._setChain()
        except TypeError:
           self.updateChain(updating=False)

    def login(self):
        '''
        Login to site if needed
        ---------------------
        Parameters:
        Uses:
        =====================
        Return:
            session object with relevant cookies
        Side effects:
            downloads files to dirname
        '''
        pass

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
            links, continuePaging = self.download_page(page, updateDate, firstOfLast)
            if len(links) == 0:
                firstOfLast = None
            else:
                firstOfLast = links[0]['name']
            downloaded_files = [self._download_gz(item['name'], item['link'], prior=item.get('prior', None)) for item in links]
            downloaded = downloaded + downloaded_files
        downloaded = [d for d in downloaded if d is not None]
        return(downloaded)

    def download_page(self, page, updateDate=None, firstOfLast=None):
        '''
            Download page with links to FullPrice pages
            impl. per chain subclass
            ---------------------
            Parameters:
                page - page in paging system
                updateDate - earliest date to download
                firstOfLast - first file of last paging, to prevent infinite downloads
            Uses:
            =====================
            Return:
                list of dics containing link to download and file name
            Side effects:
        '''
        pass


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
        priceFiles = [f for f in filenames if self.priceR.match(f)]
        if updateDate is None:
            relFiles = priceFiles
            self._log("No last update time, using all files in folder")
        else:
            self._log(f"last update date {updateDate}, fetching files after that date")
            self._log(f"Starting with {len(priceFiles)} files")
            matchPrice = {f: self._todatetime(self.dateR.search(f).group(1))  for f in priceFiles}

            relFiles = [file for file, date in matchPrice.items() if date > updateDate]
        self._log(f"Fetching {len(relFiles)} files")
        return relFiles

    def scanStores(self, newDay=True):
        '''
            Main entry point
            Scan prices files from stores and inserts prices to db
            ---------------------
            Parameters:
                newDay - should download new files
            Uses:
            =====================
            Return:
            Side effects:
                updates db
        '''
        if newDay:
            newFiles = self.download()
        files = self.fileList()
        missingStore = False
        for fn in files:
            storeFile = f"{self.dirname}/{fn}"
            try:
                store = Store(self.db, storeFile, self.targetManu, self.itemCodes, self.codeCategoryR, self.chainId)
            except NoStoreException:
                self._log(f"Missing store from file {storeFile}")
                try:
                    self.updateChain()
                    # store that was missing hasn't initiated, recap
                    store = Store(self.db, storeFile, self.targetManu, self.itemCodes, self.codeCategoryR, self.chainId)
                except NoSuchStoreException:
                    self._log(f"Store in file {storeFile} missing from latest stores file")
                    missingStore = True
                    # removed store, continue
                except NoStoreException:
                    self._log(f"Store in file {storeFile} missing and a glitch happened")
                    missingStore = True
            except WrongStoreFileException:
                missingStore = True
                self._log(f"Store file {storeFile} can't init a store")

            finally:
                if not missingStore:
                    items = store.obtainItems()
                    if len(items) > 0:
                        prices = store.getPrices(items)
                        store.insertPrices(prices)
                    else:
                        self._log(f"No manufacturer items in this store")
                missingStore = False
                store = None

    def getStoreFile(self, updating):
        '''
            Get file with chain stores for updating
            ---------------------
            Parameters:
                upating - updating or inserting chain, should store file be rejected if downloaded
            Uses:
            =====================
            Return:
                location of stored file
            Side effects:
                Download file with stores data
        '''
        pass

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
        pass

    def updateChain(self, updating=True):
        '''
            Updates chain's stores with new file
            ---------------------
            Parameters:
                updating - bool, should check file store for duplicate or inserting chain
            Uses:
            =====================
            Return:
                Void
            Side effects:
                downloads file and updates db
        '''
        storeFile = self.getStoreFile(updating=updating)
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
        query = "SELECT chainId FROM chain WHERE chainId = ?"
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
        return({subchainId: iid for iid, subchainId in cur.fetchall()})

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
        return({ store: sid for sid, store in cur.fetchall()})

    def _download_gz(self, fn, link, prior=None):
        '''
            Download a gzip file
            ---------------------
            Parameters:
               fn - name to save
               link - where to download from
               prior - some sites (bina chains) require server-side refreshing
            Uses:
            =====================
            Return:
                path to file
            Side effects:
                downloads file
        '''
        self._log(f"Downloading file {link}")
        counter = 0
        while True:
            try:
                if prior is not None:
                    self.session.get(prior, verify=False)
                    self._log("Accessing refreshing point (prior) at {prior}")
                data = self.session.get(link, verify=False)
                break
            except requests.exceptions.ConnectionError:
                if counter < MAX_SLEEPS:
                    counter = counter + 1
                    self._log("ConnectionError with {link}, trying sleeping for {SLEEP_SECS} seconds try #{counter}")
                    time.sleep(SLEEP_SECS)
                else:
                    self._log("Too many ConnectionErrors with {link}, skipping it")
                    return None

        content = data.content
        filename = f'{self.dirname}/{fn}.gz'

        if content[:2] == ZIP_MAGIC_NUMBER:
            self._log(f"File {filename} is in ZIP format and will be unzipped")
            with ZipFile(io.BytesIO(content), "r") as myzip:
                zipList = myzip.infolist()
                with myzip.open(zipList[0]) as f:
                    content = f.read()

        if content[:2] != GZIP_MAGIC_NUMBER:
            self._log(f'magic number {content[:2]}')
            self._log(f"File {filename} is ungzipped and will be compressed")
            content = gzip.compress(content)

        with open(filename, 'wb') as f:
            f.write(content)
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
        self._log(f"Created new chain in db, {chain}")
        return chain

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

    def _todatetime(self, date, typ='str'):
        if typ == 'str':
            return datetime.datetime(int(date[0:4]), int(date[4:6]), int(date[6:8]))
        elif typ == 'cerberus':
            return datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ')

    def _getLatestDate(self):
        con = self.db.getConn()
        cur = con.cursor()
        query = '''SELECT price.filedate
        FROM price
        INNER JOIN chainItem on chainItem.id = price.item
        WHERE chainItem.chain = ?
        ORDER BY price.filedate DESC
        LIMIT 1
        '''
        cur.execute(query, (self.chainId,))
        try:
            sqlUpdateDate, = cur.fetchone()
            updateDate = datetime.datetime.strptime(sqlUpdateDate, "%Y-%m-%d %H:%M")

            # TODO archive used files
        except TypeError as e:
            updateDate = self._todatetime("19700101")
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
