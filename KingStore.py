import os
import datetime
import re
import json
from zipfile import ZipFile
import xml.etree.ElementTree as ET

import requests

from CustomExceptions import WrongChainFileException, NoStoreException, NoSuchStoreException
from Chain import Chain

class KingStore(Chain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db):
        url = "https://www.kingstore.co.il/Food_Law"
        username = None
        password = None
        name = 'KingStore'
        chainId = 7290058108879
        codeCategoryR = re.compile(r'777\d{3}')
        super().__init__(db, url, username, password, name, chainId, codeCategoryR=codeCategoryR)

    def login(self):
        return requests.Session()

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
                1. list of dics containing link to download and file name
                2. should the paging continue
            Side effects:
        '''
        continuePaging=True
        if updateDate is None:
            updateDate = self._getLatestDate()
        params = {
                'WFileType': 4
        }
        url = f'{self.url}/MainIO_Hok.aspx'
        r = self.session.get(url, params=params)
        filesJson = r.json()
        links = []

        continuePaging = len(filesJson) == 0

        fileDate = None
        for f in filesJson:
            fn = f['FileNm']
            try:
                fileDate = self._todatetime(self.dateR.search(fn).group(1))
            except AttributeError:
                self._log(f"Tried to find date in {fn}")
                continue

            if fileDate < updateDate:
                self._log(f"Stop paging, reached fileDate: {fileDate}")
                break

            link = f'{self.url}/Download/{fn}'
            links.append({ 'link': link, 'name': fn[:-3] })

        updateDate = fileDate
        return links, continuePaging

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
        params = {
                'WFileType': 1
        }
        url = f'{self.url}/MainIO_Hok.aspx'
        r = self.session.get(url, params=params)
        filesJson = r.json()
        fileJson = filesJson[0]

        storeFileName = fileJson['FileNm']
        link = f'{self.url}/Download/{storeFileName}'
        if os.path.exists(f"{self.dirname}/{storeFileName}"):
            raise NoSuchStoreException

        return(self._download_gz(storeFileName[:-3], link))

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
        with ZipFile(fn) as myzip:
            realName = f"{fn.split('/')[-1][:-3]}.xml"
            with myzip.open(realName) as f:
                data = f.read()
                context = ET.fromstring(data)

        chainId = int(context.find('.//ChainId').text)
        if self.chainId is not None and chainId != self.chainId:
            # chainId in file should be like setup
            logger.error(f"Chain {self.chainId}: file with wrong chain Id {chainId} supplied {fn}")
            raise WrongChainFileException
        try:
            self.chain = self._getChain(chainId)
        except TypeError:
            self.chain = self._insertChain(chainId)

        subchains = self._getSubchains(self.chain) # TODO check this
        stores = self._getStores(self.chain)

        subchainsElem = context.find('.//SubChains')
        storesIns = {}
        storeLinks = {}
        for sc in subchainsElem:
            subchainId = int(sc.find('SubChainId').text)
            if subchainId in subchains:
                subchain = subchains[subchainId]
            else:
                subchainName = int(sc.find('SubChainName').text)
                subchain = self._insertSubchain(self.chain, subchainId, subchainName)
                subchains[subchainId] = subchain

            storesElem = sc.find('Stores')
            for store in storesElem:
                storeId = int(store.find("StoreId").text)
                if storeId in stores:
                    continue
                storeName = store.find("StoreName").text
                city = store.find("City").text

                storesIns[storeId] = [self.chain, storeId, storeName, city]
                storeLinks[storeId] = subchain

        self._insertStores(storesIns, storeLinks)