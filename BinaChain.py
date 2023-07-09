# TODO downloading twice file for same date, fix?
import os
import gzip
import datetime
import re
import json
import xml.etree.ElementTree as ET

import requests

from CustomExceptions import WrongChainFileException, NoStoreException, NoSuchStoreException
from Chain import Chain

class BinaChain(Chain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db, url, name, chainId, manu=None, itemCodes=None, codeCategoryR=None):
        username = None
        password = None
        super().__init__(db, url, username, password, name, chainId, manu=manu, itemCodes=itemCodes, codeCategoryR=codeCategoryR)

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
                1. list of dics containing link to download and file name, and refreshing paths (priors)
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
            prior = f'{self.url}/Download.aspx?FileNm={fn}'
            links.append({ 'prior': prior, 'link': link, 'name': fn[:-3] })

        updateDate = fileDate
        return links, continuePaging

    def getStoreFile(self, updating=True):
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
        prior = f'{self.url}/Download.aspx?FileNm={storeFileName}'
        if updating and os.path.exists(f"{self.dirname}/{storeFileName}"):
            raise NoSuchStoreException

        return(self._download_gz(storeFileName[:-3], link, prior=prior))

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

        chainId = int(context.find('.//ChainId').text)
        if self.chainId is not None and chainId != self.chainId:
            # chainId in file should be like setup
            logger.error(f"Chain {self.chainId}: file with wrong chain Id {chainId} supplied {fn}")
            raise WrongChainFileException
        try:
            self.chainId = self._getChain(chainId)
        except TypeError:
            self.chainId = self._insertChain(chainId)

        subchains = self._getSubchains(self.chainId)
        stores = self._getStores(self.chainId)

        subchainsElem = context.find('.//SubChains')
        storesIns = {}
        storeLinks = {}
        for sc in subchainsElem:
            subchainId = int(sc.find('SubChainId').text)
            if subchainId in subchains:
                subchain = subchains[subchainId]
            else:
                subchainName = int(sc.find('SubChainName').text)
                subchain = self._insertSubchain(self.chainId, subchainId, subchainName)
                subchains[subchainId] = subchain

            storesElem = sc.find('Stores')
            for store in storesElem:
                storeId = int(store.find("StoreId").text)
                if storeId in stores:
                    continue
                storeName = store.find("StoreName").text
                city = store.find("City").text

                storesIns[storeId] = [self.chainId, storeId, storeName, city]
                storeLinks[storeId] = subchain

        self._insertStores(storesIns, storeLinks)

class KingStore(BinaChain):
    def __init__(self, db):
        url = "https://www.kingstore.co.il/Food_Law"
        name = 'KingStore'
        chainId = 7290058108879
        itemCodes = [7290016057072, 7290016334500, 7290016334166,
                7290016334616]
        codeCategoryR = re.compile(r'^777\d{3}')
        super().__init__(db, url, name, chainId, codeCategoryR=codeCategoryR)

class ZolVeBegadol(BinaChain):
    def __init__(self, db):
        url = "https://zolvebegadol.binaprojects.com"
        name = 'ZolVeBegadol'
        chainId = 7290058173198
        codeCategoryR = re.compile(r'10[123]{1}\d{2}')
        super().__init__(db, url, name, chainId, codeCategoryR=codeCategoryR)

class ShukHayir(BinaChain):
    def __init__(self, db):
        url = "https://shuk-hayir.binaprojects.com"
        name = 'ShukHayir'
        chainId = 7290058148776
        codeCategoryR = re.compile(r'\d{3}')
        super().__init__(db, url, name, chainId, codeCategoryR=codeCategoryR)

class ShefaBirkat(BinaChain):
    def __init__(self, db):
        url = "https://shefabirkathashem.binaprojects.com"
        name = 'ShefaBirkat'
        chainId = 7290058134977
        codeCategoryR = re.compile(r'\d{3,4}')
        super().__init__(db, url, name, chainId, codeCategoryR=codeCategoryR)
