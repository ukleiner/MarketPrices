import os
import re
import gzip
import xml.etree.ElementTree as ET
import requests

from lxml import etree
from loguru import logger

from CustomExceptions import WrongChainFileException, NoStoreException, NoSuchStoreException
from Chain import Chain

removeExtrasR = re.compile(r'-001$')
class MatrixChain(Chain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db, name, chainId, manu=None, itemCodes=None, codeCategoryR=None):
        url = "http://matrixcatalog.co.il"
        username = None
        password = None
        super().__init__(db, url, username, password, name, chainId, manu=manu, itemCodes=itemCodes, codeCategoryR=codeCategoryR)

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
        return requests.Session()

    def download_page(self, page=1, updateDate=None, firstOfLast=None):
        '''
            get PriceFull file list created after updateDate
            ---------------------
            Parameters:
                updateDate - update date of reference
                csrfToken - cerberus token for identification
            Uses:
            =====================
            Return:
                1. list of dics containing link to download and file name
                2. False, shouldn't continue paging
            Side effects:
        '''
        if updateDate is None:
            updateDate = self._getLatestDate()
        table = self._getInfoTable("pricefull")
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
                aElem = elem.find('a')
                if aElem is not None:
                    link = aElem.get('href')
                    link = "".join(link.split()).replace('\\', '/')
                    link = f'{self.url}/{link}'
                else:
                    if elem.text is not None and self.priceR.search(elem.text):
                        fileDate = self._todatetime(self.dateR.search(elem.text).group(1))
                        if fileDate <= updateDate or firstOfLast == elem.text:
                            continuePaging = False
                            self._log(f"Stop paging, reached fileDate: {fileDate}, repeated fetched: {firstOfLast == elem.text}")
                            break
                        priceFileName = removeExtrasR.sub('', elem.text)

                if priceFileName is not None and link is not None:
                    links.append({'link': link, 'name': priceFileName})
                    self._log(f"Found price file {priceFileName}")
                    skip = True
        return links, False

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
        table = self._getInfoTable("storesfull")
        storeFileName = None
        link = None
        for elem in table.iter():
            if elem.tag == "td":
                aElem = elem.find('a')
                if aElem is not None:
                    link = aElem.get('href')
                    link = "".join(link.split()).replace('\\', '/')
                    link = f'{self.url}/{link}'
                else:
                    if elem.text is not None and self.storeR.search(elem.text):
                        storeFileName = elem.text
                if storeFileName is not None and link is not None:
                    break
        if os.path.exists(f"{self.dirname}/{storeFileName}.gz"):
            raise NoSuchStoreException

        return(self._download_gz(storeFileName, link))

    def obtainStores(self, fn):
        '''
            Obtain chain stores
            Has manual override for Victory, wrong store file ID
            ---------------------
            Parameters:
                fn - file name
            =====================
            Return:
                list of Item objects
        '''
        self._log(f"Obtaining stores from {fn}")
        subchains = self._getSubchains(self.chainId)
        stores = self._getStores(self.chainId)

        storesIns = {}
        storeLinks = {}

        with gzip.open(fn, 'rt') as f:
            data = f.read()
            context = ET.fromstring(data)
        storesElem = context.find('.//Branches')
        for store in storesElem:
            chainId = int(store.find('ChainID').text)
            # TODO manual override for Victory, wrong chain ID
            if self.name == 'Victory':
                chainId = self.chainId
            if self.chainId is not None and chainId != self.chainId:
                # chainId in file should be like setup
                logger.error(f"Chain {self.chainId}: file with wrong chain Id {chainId} supplied {fn}")
                raise WrongChainFileException
            try:
                self.chainId = self._getChain(chainId)
            except TypeError:
                self.chainId = self._insertChain(chainId)

            storeId = int(store.find("StoreID").text)
            if storeId in stores:
                continue

            subchainId = store.find('SubChainID').text
            if subchainId not in subchains:
                scname = store.find('SubChainName').text
                subchain = self._insertSubchain(self.chainId, subchainId, scname)
                subchains[subchainId] = subchain

            subchain = subchains[subchainId]
            storeName = store.find("StoreName").text
            city = store.find("City").text

            storesIns[storeId] = [self.chainId, storeId, storeName, city]
            storeLinks[storeId] = subchain

        self._insertStores(storesIns, storeLinks)

    # ========== PRIVATE ==========
    def _getInfoTable(self, fType):
        '''
            In MatrixChain interface get information table
            ---------------------
            Parameters:
                local_path - xml path to table
            Uses:
            =====================
            Return:
                xml tree of the table
            Side effects:
        '''

        self._log(f"searching for table for file type {fType}")
        r = self.session.get(f'{self.url}/NBCompetitionRegulations.aspx', params={
            'code': self.chainId,
            'fileType': fType
            })
        res = r.text
        html = etree.HTML(res)
        table = html.find(".//div[@id='download_content']/table")
        return(table)

### SubClasses ###
class Victory(MatrixChain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db):
        name = 'Victory'
        chainId = 7290696200003
        itemCodes = [7290000654973, 7290010051267, 7290010051335,
                7290017291123, 7290017291123, 7290010051168]
        codeCategoryR = re.compile(r'^2\d{3}')
        super().__init__(db, name, chainId, codeCategoryR=codeCategoryR, itemCodes = itemCodes)

class HaShuk(MatrixChain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db):
        name = 'HaShuk'
        chainId = 7290661400001
        itemCodes = [7290006265500, 7290016334494, 7290011276485,
                7290016334166, 7290016334616, 7290018825006,
                7290017586007]
        codeCategoryR = re.compile(r'^\d{3}')
        super().__init__(db, name, chainId, itemCodes=itemCodes, codeCategoryR=codeCategoryR)
