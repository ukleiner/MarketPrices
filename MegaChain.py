import os
import re
import gzip
import xml.etree.ElementTree as ET
import requests

from lxml import etree
from loguru import logger

from CustomExceptions import WrongChainFileException, NoStoreException, NoSuchStoreException
from Chain import Chain

dateLinkR = re.compile('\d{8}/')
class MegaChain(Chain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db, url, name, chainId, manu=None, itemCodes=None, codeCategoryR=None):
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
        folders = self._getFolderContent(reFilter=dateLinkR)
        relFolders = [folder for folder in folders if self._todatetime(folder[:-1]) > updateDate]
        links = []
        for folder in folders:
            date = self._todatetime(folder[:-1])
            if date < updateDate:
                self._log(f"Stop paging, reached foldr date: {folder}")
                break
            files = self._getFolderContent(folder=folder, reFilter=self.priceR)
            self._log(f"Found {len(files)} links in folder {folder}")
            links = links + [{'link': f'{self.url}/{folder}{file}', 'name': file} for file in files]
        return links, False

    def getStoreFile(self, updating):
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
        folders = self._getFolderContent(reFilter=dateLinkR)
        folder = folders[0]
        folderFiles = self._getFolderContent(reFilter=self.storeR, folder=folder)
        storeFileName = folderFiles[0]
        link = f'{self.url}/{folder}{storeFileName}'
        if updating and os.path.exists(f"{self.dirname}/{storeFileName}.gz"):
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
        with gzip.open(fn, 'rt', encoding='utf-16') as f:
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

    # ========== PRIVATE ==========
    def _getFolderContent(self, reFilter, folder=''):
        '''
            In MegaChain interface get links to data
            ---------------------
            Parameters:
                reFilter - regex precompiled of which links to pick
                folder - where to search
            Uses:
            =====================
            Return:
                list of links
            Side effects:
        '''

        self._log(f"searching for table for files in folder {folder} regex {reFilter}")
        r = self.session.get(f'{self.url}/{folder}')
        res = r.text
        html = etree.HTML(res)
        linksXml = html.findall(".//td[@valign='top']/a")
        if reFilter is None:
            relLinks = linksXml
        else:
            relLinks = [linkXml.attrib['href'] for linkXml in linksXml if reFilter.match(linkXml.attrib['href'])]
        return(relLinks)

class YBitan(MegaChain):
    def __init__(self, db):
        name = "YBitan"
        url = "http://publishprice.ybitan.co.il"
        chainId = 7290725900003
        codeCategoryR = re.compile(r'^1?\d{3}$')
        itemCodes = [7290016334166, 7290003706167, 3560071348007,
                7290000011593]
        super().__init__(db, url, name, chainId, itemCodes=itemCodes, codeCategoryR=codeCategoryR)

class Mega(MegaChain):
    def __init__(self, db):
        name = "Mega"
        url = "http://publishprice.mega.co.il"
        chainId = 7290055700007
        codeCategoryR = re.compile(r'^1\d{3}$')
        super().__init__(db, url, name, chainId, itemCodes=itemCodes, codeCategoryR=codeCategoryR)

class MegaMarket(MegaChain):
    def __init__(self, db):
        name = "MegaMarket"
        url = "http://publishprice.mega-market.co.il"
        chainId = 7290055700014
        codeCategoryR = re.compile(r'^1\d{3}$')
        super().__init__(db, url, name, chainId, itemCodes=itemCodes, codeCategoryR=codeCategoryR)
