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
        folders = self._getDateFolders()
        dates = [folder.text[:-1] for folder in folders]
        self._log(dates)
        raise Exception

        
        table = self._getInfoTable()
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
        folders = self._getFolderContent(reFilter=dateLinkR)
        folder = folders[0]
        folderFiles = self._getFolderContent(reFilter=self.storeR, folder=folder)
        storeFileName = folderFiles[0]
        link = f'{self.url}/{folder}{storeFileName}'
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
        subchains = self._getSubchains(self.chain)
        stores = self._getStores(self.chain)

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
                self.chain = self._getChain(chainId)
            except TypeError:
                self.chain = self._insertChain(chainId)

            storeId = int(store.find("StoreID").text)
            if storeId in stores:
                continue

            subchainId = store.find('SubChainID').text
            if subchainId not in subchains:
                scname = store.find('SubChainName').text
                subchain = self._insertSubchain(self.chain, subchainId, scname)
                subchains[subchainId] = subchain

            subchain = subchains[subchainId]
            storeName = store.find("StoreName").text
            city = store.find("City").text

            storesIns[storeId] = [self.chain, storeId, storeName, city]
            storeLinks[storeId] = subchain

        self._insertStores(storesIns, storeLinks)

    # ========== PRIVATE ==========
    def _getFolderContent(self, reFilter, folder=None):
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
        # r = self.session.get(f'{self.url}/{folder}')
        # res = r.text
        with open("test.html", "r") as f:
            res = f.read()
        html = etree.HTML(res)
        linksXml = html.findall(".//td[@valign='top']/a")
        if reFilter is None:
            relLinks = linksXml
        else:
            relLinks = [linkXml for linkXml in linksXml if reFilter.match(linkXml.attrib['href'])]
        return(relLinks)

class YBitan(MegaChain):
    def __init__(self, db):
        name = "YBitan"
        url = "http://publishprice.ybitan.co.il"
        chainId = "7290725900003"
        super().__init__(db, url, name, chainId)
