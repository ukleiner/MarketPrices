import os
import re
from io import BytesIO
from zipfile import ZipFile
import requests

import xml.etree.ElementTree as ET
from loguru import logger

from CustomExceptions import WrongChainFileException, NoStoreException, NoSuchStoreException
from Chain import Chain

csrfTokenR = re.compile('<meta name="csrftoken" content="(.*)"')
class CerberusChain(Chain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db, username, password, name, chainId, manu=None, itemCodes=None, codeCategoryR=None):
        url = "https://url.retail.publishedprices.co.il"
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
        session = requests.Session()
        loginCsrfToken = self._getCSRF(session=session, typ="login")

        loginUrl = f"{self.url}/login/user"
        r = session.post(loginUrl,
                data={
                    'username': self.username,
                    'password': self.password,
                    'csrftoken': loginCsrfToken
                },
            verify=False)
        self._log(f"Post to {self.username}:{self.password}@{loginUrl} with response code {r.status_code}")
        return session

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
        csrfToken = self._getCSRF()
        url = f"{self.url}/file/json/dir"
        data = self.session.post(url, data={
            'csrftoken':csrfToken,
            'sSearch': 'PriceFull',
            'iDisplayLength': 100000,
            }, verify=False)
        json_data = data.json()
        allFilesData = {f['DT_RowId']: self._todatetime(f['time'], typ='cerberus') for f in json_data['aaData']}
        filesData = [{ 'link': f'{self.url}/file/d/{rid}', 'name': rid[:-3]} for rid, date in allFilesData.items() if date > updateDate]
        return filesData, False

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
        csrfToken = self._getCSRF()
        url = f"{self.url}/file/json/dir"
        data = self.session.post(url, data={
            'csrftoken':csrfToken,
            'sSearch': 'Stores',
            'iDisplayLength': 100000,
            }, verify=False)
        json_data = data.json()
        storeFiles = { f['DT_RowId']: self._todatetime(self.dateR.search(f['DT_RowId']).group(1)) for f in json_data['aaData'] if self.storeR.match(f['DT_RowId'])}
        storeFile = max(storeFiles, key=storeFiles.get)
        storeFileName = storeFile # xml
        link = f'{self.url}/file/d/{storeFile}'
        if os.path.exists(f"{self.dirname}/{storeFileName}"):
            raise NoSuchStoreException

        return(self._download_xml(storeFileName, link))

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
        with open(fn, encoding='utf-16') as f:
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

    # ========== PRIVATE ==========
    def _getCSRF(self, session=None, typ="regular"):
        if session is None:
            session = self.session
        url = self.url
        if typ == "regular":
            url = f"{self.url}/file"
        elif typ == "login":
            url = f"{self.url}/login"
        else:
            # TODO special exception
            raise Exception
        csrfPage = session.get(url, verify=False)
        csrfPageContent = csrfPage.text
        return csrfTokenR.search(csrfPageContent).group(1)

### SubClasses ###
class RamiLevy(CerberusChain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db):
        url = "https://url.retail.publishedprices.co.il"
        username = "RamiLevi"
        password = ''
        name = 'RamiLevy'
        chainId = 7290058140886
        manu = "ביכורי השקמה"
        itemCodes = [7290000012346]
        codeCategoryR = re.compile("7290000000")
        super().__init__(db, url, username, password, name, chainId, codeCategoryR=codeCategoryR)

class Dabach(CerberusChain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db):
        url = "https://url.retail.publishedprices.co.il"
        username = "SalachD"
        password = '12345'
        name = 'Dabach'
        chainId = 7290526500006
        codeCategoryR = re.compile("729000000")
        super().__init__(db, url, username, password, name, chainId, codeCategoryR=codeCategoryR)

class DorAlon(CerberusChain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db):
        # TODO add filtering, many meat Items
        url = "https://url.retail.publishedprices.co.il"
        username = "doralon"
        password = ''
        name = 'DorAlon'
        chainId = 7290492000005
        manu = "חקלאי"
        itemCodes = None
        super().__init__(db, url, username, password, name, chainId, manu, itemCodes)

class HaziHinam(CerberusChain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db):
        url = "https://url.retail.publishedprices.co.il"
        username = "HaziHinam"
        password = ''
        name = 'HaziHinam'
        chainId = 7290700100008
        codeCategoryR = re.compile(r'^\d{1,4}')
        super().__init__(db, url, username, password, name, chainId, codeCategoryR=codeCategoryR)

class Keshet(CerberusChain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db):
        url = "https://url.retail.publishedprices.co.il"
        username = "Keseht"
        password = ''
        name = 'Keseht'
        chainId = 7290785400000
        codeCategoryR = re.compile(r'>1?\d{1,2}<')

        super().__init__(db, url, username, password, name, chainId, codeCategoryR)

class OsherAd(CerberusChain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db):
        # TODO add filtering, many meat Items
        url = "https://url.retail.publishedprices.co.il"
        username = "osherad"
        password = ''
        name = 'OsherAd'
        chainId = 7290103152017
        manu = None
        itemCodes = None
        codeCategoryR = re.compile("7290000999")
        super().__init__(db, url, username, password, name, chainId, manu, codeCategory=codeCategoryR)

class StopMarket(CerberusChain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db):
        url = "https://url.retail.publishedprices.co.il"
        username = "Stop_Market"
        password = ''
        name = 'StopMarket'
        chainId = 7290639000004
        codeCategoryR = re.compile(r'>\d{2,3}<')
        super().__init__(db, url, username, password, name, chainId, codeCategoryR=codeCategoryR)

class TivTaam(CerberusChain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db):
        url = "https://url.retail.publishedprices.co.il"
        username = "TivTaam"
        password = ''
        name = 'TivTaam'
        chainId = 7290873255550
        codeCategoryR = re.compile(r'\d{1,7}')
        super().__init__(db, url, username, password, name, chainId, codeCategoryR=codeCategoryR)

class Yohananof(CerberusChain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db):
        # TODO add filtering, many meat Items
        url = "https://url.retail.publishedprices.co.il"
        username = "yohananof"
        password = ''
        name = 'Yohananof'
        chainId = 7290803800003
        manu = "משתנה"
        itemCodes = None
        super().__init__(db, url, username, password, name, chainId, manu=manu, itemCodes=itemCodes)
