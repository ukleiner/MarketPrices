import requests
from lxml import etree

from Chain import Chain

class MatrixChain(Chain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db, username, password, name, chainId, manu=None, itemCodes=None, codeCategoryR=None):
        url = "http://matrixcatalog.co.il/NBCompetitionRegulations.aspx"
        super().__init__(db, url, username, password, name, chainId, manu, itemCodes, codeCategoryR)

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
        data = self._getInfoTable("pricefull")
        # TODO continue from here
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

        self._log(f"searching for table for fiel type {fType}")
        r = self.session.get(self.url, params={
            'code': self.chainId,
            'fileType': fType
            })
        res = r.text
        html = etree.HTML(res)
        table = html.find(".//div[@id='download_content']/table/tbody")
        return(table)
