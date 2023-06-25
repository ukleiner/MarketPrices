import requests
from lxml import etree

from Chain import Chain

class MatrixChain(Chain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db, name, chainId, manu=None, itemCodes=None, codeCategoryR=None):
        url = "http://matrixcatalog.co.il/NBCompetitionRegulations.aspx"
        username = None
        password = None
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
        table = self._getInfoTable("pricefull")
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
                        if fileDate <= updateDate or firstOfLast == elem.text:
                            continuePaging = False
                            self._log(f"Stop paging, reached fileDate: {fileDate}, repeated fetched: {firstOfLast == elem.text}")
                            break
                        priceFileName = elem.text

                if priceFileName is not None and link is not None:
                    links.append({'link': link, 'name': priceFileName})
                    self._log(f"Found price file {priceFileName}")
                    skip = True
        return filesData, continuePaging

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
                if elem.text is None:
                    link = elem.find('a').get('href')
                    link = "".join(link.split())
                else:
                    if self.storeR.search(elem.text):
                        storeFileName = elem.text
                if storeFileName is not None and link is not None:
                    break
        if os.path.exists(f"{self.dirname}/{storeFileName}.gz"):
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
        '''
        with open("test.html", "w") as f:
            f.write(res)
        with open("test.html", "r") as f:
           res = f.read()
       '''
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
        chainId = 7290803800003
        manu = "ביכורי השדה צפון 1994 ג.ד. בעמ"
        super().__init__(db, name, chainId, manu=manu)
