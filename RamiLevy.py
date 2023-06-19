import os
import re
from io import BytesIO
from zipfile import ZipFile
import requests

from lxml import etree

from Chain import Chain

csrfTokenR = re.compile('<meta name="csrftoken" content="(.*)"')
class RamiLevy(Chain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db):
        url = "https://url.retail.publishedprices.co.il"
        username = "RamiLevi"
        password = ''
        name = 'RamiLevy'
        chainId = 7290058140886
        manu = "קטיף."
        super().__init__(db, url, username, password, name, chainId, manu)
        # self._log(self.session.cookies)

    def login(self):
        # download self.url
        # find csrftoken meta tag and extract csrftoken
        # send to self.url/user a post request with 
        # username: usenrmae; password: password; csrftoken: csrftoken
        # Set-Cookie cftpSID
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
        # get PriceFull file list
        # csrftoken, sSearc="PriceFull"
        # post to: url.retail.publishedprices.co.il/file/json/dir
        # get filename, filter and than request 
        # zipname: "Archivename.zip", cd:"/", csrftoken, ID:[files .gz]
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
            'sSearch': 'PriceFull',
            'iDisplayLength': 100000,
            }, verify=False)
        json_data = data.json()
        storeFiles = [{ f['DT_RowId']: self._todatetime(self.dateR.search(f['DT_RowId']).group(1)) } for f in json_data['aaData'] if self.storeR.match(f['DT_RowId'])]
        storeFile = max(storeFiles, key=storeFiles.get)
        storeFileName = storeFile[:-3]
        link = f'{self.url}/file/d/{storeFile}'
        if os.path.exists(f"{self.dirname}/{storeFileName}.gz"):
            raise NoSuchStoreException

        return(self._download_gz(storeFileName, link))

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

    def _getInfoTable(self, local_path):
        '''
            In Shufersal interface get information table
            ---------------------
            Parameters:
                local_path - xml path to table
            Uses:
            =====================
            Return:
                xml tree of the table
            Side effects:
        '''

        url =f'{self.url}/{local_path}'
        self._log(f"searching for table {url}")
        r = requests.get(url)
        res = r.text
        html = etree.HTML(res)
        table = html.find("body/div/table/tbody")
        return(table)
