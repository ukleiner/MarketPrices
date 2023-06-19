import os
import re
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

    def filesInfo(self):
        # get PriceFull file list
        # csrftoken, sSearc="PriceFull"
        # post to: url.retail.publishedprices.co.il/file/json/dir
        # get filename, filter and than request 
        # zipname: "Archivename.zip", cd:"/", csrftoken, ID:[files .gz]
        csrfToken = self._getCSRF()
        url = f"{self.url}/file/json/dir"
        data = self.session.post(url, data={'csrftoken':csrfToken, 'sSearch': 'PriceFull' }, verify=False)
        with open("tmp_res.json", 'w') as f:
            f.write(data.text)
            self._log(f"Saved to tmp_res.json")

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
        table = self._getInfoTable(f"FileObject/UpdateCategory/?catID=2&storeId=0&sort=Time&sortdir=DESC&page={page}")
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
                if elem.text is None:
                    a_elem = elem.find('a')
                    if a_elem is None:
                        continue
                    link = a_elem.get('href')
                    link = "".join(link.split())
                else:
                    if self.priceR.search(elem.text):
                        fileDate = self._todatetime(self.dateR.search(elem.text).group(1))
                        self._log(f'fd {fileDate} ud {updateDate}')
                        if fileDate <= updateDate or firstOfLast == elem.text:
                            continuePaging = False
                            self._log(f"Stop paging, reached fileDate: {fileDate}, repeated fetched: {firstOfLast == elem.text}")
                            break
                        priceFileName = elem.text

                if priceFileName is not None and link is not None:
                    links.append({'link': link, 'name': priceFileName})
                    self._log(f"Found price file {priceFileName}")
                    skip = True
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
        table = self._getInfoTable("FileObject/UpdateCategory?catID=5")

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
