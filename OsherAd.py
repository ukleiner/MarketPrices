import re

from CerberusChain import CerberusChain

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
