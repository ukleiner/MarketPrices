import re

from CerberusChain import CerberusChain

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
