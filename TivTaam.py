import re

from CerberusChain import CerberusChain

class TivTaam(CerberusChain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db):
        # TODO add filtering, many meat Items
        url = "https://url.retail.publishedprices.co.il"
        username = "TivTaam"
        password = ''
        name = 'TivTaam'
        chainId = 7290873255550
        codeCategoryR = re.compile(r'>\\d{1,7}<')
        super().__init__(db, url, username, password, name, chainId, codeCategoryR=codeCategoryR)
