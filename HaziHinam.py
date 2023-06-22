from CerberusChain import CerberusChain

class HaziHinam(CerberusChain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db):
        # TODO add filtering, many meat Items
        url = "https://url.retail.publishedprices.co.il"
        username = "HaziHinam"
        password = ''
        name = 'HaziHinam'
        chainId = 7290700100008
        manu = None
        itemCodes = None
        codeLength = 4
        super().__init__(db, url, username, password, name, chainId, manu, itemCodes, codeLength)
