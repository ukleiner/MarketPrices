from CerberusChain import CerberusChain

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
        super().__init__(db, url, username, password, name, chainId, manu, itemCodes)
