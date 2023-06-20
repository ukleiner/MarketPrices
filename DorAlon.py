from CerberusChain import CerberusChain

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
