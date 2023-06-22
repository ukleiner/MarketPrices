from CerberusChain import CerberusChain

class StopMarket(CerberusChain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db):
        url = "https://url.retail.publishedprices.co.il"
        username = "Stop_Market"
        password = ''
        name = 'StopMarket'
        chainId = 7290639000004
        codeCategoryR = re.compile(r'>\d{2,3}<')
        super().__init__(db, url, username, password, name, chainId, codeCategoryR=codeCategoryR)
