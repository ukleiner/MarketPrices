from CerberusChain import CerberusChain

class RamiLevy(CerberusChain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db):
        url = "https://url.retail.publishedprices.co.il"
        username = "RamiLevi"
        password = ''
        name = 'RamiLevy'
        chainId = 7290058140886
        manu = "ביכורי השקמה"
        itemCodes = [7290000012346]
        codeCategoryR = re.compile("7290000000")
        super().__init__(db, url, username, password, name, chainId, codeCategoryR=codeCategoryR)
