from CerberusChain import CerberusChain

class Keshet(CerberusChain):
    '''
    The basic functions each Chain should implement
    '''
    def __init__(self, db):
        url = "https://url.retail.publishedprices.co.il"
        username = "Keseht"
        password = ''
        name = 'Keseht'
        chainId = 7290785400000
        codeCategoryR = re.compile(r'>1?\d{1,2}<')

        super().__init__(db, url, username, password, name, chainId, codeCategoryR)
