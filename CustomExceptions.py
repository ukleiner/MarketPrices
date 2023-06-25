'''Raised when a a file of a different chain is used'''
class WrongChainFileException(Exception):
    def __init__(self):
        pass

'''Raised when the file cant init a Store'''
class WrongStoreFileException(Exception):
    def __init__(self):
        pass
'''Raised when a prices file has a store not in the db'''
class NoStoreException(Exception):
    def __init__(self):
        pass

'''Raised when a store is missing from the most up-to-date stores file'''
class NoSuchStoreException(Exception):
    def __init__(self):
        pass
