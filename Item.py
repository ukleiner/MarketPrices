class Item:
    def __init__(self, chainId, store, datetime, xmlObject):
        self.chainId = chainId
        self.store = store
        self.datetime = datetime
        self._strdate = self.datetime.strftime('%Y-%m-%d %H:%M')
        self._parse(xmlObject)

    def getChainItem(self):
        return([self.chainId, self.code, self.name, self.manu, self.units])

    def getPriceItem(self, item):
        return([self._strdate, self.store, item, self.update_date, self.price])

    def _parse(self, obj):
        for elem in obj.iter():
            if elem.tag == 'ItemCode':
                self.code = int(elem.text)
            # ItemNm for KingStore
            elif elem.tag in  ['ItemName', 'ItemNm']:
                self.name = elem.text
            # ManufactureName for MatrixChain
            elif elem.tag in ['ManufacturerName', 'ManufactureName']:
                self.manu = elem.text
            # UnitQty for MatrixChain
            elif elem.tag in ['UnitOfMeasure', 'UnitQty']:
                self.units = elem.text
            elif elem.tag == 'UnitOfMeasurePrice':
                self.price = elem.text
            elif elem.tag == 'PriceUpdateDate':
                self.update_date = elem.text
