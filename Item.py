class Item:
    def __init__(self, chain, xmlObject):
        self.chain = chain
        self._parse(xmlObject)

    def getChainItem(self):
        return([self.chain, self.code, self.name, self.manu, self.units])

    def getPriceItem(self, item):
        return([item, self.update_date, self.price])

    def _parse(self, obj):
        for elem in obj.iter():
            if elem.tag == 'ItemCode':
                self.code = elem.text
            elif elem.tag == 'ItemName':
                self.name = elem.text
            elif elem.tag == 'ManufacturerName':
                self.manu = elem.text
            elif elem.tag == 'UnitOfMeasure':
                self.units = elem.text
            elif elem.tag == 'UnitOfMeasurePrice':
                self.price = elem.text
            elif elem.tag == 'PriceUpdateDate':
                self.update_date = elem.text
