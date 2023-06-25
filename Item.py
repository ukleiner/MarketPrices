class Item:
    def __init__(self, chain, store, datetime, xmlObject):
        self.chain = chain
        self.store = store
        self.datetime = datetime
        self._strdate = self.datetime.strftime('%Y-%m-%d %H:%M')
        self._parse(xmlObject)

    def getChainItem(self):
        return([self.chain, self.code, self.name, self.manu, self.units])

    def getPriceItem(self, item):
        return([self._strdate, self.store, item, self.update_date, self.price])

    def _parse(self, obj):
        for elem in obj.iter():
            if elem.tag == 'ItemCode':
                self.code = int(elem.text)
            elif elem.tag in  ['ItemName', 'ItemNm']:
                self.name = elem.text
            elif elem.tag == 'ManufacturerName':
                self.manu = elem.text
            elif elem.tag == 'UnitOfMeasure':
                self.units = elem.text
            elif elem.tag == 'UnitOfMeasurePrice':
                self.price = elem.text
            elif elem.tag == 'PriceUpdateDate':
                self.update_date = elem.text
