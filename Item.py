class Item:
    def __init__(self, chain, store, xmlObject):
        self.chain = chain
        self.store = store
        self._parse(xmlObject)

    def _parse(self):
        for elem in self.obj.iter():
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

