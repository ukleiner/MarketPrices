class Item:
    def __init__(self, xmlObject):
        self.obj = xmlObject
        self._parse()

    def _parse(self):
        # PriceUpdateDate
        # ItemName
        # ManufacturerName
        # ManufactureCountry
        # UnitOfMeasure
        # UnitOfMeasurePrice
#      <PriceUpdateDate>2022-03-27 15:09</PriceUpdateDate>
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

