import json
from csv import DictWriter

class Convert:

    """ A Convert class to handle file type conversions.

    This class will be used to handle the tasks inputted in the convert tab of the application. """

    def to_csv(self, fp, new_fp):
        #new_fp = newDir + fp[:fp.index(".")] + ".csv"
        first = True
        with open(fp, 'rb') as input, open(new_fp, 'w') as output:
            json_array = json.load(input)
            for json_object in json_array:
                writer = DictWriter(output, json_object.keys())
                if first:
                    writer.writeheader()
                    first = False
                writer.writerow(json_object)

    def to_json(self, fp, newDir):
        pass

    def to_dta(self, fp, newDir):
        pass

    def concat_csv(self, first, second, new_fp):
        with open(new_fp, 'w') as new:
            with open(first, 'r') as first:
                for row in first:
                    new.write(row)
            with open(second, 'r') as second:
                next(second) #Remove second header
                for row in second:
                    new.write(row)