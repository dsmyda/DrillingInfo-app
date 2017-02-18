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

if __name__ == "__main__":
    import os
    convert = Convert()
    old = [file for file in os.listdir("/home/dsmyda/PycharmProjects") if file.endswith(".csv")]
    new = [file for file in os.listdir("/home/dsmyda/PycharmProjects/Permits1") if file in old]
    new.sort()
    old.sort()
    #for file in zip(old,new):
    convert.concat_csv("/home/dsmyda/PycharmProjects/tmp/NewMexicoperm.csv", "/home/dsmyda/PycharmProjects/CLI/NewMexicoperm.csv", '/home/dsmyda/PycharmProjects/tmp1/NewMexioperm.csv')
        #convert.to_csv("/home/dsmyda/PycharmProjects/Permits/"+file[0], "/home/dsmyda/PycharmProjects/Permits1/"+file[1][:file[1].index(".")] + ".csv")
    #convert.to_csv("/home/dsmyda/PycharmProjects/CLI/New Mexicoperm.json", "/home/dsmyda/PycharmProjects/CLI/NewMexicoperm.csv")