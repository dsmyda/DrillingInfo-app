import json
from csv import DictWriter
import threading
import os

class Convert(threading.Thread):

    """ A Convert class to handle file type conversions.

    This class will be used to handle the tasks inputted in the convert tab of the application. """

    def __init__(self, format, src, dest):
        threading.Thread.__init__(self)
        self.format = format
        self.src = src
        self.src_ext = self.get_src_ext()
        self.dest = dest

    def run(self):
        if self.format == "CSV":
            self.to_csv()
        elif self.format == "DTA":
            self.to_dta()
        elif self.format == "JSON":
            self.to_json()

    def to_csv(self):
        f = os.path.basename(self.src)
        f = f[:f.rindex(".")] + ".csv"

        if self.src_ext == "CSV":
            return
        elif self.src_ext == "JSON":
            with open(self.src, 'rb') as input, open(self.dest + os.sep + f, 'w') as output:
                json_array = json.load(input)
                writer = DictWriter(output, json_array[0].keys())
                writer.writeheader()
                for json_object in json_array:
                    writer.writerow(json_object)
        elif self.src_ext == "DTA":
            raise("DTA -> CSV is not supported yet.")

    def to_json(self):
        if self.src_ext == "JSON":
            return
        elif self.src_ext == "CSV":
            pass
        elif self.src_ext == "DTA":
            pass

    def to_dta(self):
        f = os.path.basename(self.src)
        f = f[:f.rindex(".")] + ".dta"

        if self.src_ext == "DTA":
            return
        elif self.src_ext == "CSV":
            try:
                import rpy2.robjects as robjects
                robjects.r("require(foreign)")
                robjects.r('x=read.csv("%s")' % self.src)
                robjects.r('write.dta(x, "%s")' % self.dest + os.sep + f)
            except ImportError:
                print("Need R installed for this feature")
        elif self.src_ext == "JSON":
            pass

    def concat_csv(self, src, src1):
        with open("/tmp", 'w') as new:
            with open(src, 'r') as fsrc:
                for row in fsrc:
                    new.write(row)
            with open(src1, 'r') as fdst:
                next(fdst) #Remove second header
                for row in fdst:
                    new.write(row)
        #os.unlink(src1)
        os.rename("/tmp", src)

    def get_src_ext(self):
        if self.src.endswith(".csv"):
            return "CSV"
        elif self.src.endswith(".json"):
            return "JSON"
        elif self.src.endswith(".dta"):
            return "DTA"
        else:
            raise("File format not supported: %s" % self.src)

    def __repr__(self):
        return "Convert " + os.path.basename(self.src)

def concat_csv(src, dest):
    with open("tmp", 'w') as new:
        with open(src, 'r') as fsrc:
            for row in fsrc:
                new.write(row)
        with open(dest, 'r') as fdst:
            next(fdst) #Remove second header
            for row in fdst:
                new.write(row)
    os.rename("tmp", src)

if __name__ == "__main__":
    concat_csv("/home/dsmyda/PycharmProjects/DrillingInfo-app/TXprodh.csv", "/home/dsmyda/PycharmProjects/DrillingInfo-app/tmp_prodh_3_29_17/tmppp2/TXprodh.csv")