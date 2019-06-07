from pipelinesink.Writer.writer import writer
import csv

class csvwriter(writer):
    def __init__(self, csvfile):
        super().__init__(csvfile)

        self.outputfile = open(csvfile, 'w')


    def write(self, inputmessage):
        print(inputmessage)
        if self.i == 0:
            self.writerobj = csv.DictWriter(self.outputfile, fieldnames=inputmessage.keys())
            self.writerobj.writeheader()

        self.writerobj.writerow(inputmessage)

        self.i += 1

    def close(self):
        self.outputfile.close()



