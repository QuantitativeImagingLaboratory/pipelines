from pipelinesink.Writer.writer import writer
import pickle

class picklewriter(writer):
    def __init__(self, file):
        super().__init__(file)

        self.writerobj = open(self.outputfile, 'ab+')

    def write(self, data):
        pickle.dump(data, self.writerobj)

    def close(self):
        self.writerobj.close()