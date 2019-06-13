from pipelinesink.Writer.writer import writer
import cv2
import os

class videowriter(writer):
    def __init__(self, videofile, convert=True):
        self.fourcc = cv2.VideoWriter_fourcc(*'MP4V')
        self.videofile = videofile
        super().__init__(self.videofile)
        self.convert = convert
        self.temp_file = "tempfiletodelete.mp4"


    def write(self, frame):

        if self.i == 0:
            shape = frame.shape
            self.writerobj = cv2.VideoWriter(self.temp_file, self.fourcc, 20.0, (shape[1], shape[0]))

        self.i += 1
        self.writerobj.write(frame)

    def conversion(self):
        os.system("ffmpeg -y -i %s -c:v libx264 -preset slow -crf 1 -c:a copy %s" % (self.temp_file, self.videofile))



    def close(self):
        self.writerobj.release()
        if self.convert:
            self.conversion()
            os.remove(self.temp_file)
        else:
            os.rename(self.temp_file, self.videofile)





