from pipelinesource.source import source
from pipelinetypes import p_image
import cv2

class videosource(source):
    output = {"image":p_image}

    def __init__(self, videofile, topic, bootstrap_servers='localhost:9092'):
        self.videofile = videofile
        self.video = cv2.VideoCapture(self.videofile)

        super().__init__(topic=topic, bootstrap_servers=bootstrap_servers)

        self.frame_rate = self.video.get(cv2.CAP_PROP_FPS)


    @staticmethod
    def get_parser():
        parser = source.default_parser()
        parser.add_argument("-v", "--video", dest="video",
                            help="specify the name of the video", metavar="VIDEO")

        return parser

    def read_asset(self):
        print('Sending %s.....' % (self.videofile))
        frameid = 0
        while (self.video.isOpened):

            success, image = self.video.read()

            if not success:
                self.video.release()
                print("Failed reading frame")
                raise StopIteration

            message = {"image": self.arraytodict(image), "frameid": frameid, "time_stamp": (frameid/self.frame_rate)}
            frameid += 1

            yield  str(message)



if __name__ == '__main__':

    parser = videosource.get_parser()
    args = parser.parse_args()


    vidsource = videosource(videofile=args.video, topic = args.topic, bootstrap_servers=args.bootstrap_servers)
    vidsource.run()
