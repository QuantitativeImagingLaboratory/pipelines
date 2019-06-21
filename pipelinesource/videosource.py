from pipelinesource.source import source
from pipelinetypes import p_image
import cv2
import inspect, os

class videosource(source):
    # output = {"image":p_image}
    output = {"image": "image"}

    def __init__(self, videofile, topic, bootstrap_servers='localhost:9092'):
        self.videofile = videofile
        self.video = cv2.VideoCapture(self.videofile)

        super().__init__(topic=topic, bootstrap_servers=bootstrap_servers)

        self.frame_rate = self.video.get(cv2.CAP_PROP_FPS)


    @staticmethod
    def get_parser():
        parser, default_args_list = source.default_parser()
        parser.add_argument("-v", "--video", dest="video",
                            help="specify the name of the video", metavar="VIDEO")
        additional_args_list = ["--video"]
        input_args_list = []
        return parser, default_args_list, additional_args_list, input_args_list

    @staticmethod
    def get_command():
        pyt = "python"

        def add_arg(argument, default_val):
            return " " + argument + " " + default_val

        intial_command = pyt + " " + inspect.getfile(__class__)
        print(intial_command)
        parser, _, _, _ = __class__.get_parser()
        for k in parser._actions[1:]:
            intial_command += add_arg(k.option_strings[1], str(k.default))

        return intial_command

    @staticmethod
    def get_command_info():

        info_dict_default = {}
        info_dict_additional = {}
        info_dict_required = {}
        parser, def_args, add_args, req_args = __class__.get_parser()
        help = {}
        for k in parser._actions[1:]:
            if k.option_strings[1] in def_args:
                info_dict_default[k.option_strings[1]] = k.default
            elif k.option_strings[1] in add_args:
                info_dict_additional[k.option_strings[1]] = k.default
            elif k.option_strings[1] in req_args:
                info_dict_required[k.option_strings[1]] = k.default
            help[k.option_strings[1]] = k.help

        return {"file": inspect.getfile(__class__).replace(os.getcwd() + "/", ""), "default_args": info_dict_default,
                "additional_args": info_dict_additional,
                "required_args": info_dict_required, "help": help}

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
    args = parser[0].parse_args()

    vidsource = videosource(videofile=args.video, topic = args.p_topic, bootstrap_servers=args.bootstrap_servers)
    vidsource.run()
