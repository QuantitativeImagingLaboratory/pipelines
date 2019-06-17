from pipelinesink.sink import sink
from pipelinetypes import *
import cv2
import ast
import os
import json
import inspect
from pipelinesink.Writer.videowriter import videowriter

class videosink(sink):
    input =  {"image":"image"}

    def __init__(self, videofile, mapping, topic, last_process, bootstrap_servers='localhost:9092'):
        self.videofile = videofile

        super().__init__(mapping=mapping, lastprocessflag=last_process, topic=topic, bootstrap_servers=bootstrap_servers)

        self.outwriter = videowriter(self.videofile)

    @staticmethod
    def get_parser():
        parser = sink.default_parser()

        parser.add_argument("-v", "--video", dest="video",
                            help="specify the name of the video", metavar="VIDEO")

        return parser

    @staticmethod
    def get_command():
        pyt = "python"

        def add_arg(argument, default_val):
            return " " + argument + " " + default_val

        intial_command = pyt + " " + inspect.getfile(__class__)
        print(intial_command)
        for k in __class__.get_parser()._actions[1:]:
            intial_command += add_arg(k.option_strings[1], str(k.default))

        return intial_command

    @staticmethod
    def get_command_info():
        info_dict = {}

        info_dict["file"] = inspect.getfile(__class__)

        for k in __class__.get_parser()._actions[1:]:
            info_dict[k.option_strings[1]] = k.default

        return info_dict

    def save_asset(self, inputmessage, covert = True):

        decoded = self.arrayfromdict(inputmessage["image"])
        print("Writing img: %s" % (self.videofile))
        self.outwriter.write(decoded)


    def end_consuming(self):
        self.outwriter.close()
        if self.lastprocessflag:
            self.end_stage(PIPELINE_END_STAGE_PIPELINE)
        else:
            return 0


if __name__ == '__main__':

    parser = videosink.get_parser()
    args = parser.parse_args()


    def converttojsonreadable(inputstring):
        inputstring = inputstring.replace(":", "\":\"")
        inputstring = inputstring.replace("{", "{\"").replace("}", "\"}")
        inputstring = inputstring.replace(",", "\",\"")
        return inputstring


    args.mapping = json.loads(converttojsonreadable(args.mapping))
    vidsink = videosink(videofile=args.video, mapping=args.mapping, last_process=args.last_process, topic = args.topic, bootstrap_servers=args.bootstrap_servers)
    vidsink.run()

