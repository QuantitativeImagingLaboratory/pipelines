from pipelinesink.sink import sink
from pipelinetypes import *
import cv2
import ast
import os
import json
from pipelinesink.Writer.videowriter import videowriter

class videosink(sink):
    def __init__(self, videofile, mapping, topic, last_process, bootstrap_servers='localhost:9092'):
        self.videofile = videofile

        super().__init__(input={"image":p_array}, mapping=mapping, lastprocessflag=last_process, topic=topic, bootstrap_servers=bootstrap_servers)

        self.outwriter = videowriter(self.videofile)


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

    from argparse import ArgumentParser

    parser = ArgumentParser()

    parser.add_argument("-v", "--video", dest="video",
                        help="specify the name of the video", metavar="VIDEO")
    parser.add_argument("-m", "--mapping", dest="mapping", type=str,
                        help="specify the input mapping", metavar="MAPPING")
    parser.add_argument("-ct", "--topic", dest="topic",
                        help="specify the name of the topic", metavar="TOPIC")
    parser.add_argument("-b", "--bootstrap-server", dest="bootstrap_servers",
                        help="specify the name of the bootstrap_servers", metavar="BOOTSTRAP", default='localhost:9092')
    parser.add_argument("-lp", "--last-process", dest="last_process",
                        help="specify the true if this is the last process", metavar="LASTPROCESS", default=False)
    args = parser.parse_args()


    def converttojsonreadable(inputstring):
        inputstring = inputstring.replace(":", "\":\"")
        inputstring = inputstring.replace("{", "{\"").replace("}", "\"}")
        inputstring = inputstring.replace(",", "\",\"")
        return inputstring


    args.mapping = json.loads(converttojsonreadable(args.mapping))
    vidsink = videosink(videofile=args.video, mapping=args.mapping, last_process=args.last_process, topic = args.topic, bootstrap_servers=args.bootstrap_servers)
    vidsink.run()

