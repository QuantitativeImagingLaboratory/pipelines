from pipelineprocess.process import process
from pipelinetypes import p_int, p_number, KEY_MESSAGE
import ast
import cv2
import json
import numpy as np
from pipelinesink.Writer.picklewriter import picklewriter

class filterbyclass(process):
    input = {"list_of_bb": p_number}
    output = {"list_of_bb": p_int}

    def __init__(self, classes, c_topic, p_topic, mapping, saveoutputflag, lastprocessflag, c_bootstrap_servers='localhost:9092', p_bootstrap_servers='localhost:9092'):
        super().__init__(mapping=mapping, saveoutputflag=saveoutputflag, lastprocessflag=lastprocessflag, c_topic=c_topic, p_topic=p_topic, c_bootstrap_servers=c_bootstrap_servers, p_bootstrap_servers=p_bootstrap_servers)
        self.classes = classes

    @staticmethod
    def get_parser():
        parser = process.default_parser()

        parser.add_argument("-ac", "--classes", dest="classes",
                            help="specify the list of classes", metavar="CLASSES")
        return parser

    def process(self, inputmessage):

        message_dict = inputmessage

        image_str = message_dict["list_of_bb"]

        processed_list = []
        for i in range(len(image_str)):
            if image_str[i]['class'] in self.classes:
                processed_list += [image_str[i]]

        message_dict['list_of_bb'] = processed_list
        message = message_dict

        if self.saveoutputflag:
            self.saveoutput({"list_of_bb": processed_list, "frameid": message_dict["frameid"], "time_stamp":message_dict["time_stamp"]})
        print({"list_of_bb": processed_list, "frameid": message_dict["frameid"]})
        return str(message)

    def end_consuming(self):
        print("end consuming")
        if self.saveoutputflag:
            print("Closing")
            self.outwriter.close()

    def initializeoutputwriter(self):
        print("Initializing")
        self.outputfile = self.outputfilebase + ".py"
        self.outwriter = picklewriter(self.outputfile)
        self.dict_output_log = {"stage": self.stagename, "data": [{"type": "list_of_bb", "location": self.outputfile}]}

    def saveoutput(self, data):
        self.outwriter.write(data)


if __name__ == '__main__':

    parser = filterbyclass.get_parser()
    args = parser.parse_args()


    def converttojsonreadable(inputstring):
        inputstring = inputstring.replace(":", "\":\"")
        inputstring = inputstring.replace("{", "{\"").replace("}", "\"}")
        inputstring = inputstring.replace(",", "\",\"")
        return inputstring


    args.mapping = json.loads(converttojsonreadable(args.mapping))

    classes = args.classes.split(",")


    th = filterbyclass(classes = classes, c_topic=args.c_topic, p_topic=args.p_topic, mapping=args.mapping, saveoutputflag=args.save_output, lastprocessflag=args.last_process, c_bootstrap_servers=args.c_bootstrap_servers, p_bootstrap_servers=args.p_bootstrap_servers)
    th.run()