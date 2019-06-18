from pipelineprocess.process import process
from pipelinetypes import p_int, p_number, KEY_MESSAGE
import ast
import cv2
import json
import inspect
import numpy as np
from pipelinesink.Writer.csvwriter import csvwriter

class thresholding(process):
    input = {"number": p_number}
    output = {"alert": p_int}

    def __init__(self, threshold, c_topic, p_topic, mapping, saveoutputflag, lastprocessflag, c_bootstrap_servers='localhost:9092', p_bootstrap_servers='localhost:9092'):
        super().__init__(mapping=mapping, saveoutputflag=saveoutputflag, lastprocessflag=lastprocessflag, c_topic=c_topic, p_topic=p_topic, c_bootstrap_servers=c_bootstrap_servers, p_bootstrap_servers=p_bootstrap_servers)
        self.threshold = float(threshold)

    @staticmethod
    def get_parser():
        parser = process.default_parser()

        parser.add_argument("-at", "--threshold", dest="threshold",
                            help="specify the threshold value", metavar="THRESHOLD")
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

    def process(self, inputmessage):
        message_dict = inputmessage

        image_str = message_dict["number"]


        if float(image_str) > self.threshold:
            alert = 1
        else:
            alert = 0

        message_dict["alert"] = alert

        message = message_dict

        if self.saveoutputflag:
            self.saveoutput({"alert": alert, "frameid": message_dict["frameid"], "time_stamp":message_dict["time_stamp"]})

        return str(message)

    def end_consuming(self):
        print("end consuming")
        self.outwriter.close()

    def initializeoutputwriter(self):
        print("Initializing")
        self.outputfile = self.outputfilebase + ".csv"
        self.outwriter = csvwriter(self.outputfile)
        self.dict_output_log = {"stage": self.stagename,
                                "data": [{"type": "list_of_binary", "location": self.outputfile}]}

    def saveoutput(self, data):
        self.outwriter.write(data)


if __name__ == '__main__':

    parser = thresholding.get_parser()
    args = parser.parse_args()


    def converttojsonreadable(inputstring):
        inputstring = inputstring.replace(":", "\":\"")
        inputstring = inputstring.replace("{", "{\"").replace("}", "\"}")
        inputstring = inputstring.replace(",", "\",\"")
        return inputstring


    args.mapping = json.loads(converttojsonreadable(args.mapping))

    th = thresholding(threshold = args.threshold, c_topic=args.c_topic, p_topic=args.p_topic, mapping=args.mapping, saveoutputflag=args.save_output, lastprocessflag=args.last_process, c_bootstrap_servers=args.c_bootstrap_servers, p_bootstrap_servers=args.p_bootstrap_servers)

    th.run()
