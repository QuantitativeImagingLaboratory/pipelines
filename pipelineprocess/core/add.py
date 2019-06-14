from pipelineprocess.process import process
from pipelinetypes import p_array, p_float, KEY_MESSAGE
import ast
import cv2
import json
import numpy as np
from pipelinesink.Writer.csvwriter import csvwriter

class add(process):
    def __init__(self, c_topic, p_topic, mapping, saveoutputflag, lastprocessflag, c_bootstrap_servers='localhost:9092', p_bootstrap_servers='localhost:9092'):
        super().__init__(input={"array": p_array}, output = {"sum":p_float}, mapping=mapping, saveoutputflag=saveoutputflag, lastprocessflag=lastprocessflag, c_topic=c_topic, p_topic=p_topic, c_bootstrap_servers=c_bootstrap_servers, p_bootstrap_servers=p_bootstrap_servers)

    @staticmethod
    def get_parser():
        parser = process.default_parser()

        return parser

    def process(self, inputmessage):
        message_dict = inputmessage

        image_str = message_dict["array"]

        x = np.fromstring(image_str["data"], dtype=image_str["dtype"])
        decoded = x.reshape(image_str["shape"])

        message_dict["sum"] = np.sum(decoded)

        message = message_dict


        if self.saveoutputflag:
            self.saveoutput({"sum": np.sum(decoded), "frameid": message_dict["frameid"], "time_stamp":message_dict["time_stamp"]})

        return str(message)

    def end_consuming(self):
        print("end consuming")
        self.outwriter.close()

    def initializeoutputwriter(self):
        print("Initializing")
        self.outputfile = self.outputfilebase + ".csv"
        self.outwriter = csvwriter(self.outputfile)
        self.dict_output_log = {"stage": self.stagename,
                                "data": [{"type": "list_of_integers", "location": self.outputfile}]}

    def saveoutput(self, data):
        self.outwriter.write(data)


if __name__ == '__main__':

    parser = add.get_parser()
    args = parser.parse_args()


    def converttojsonreadable(inputstring):
        inputstring = inputstring.replace(":", "\":\"")
        inputstring = inputstring.replace("{", "{\"").replace("}", "\"}")
        inputstring = inputstring.replace(",", "\",\"")
        return inputstring


    args.mapping = json.loads(converttojsonreadable(args.mapping))

    add = add(c_topic=args.c_topic, p_topic=args.p_topic, mapping=args.mapping, saveoutputflag=args.save_output, lastprocessflag=args.last_process, c_bootstrap_servers=args.c_bootstrap_servers, p_bootstrap_servers=args.p_bootstrap_servers)
    add.run()