from pipelineprocess.process import process
from pipelinetypes import p_int, p_number, KEY_MESSAGE
import ast
import cv2
import json
import numpy as np
from pipelinesink.Writer.csvwriter import csvwriter

class movingaverage(process):
    def __init__(self, percentchange, count, c_topic, p_topic, mapping, saveoutputflag, lastprocessflag, c_bootstrap_servers='localhost:9092', p_bootstrap_servers='localhost:9092'):
        super().__init__(input={"number": p_number}, output = {"alert":p_int}, mapping=mapping, saveoutputflag=saveoutputflag, lastprocessflag=lastprocessflag, c_topic=c_topic, p_topic=p_topic, c_bootstrap_servers=c_bootstrap_servers, p_bootstrap_servers=p_bootstrap_servers)
        self.values = []
        self.count = int(count)
        self.i = 0
        self.percent = percentchange

    @staticmethod
    def get_parser():
        parser = process.default_parser()

        parser.add_argument("-ap", "--percent", dest="percent",
                            help="specify the change in percent to alert", metavar="PERCENT", default=10)
        parser.add_argument("-ac", "--count", dest="count",
                            help="specify the number of values to compute the average", metavar="COUNT", default=15)

        return parser

    def process(self, inputmessage):
        message_dict = inputmessage

        val = float(message_dict["number"])

        if self.i < self.count:
            alert = 0
            self.values += [val]
        else:
            self.values.pop(0)
            self.values += [val]
            meanofvalues = np.mean(self.values)
            if ((val - meanofvalues)/meanofvalues) * 100 > 0.1:
                alert = 1
            else:
                alert = 0

        message_dict["alert"] = alert

        message = message_dict

        if self.saveoutputflag:
            self.saveoutput({"alert": alert, "frameid": message_dict["frameid"], "time_stamp":message_dict["time_stamp"]})

        self.i += 1

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

    parser = movingaverage.get_parser()
    args = parser.parse_args()


    def converttojsonreadable(inputstring):
        inputstring = inputstring.replace(":", "\":\"")
        inputstring = inputstring.replace("{", "{\"").replace("}", "\"}")
        inputstring = inputstring.replace(",", "\",\"")
        return inputstring


    args.mapping = json.loads(converttojsonreadable(args.mapping))

    th = movingaverage(percentchange = args.percent, count = args.count, c_topic=args.c_topic, p_topic=args.p_topic, mapping=args.mapping, saveoutputflag=args.save_output, lastprocessflag=args.last_process, c_bootstrap_servers=args.c_bootstrap_servers, p_bootstrap_servers=args.p_bootstrap_servers)
    th.run()