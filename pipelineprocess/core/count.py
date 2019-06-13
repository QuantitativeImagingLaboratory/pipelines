from pipelineprocess.process import process
from pipelinetypes import p_list, p_int, KEY_MESSAGE
import ast
import cv2
import json
import numpy as np
from pipelinesink.Writer.csvwriter import csvwriter

class count(process):
    def __init__(self, c_topic, p_topic, mapping, saveoutputflag, lastprocessflag, c_bootstrap_servers='localhost:9092', p_bootstrap_servers='localhost:9092'):
        super().__init__(input={"list": p_list}, output = {"count":p_int}, mapping=mapping, saveoutputflag=saveoutputflag, lastprocessflag=lastprocessflag, c_topic=c_topic, p_topic=p_topic, c_bootstrap_servers=c_bootstrap_servers, p_bootstrap_servers=p_bootstrap_servers)


    def process(self, inputmessage):
        message_dict = inputmessage

        image_str = message_dict["list"]


        message_dict["count"] = len(image_str)

        message = message_dict


        if self.saveoutputflag:
            self.saveoutput({"count": len(image_str), "frameid": message_dict["frameid"], "time_stamp":message_dict["time_stamp"]})

        print({"count": len(image_str), "frameid": message_dict["frameid"]})
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

    from argparse import ArgumentParser

    parser = ArgumentParser()

    parser.add_argument("-pt", "--producer-topic", dest="p_topic",
                        help="specify the name of the producer topic", metavar="PTOPIC")
    parser.add_argument("-ct", "--consumer-topic", dest="c_topic",
                        help="specify the name of the consumer topic", metavar="CTOPIC")
    parser.add_argument("-m", "--mapping", dest="mapping", type=str,
                        help="specify the input mapping", metavar="MAPPING")
    parser.add_argument("-pb", "--producer-bootstrap-server", dest="p_bootstrap_servers",
                        help="specify the name of the bootstrap_servers for producer", metavar="PBOOTSTRAP", default='localhost:9092')
    parser.add_argument("-cb", "--consumer-bootstrap-server", dest="c_bootstrap_servers",
                        help="specify the name of the bootstrap_servers", metavar="BOOTSTRAP", default='localhost:9092')
    parser.add_argument("-so", "--save-output", dest="save_output",
                        help="specify the true to save output for this process", metavar="SAVEOUTPUT", default=True)
    parser.add_argument("-lp", "--last-process", dest="last_process",
                        help="specify the true if this is the last process", metavar="LASTPROCESS", default=False)
    args = parser.parse_args()


    def converttojsonreadable(inputstring):
        inputstring = inputstring.replace(":", "\":\"")
        inputstring = inputstring.replace("{", "{\"").replace("}", "\"}")
        inputstring = inputstring.replace(",", "\",\"")
        return inputstring


    args.mapping = json.loads(converttojsonreadable(args.mapping))

    add = count(c_topic=args.c_topic, p_topic=args.p_topic, mapping=args.mapping, saveoutputflag=args.save_output, lastprocessflag=args.last_process, c_bootstrap_servers=args.c_bootstrap_servers, p_bootstrap_servers=args.p_bootstrap_servers)
    add.run()