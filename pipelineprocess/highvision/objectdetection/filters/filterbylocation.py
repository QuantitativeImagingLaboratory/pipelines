from pipelineprocess.process import process
from pipelinetypes import p_list_of_bb, p_int, KEY_MESSAGE
import ast
import cv2
import json
import numpy as np
from pipelinesink.Writer.csvwriter import csvwriter

class filterbylocation(process):
    def __init__(self, bounding_box, c_topic, p_topic, mapping, saveoutputflag, lastprocessflag, c_bootstrap_servers='localhost:9092', p_bootstrap_servers='localhost:9092'):
        super().__init__(input={"list_of_bb": p_list_of_bb}, output = {"list_of_bb":p_list_of_bb}, mapping=mapping, saveoutputflag=saveoutputflag, lastprocessflag=lastprocessflag, c_topic=c_topic, p_topic=p_topic, c_bootstrap_servers=c_bootstrap_servers, p_bootstrap_servers=p_bootstrap_servers)
        self.bb = bb

    def process(self, inputmessage):
        message_dict = inputmessage

        image_str = message_dict["list_of_bb"]

        def center(inputbb):
            return ((inputbb['tl'][0]+inputbb['br'][0])/2, (inputbb['tl'][1]+inputbb['br'][1])/2)

        def isinside(inputbb):
            center_of_bb = center(inputbb)

            if center_of_bb[0] >= self.bb["tl"][0] and center_of_bb[0] <= self.bb["br"][0]:
                if center_of_bb[1] >= self.bb["tl"][1] and center_of_bb[1] <= self.bb["br"][1]:
                    return True

            return False

        processed_list= []
        for i in range(len(image_str)):
            if isinside(image_str[i]['bb']):
                processed_list += [image_str[i]]

        message_dict['list_of_bb'] = processed_list
        message = message_dict

        if self.saveoutputflag:
            self.saveoutput({"list_of_bb": processed_list, "frameid": message_dict["frameid"],
                             "time_stamp": message_dict["time_stamp"]})
        print({"list_of_bb": processed_list, "frameid": message_dict["frameid"]})
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
    parser.add_argument("-ab", "--bounding-box", dest="bounding_box",
                        help="specify the bounding rectangle tlx,tly,brx,bry", metavar="BOUNDINGBOX")
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

    bbvalues = [int(k) for k in args.bounding_box.split(",")]
    bb = {"tl":(bbvalues[0], bbvalues[1]), "br":(bbvalues[2],bbvalues[3])}

    args.mapping = json.loads(converttojsonreadable(args.mapping))

    filter = filterbylocation(bounding_box= bb, c_topic=args.c_topic, p_topic=args.p_topic, mapping=args.mapping, saveoutputflag=args.save_output, lastprocessflag=args.last_process, c_bootstrap_servers=args.c_bootstrap_servers, p_bootstrap_servers=args.p_bootstrap_servers)
    filter.run()