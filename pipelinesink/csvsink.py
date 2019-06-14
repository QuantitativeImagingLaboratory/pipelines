from pipelinesink.sink import sink
from pipelinetypes import p_message, PIPELINE_END_STAGE_PIPELINE
import csv
import numpy as np
import json
from pipelinesink.Writer.csvwriter import csvwriter

class csvsink(sink):
    def __init__(self, csvfile, mapping, topic, last_process, bootstrap_servers='localhost:9092'):
        super().__init__(input={"value": p_message}, mapping=mapping, lastprocessflag = last_process, topic=topic, bootstrap_servers=bootstrap_servers)

        self.file = open(csvfile, 'w')
        self.writer = csvwriter(csvfile)

    @staticmethod
    def get_parser():
        parser = sink.default_parser()

        parser.add_argument("-f", "--file", dest="filename",
                            help="specify the name of the csv file", metavar="FILE")

        return parser

    def save_asset(self, inputmessage):
        self.writer.write(inputmessage)

    def end_consuming(self):
        self.writer.close()
        if self.lastprocessflag:
            self.end_stage(PIPELINE_END_STAGE_PIPELINE)
        else:
            return 0


if __name__ == '__main__':

    parser = csvsink.get_parser()
    args = parser.parse_args()


    def converttojsonreadable(inputstring):
        inputstring = inputstring.replace(":", "\":\"")
        inputstring = inputstring.replace("{", "{\"").replace("}", "\"}")
        inputstring = inputstring.replace(",", "\",\"")
        return inputstring


    args.mapping = json.loads(converttojsonreadable(args.mapping))
    csink = csvsink(csvfile=args.filename, mapping=args.mapping, last_process = args.last_prcoess, topic = args.topic, bootstrap_servers=args.bootstrap_servers)
    csink.run()

