from pipelinesink.sink import sink
from pipelinetypes import p_message, PIPELINE_END_STAGE_PIPELINE
import csv
import numpy as np
import json
import inspect
from pipelinesink.Writer.csvwriter import csvwriter

class csvsink(sink):
    input = {"value": "message"}

    def __init__(self, csvfile, mapping, topic, last_process, bootstrap_servers='localhost:9092'):
        super().__init__(mapping=mapping, lastprocessflag = last_process, topic=topic, bootstrap_servers=bootstrap_servers)

        self.file = open(csvfile, 'w')
        self.writer = csvwriter(csvfile)


    @staticmethod
    def get_parser():
        parser, default_args_list = sink.default_parser()
        parser.add_argument("-f", "--file", dest="filename",
                            help="specify the name of the csv file", metavar="FILE")
        additional_args_list = ["--file"]
        return parser, default_args_list, additional_args_list

    @staticmethod
    def get_command():
        pyt = "python"

        def add_arg(argument, default_val):
            return " " + argument + " " + default_val

        intial_command = pyt + " " + inspect.getfile(__class__)
        print(intial_command)
        parser, _, _ = __class__.get_parser()
        for k in parser._actions[1:]:
            intial_command += add_arg(k.option_strings[1], str(k.default))

        return intial_command

    @staticmethod
    def get_command_info():
        info_dict = {}

        info_dict["file"] = inspect.getfile(__class__)
        info_dict_default = {}
        info_dict_additional = {}
        parser, def_args, add_args = __class__.get_parser()
        help = {}
        for k in parser._actions[1:]:
            if k.option_strings[1] in def_args:
                info_dict_default[k.option_strings[1]] = k.default
            elif k.option_strings[1] in add_args:
                info_dict_additional[k.option_strings[1]] = k.default
            help[k.option_strings[1]] = k.help

        return {"default_args": info_dict_default, "additional_args": info_dict_additional, "help": help}
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

