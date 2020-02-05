from pipelineprocess.process import process
from pipelinetypes import p_list_of_bb, p_int, KEY_MESSAGE
import ast
import cv2
import json
import inspect, os
import numpy as np
from pipelinesink.Writer.csvwriter import csvwriter

class filterbylocation(process):
    input = {"list_of_bb": "list_of_bb"}
    output = {"list_of_bb": "list_of_bb"}

    def __init__(self, bounding_box, c_topic, p_topic, mapping, saveoutputflag, lastprocessflag, c_bootstrap_servers='localhost:9092', p_bootstrap_servers='localhost:9092'):
        super().__init__(mapping=mapping, saveoutputflag=saveoutputflag, lastprocessflag=lastprocessflag, c_topic=c_topic, p_topic=p_topic, c_bootstrap_servers=c_bootstrap_servers, p_bootstrap_servers=p_bootstrap_servers)
        self.bb = bounding_box
        # self.bb['tl'][0] = self.bb['tl'][0]*self.videowidth
        # self.bb['tl'][1] = self.bb['tl'][1] * self.videoheight
        # self.bb['br'][0] = self.bb['br'][0] * self.videowidth
        # self.bb['br'][1] = self.bb['br'][1] * self.videoheight

    @staticmethod
    def get_parser():

        parser, default_args_list = process.default_parser()

        parser.add_argument("-ab", "--bounding-box", dest="bounding_box",
                            help="specify the bounding rectangle tlx,tly,brx,bry", metavar="BOUNDINGBOX")

        additional_args_list = []
        input_args_list = ["--bounding-box"]
        return parser, default_args_list, additional_args_list, input_args_list



    @staticmethod
    def get_command():
        pyt = "python"

        def add_arg(argument, default_val):
            return " " + argument + " " + default_val

        intial_command = pyt + " " + inspect.getfile(__class__)
        print(intial_command)
        parser, _, _, _ = __class__.get_parser()
        for k in parser._actions[1:]:
            intial_command += add_arg(k.option_strings[1], str(k.default))

        return intial_command

    @staticmethod
    def get_command_info():

        info_dict_default = {}
        info_dict_additional = {}
        info_dict_required = {}
        parser, def_args, add_args, req_args = __class__.get_parser()
        help = {}
        for k in parser._actions[1:]:
            if k.option_strings[1] in def_args:
                info_dict_default[k.option_strings[1]] = k.default
            elif k.option_strings[1] in add_args:
                info_dict_additional[k.option_strings[1]] = k.default
            elif k.option_strings[1] in req_args:
                info_dict_required[k.option_strings[1]] = k.default
            help[k.option_strings[1]] = k.help

        return {"file": inspect.getfile(__class__).replace(os.getcwd() + "/", ""), "default_args": info_dict_default,
                "additional_args": info_dict_additional,
                "required_args": info_dict_required, "help": help}

    def process(self, inputmessage):
        message_dict = inputmessage

        image_str = message_dict["list_of_bb"]

        frame_width = message_dict["framewidth"]
        frame_height = message_dict["frameheight"]

        def center(inputbb):
            return ((inputbb['tl'][0]+inputbb['br'][0])/2, (inputbb['tl'][1]+inputbb['br'][1])/2)

        def isinside(inputbb):
            center_of_bb = center(inputbb)

            if center_of_bb[0] >= self.bb["tl"][0]*frame_width and center_of_bb[0] <= self.bb["br"][0]*frame_width:
                if center_of_bb[1] >= self.bb["tl"][1]*frame_height and center_of_bb[1] <= self.bb["br"][1]*frame_width:

                    return True

            return False

        processed_list= []
        for i in range(len(image_str)):
            if isinside(image_str[i]['bb']):
                processed_list += [image_str[i]]


        message_dict['list_of_bb'] = processed_list
        message = message_dict
        # image_str = message_dict["image"]
        # x = np.fromstring(image_str["data"], dtype=image_str["dtype"])
        # decoded = x.reshape(image_str["shape"])
        # print((self.bb["tl"][0]*frame_width,self.bb["tl"][1]*frame_height), (self.bb['br'][0]*frame_width, self.bb["br"][1]*frame_height))
        # cv2.rectangle(decoded, (int(self.bb["tl"][0]*frame_width),int(self.bb["tl"][1]*frame_height)), (int(self.bb['br'][0]*frame_width), int(self.bb["br"][1]*frame_height)), (0, 255, 0), 3)
        # cv2.imshow("iamge", decoded)
        # cv2.waitKey(0)
        # exit()
        if self.saveoutputflag:
            self.saveoutput({"list_of_bb": processed_list, "frameid": message_dict["frameid"],
                             "time_stamp": message_dict["time_stamp"], "framewidth": message_dict["framewidth"], "frameheight": message_dict["frameheight"]})
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
                                "data": [{"type": "list_of_bb", "location": self.outputfile}]}

        self.outputfile_relative = self.outputfilebase_relative + ".py"
        self.dict_output_log_relative = {"stage": self.stagename,
                                         "data": [{"type": "list_of_bb", "location": self.outputfile_relative}]}

    def saveoutput(self, data):
        self.outwriter.write(data)


if __name__ == '__main__':

    parser = filterbylocation.get_parser()
    args = parser[0].parse_args()

    def converttojsonreadable(inputstring):
        inputstring = inputstring.replace(":", "\":\"")
        inputstring = inputstring.replace("{", "{\"").replace("}", "\"}")
        inputstring = inputstring.replace(",", "\",\"")
        return inputstring

    bbvalues = [float(k) for k in args.bounding_box.split(",")]
    bb = {"tl":(bbvalues[0], bbvalues[1]), "br":(bbvalues[2],bbvalues[3])}

    args.mapping = json.loads(converttojsonreadable(args.mapping))

    filter = filterbylocation(bounding_box= bb, c_topic=args.c_topic, p_topic=args.p_topic, mapping=args.mapping, saveoutputflag=args.save_output, lastprocessflag=args.last_process, c_bootstrap_servers=args.c_bootstrap_servers, p_bootstrap_servers=args.p_bootstrap_servers)
    filter.run()