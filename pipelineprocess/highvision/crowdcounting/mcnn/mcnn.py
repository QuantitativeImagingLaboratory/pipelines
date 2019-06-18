from pipelineprocess.process import process
from pipelinetypes import p_array, p_image, KEY_MESSAGE
import ast
import cv2
import json
import os, inspect
import numpy as np

from pipelineprocess.highvision.crowdcounting.mcnn.src.crowd_count import CrowdCounter
from pipelineprocess.highvision.crowdcounting.mcnn.src import network

from pipelinesink.Writer.picklewriter import picklewriter

class mcnn(process):
    input = {"image": "image"}
    output = {"density": "array"}

    def __init__(self, c_topic, p_topic, mapping, saveoutputflag, lastprocessflag, c_bootstrap_servers='localhost:9092', p_bootstrap_servers='localhost:9092'):
        super().__init__(mapping=mapping, saveoutputflag=saveoutputflag, lastprocessflag=lastprocessflag, c_topic=c_topic, p_topic=p_topic, c_bootstrap_servers=c_bootstrap_servers, p_bootstrap_servers=p_bootstrap_servers)

        model_path = 'pipelineprocess/highvision/crowdcounting/mcnn/mcnn_shtechA_660.h5'

        self.net = CrowdCounter()

        trained_model = os.path.join(model_path)
        network.load_net(trained_model, self.net)

        self.net.eval()
        self.net.cpu()

    @staticmethod
    def get_parser():
        parser, default_args_list = process.default_parser()

        additional_args_list = []
        input_args_list = []

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
        info_dict = {}

        info_dict["file"] = inspect.getfile(__class__)
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

        return {"default_args": info_dict_default, "additional_args": info_dict_additional,
                "required_args": info_dict_required, "help": help}


    def preprocess_image(self, img):
        cv2_im = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        img = cv2_im.astype(np.float32, copy=False)
        ht = img.shape[0]
        wd = img.shape[1]
        ht_1 = (ht * 4) // 4
        wd_1 = (wd * 4) // 4
        img = cv2.resize(img, (wd_1, ht_1))
        img = img.reshape((1, 1, img.shape[0], img.shape[1]))

        return img

    def process(self, inputmessage):
        message_dict = inputmessage

        image_str = message_dict["image"]
        x = np.fromstring(image_str["data"], dtype=image_str["dtype"])
        decoded = x.reshape(image_str["shape"])

        img = self.preprocess_image(decoded)

        density = self.net(img)

        density_map = density.data.cpu().numpy()

        density_map = density_map.reshape((density_map.shape[2], density_map.shape[3]))


        message_dict["density"] = self.arraytodict(density_map)

        message = message_dict

        if self.saveoutputflag:
            self.saveoutput({"density": density_map, "frameid": message_dict["frameid"], "time_stamp":message_dict["time_stamp"]})


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
        self.dict_output_log = {"stage": self.stagename, "data":[{"type": "list_of_2D_array", "location": self.outputfile}]}


    def saveoutput(self, data):
        self.outwriter.write(data)


if __name__ == '__main__':
    parser = mcnn.get_parser()
    args = parser.parse_args()

    def converttojsonreadable(inputstring):
        inputstring = inputstring.replace(":","\":\"")
        inputstring = inputstring.replace("{", "{\"").replace("}", "\"}")
        inputstring = inputstring.replace(",", "\",\"")
        return inputstring

    args.mapping = json.loads(converttojsonreadable(args.mapping))
    p_mcnn = mcnn(c_topic=args.c_topic, p_topic=args.p_topic, mapping=args.mapping, saveoutputflag=args.save_output, lastprocessflag=args.last_process, c_bootstrap_servers=args.c_bootstrap_servers, p_bootstrap_servers=args.p_bootstrap_servers)
    p_mcnn.run()