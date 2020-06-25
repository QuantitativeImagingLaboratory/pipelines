from pipelinesink.sink import sink
from pipelinetypes import *
import cv2
import ast
import os
import json
import inspect, os
from pipelinesink.Writer.videowriter import videowriter
from resources.resource_s3 import resource_s3
from datetime import datetime
import requests



class s3sink(sink):
    input =  {"any": "None"}

    def __init__(self, mapping, topic, last_process, s3dir, bootstrap_servers='localhost:9092', bucket_name = 'vaaas-media'):


        super().__init__(mapping=mapping, lastprocessflag=last_process, topic=topic, bootstrap_servers=bootstrap_servers)

        # self.outwriter = videowriter(self.videofile)

        self.s3 = resource_s3(bucket_name)


        self.s3dir = s3dir

        # now = datetime.now().strftime("_%Y%m%d%H%M%S")

        # s3_folder_video_name = os.path.join(self.s3dir, self.videofile.split(".")[0])
        # self.output_folder_s3 = os.path.join(s3_folder_video_name, self.pipeline_name + now)
        self.output_folder_s3 = os.path.join(self.s3dir, self.pipeline_name)


    @staticmethod
    def get_parser():

        parser, default_args_list = sink.default_parser()
        parser.add_argument("-b", "--bucket", dest="bucket",
                            help="specify the name of the bucket", metavar="BUCKET", default='vaaas-media')
        parser.add_argument("-sd", "--s3-dir", dest="s3dir",
                            help="specify the name of the directory on s3", metavar="DIRECTORY")
        # parser.add_argument("-f", "--file", dest="file",
        #                     help="specify the name of the input video file", metavar="FILE")
        # additional_args_list = ["--bucket", "--s3-dir", "--file"]
        additional_args_list = ["--bucket", "--s3-dir"]
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

        return {"file":inspect.getfile(__class__).replace(os.getcwd()+"/", ""), "default_args": info_dict_default, "additional_args": info_dict_additional,
                "required_args": info_dict_required, "help": help}

    def do_chunk(self, chunk_name):
        # upload chunk folder to s3
        self.chunk_folder = os.path.join(self.pipeline_output_folder, str(chunk_name))
        chunk_output_s3_folder = os.path.join(self.output_folder_s3, chunk_name)
        self.pipelineprint(self.chunk_folder)


        for root, dirs, files in os.walk(self.chunk_folder):
            for file in files:
                this_file = os.path.join(root, file)
                b = 0
                while not b:
                    b = os.path.getsize(this_file)

                    # print("size", b)
                # with open(this_file, 'a') as low:
                #     print(low.read())
                #     low.close()
                # try:
                #     with open(file, "r") as filer:
                #         print("%s is closed", file)
                self.s3.upload(this_file, chunk_output_s3_folder, file)


        self.pipelineprint("Deleting Folder: " + self.chunk_folder)
        # if chunk_name != SIGNAL_END:
        self.folder_delete(self.chunk_folder)
                # except:
                #     print("%s is closed", file)
                #     pass

        #Add chunk nameto api server.

        self.post_chunk_to_vision_flow_server(chunk_name, chunk_output_s3_folder)
        pass

    def post_chunk_to_vision_flow_server(self, chunk_name, chunk_output_s3_folder):

        response = requests.get(self.pipeline_contoller_url,
                                headers={'Content-Type': 'application/json',
                                         'Authorization': 'Token {}'.format(self.access_token)})

        response_dict = response.json()

        pipeline_id = None
        for k in response_dict:

            if k["name"] == self.pipeline_name:
                pipeline_id = k["id"]

        if pipeline_id == None:
            print("Pipeline id corresponding to %s, not found" % (self.pipeline_name))

        pipeline_url = self.pipeline_contoller_output_url
        # print("-----------------------------------", chunk_name, self.current_time_stamp)
        # print(float(chunk_name) - float(self.current_time_stamp))
        payload_completed = {
            'pipeline_id': pipeline_id,
            'start_time': self.current_time_stamp,
            'end_time': chunk_name,
            'output': chunk_output_s3_folder
        }
        response = requests.post(pipeline_url, json.dumps(payload_completed),
                                headers={'Content-Type': 'application/json',
                                         'Authorization': 'Token {}'.format(self.access_token)})
        # print("==================================================")
        # print(response)
        # print("==================================================")
        self.current_time_stamp = None
        return response

    def save_asset(self, inputmessage, covert = True):
        if not self.current_time_stamp:
            self.current_time_stamp = inputmessage["time_stamp"]
        #upload chunk folder to s3

        # print(self.chunk_folder)
        #post save results to api

        # decoded = self.arrayfromdict(inputmessage["image"])
        # print("Writing img: %s" % (self.videofile))
        # self.outwriter.write(decoded)
        pass

    def end_consuming(self):
        print(self.__class__.__name__+"------------------------"+self.lastprocessflag)
        if self.lastprocessflag:
            self.end_stage(PIPELINE_END_STAGE_PIPELINE)
        else:
            return 0


if __name__ == '__main__':

    parser = s3sink.get_parser()
    args = parser[0].parse_args()

    def converttojsonreadable(inputstring):
        inputstring = inputstring.replace(":", "\":\"")
        inputstring = inputstring.replace("{", "{\"").replace("}", "\"}")
        inputstring = inputstring.replace(",", "\",\"")
        return inputstring


    args.mapping = json.loads(converttojsonreadable(args.mapping))
    vidsink = s3sink(mapping=args.mapping, last_process=args.last_process, topic = args.topic, bootstrap_servers=args.bootstrap_servers,
                     s3dir=args.s3dir)
    vidsink.run()

