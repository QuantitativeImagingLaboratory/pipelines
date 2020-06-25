from resources.resource_s3 import resource_s3
from pipelineterminate.terminate import terminate
import os, inspect
from datetime import datetime
import time

class put_s3(terminate):
    def __init__(self, lastprocess, bootstrap_servers, s3dir, inputvideofile, bucket_name = 'vaaas-media'):
        self.s3 = resource_s3(bucket_name)
        super().__init__(lastprocess, bootstrap_servers)

        self.videofile = inputvideofile
        self.s3dir = s3dir

        now = datetime.now().strftime("_%Y%m%d%H%M%S")

        s3_folder_video_name = os.path.join(self.s3dir, self.videofile.split(".")[0])
        self.output_folder_s3 = os.path.join(s3_folder_video_name, self.pipeline_name + now)


    @staticmethod
    def get_parser():
        parser, default_args_list = terminate.default_parser()
        parser.add_argument("-b", "--bucket", dest="bucket",
                            help="specify the name of the bucket", metavar="BUCKET", default='vaaas-media')
        parser.add_argument("-sd", "--s3-dir", dest="s3dir",
                            help="specify the name of the directory on s3", metavar="DIRECTORY")
        parser.add_argument("-f", "--file", dest="file",
                            help="specify the name of the input video file", metavar="FILE")
        additional_args_list = ["--bucket", "--s3-dir", "--file"]
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

        return {"file": inspect.getfile(__class__).replace(os.getcwd() + "/", ""), "default_args": info_dict_default,
                "additional_args": info_dict_additional,
                "required_args": info_dict_required, "help": help}

    def terminate(self):
        # print(self.pipeline_output_folder)
        # for root, dirs, files in os.walk(self.pipeline_output_folder):
        #     for file in files:
        #         print(root, file)
        #         self.s3.upload(os.path.join(root, file), self.output_folder_s3, file)
        print("called terminate")
        time.sleep(10)
        self.end_terminate()


if __name__ == "__main__":
    parser = put_s3.get_parser()
    args = parser[0].parse_args()

    s3 = put_s3(s3dir=args.s3dir, inputvideofile=args.file, bucket_name=args.bucket, lastprocess=args.last_process, bootstrap_servers = args.bootstrap_servers)
    s3.terminate()