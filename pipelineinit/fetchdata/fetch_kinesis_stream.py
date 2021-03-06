from resources.resource_s3 import resource_kinesis
from pipelineinit.init import init
import inspect, os

class fetch_kinesis_stream(init):
    is_pipeline_module = True

    def __init__(self, lastprocess, bootstrap_servers):
        self.kinesis = resource_kinesis()
        super().__init__(lastprocess, bootstrap_servers)


    # @staticmethod
    # def get_parser():
    #     parser = init.default_parser()
    #     parser.add_argument("-b", "--bucket", dest="bucket",
    #                         help="specify the name of the bucket", metavar="BUCKET", default='vaaas-media')
    #     parser.add_argument("-d", "--dir", dest="dir",
    #                         help="specify the name of the directory", metavar="DIRECTORY")
    #     parser.add_argument("-f", "--file", dest="file",
    #                         help="specify the name of the file", metavar="FILE")
    #     parser.add_argument("-o", "--output", dest="output",
    #                         help="specify the name of the output file", metavar="OUTPUT")
    #
    #     return parser

    @staticmethod
    def get_parser():
        parser, default_args_list = init.default_parser()
        parser.add_argument("-s", "--stream", dest="stream",
                            help="specify the name of the stream name", metavar="STREAM")
        parser.add_argument("-o", "--output", dest="output",
                            help="specify the name of the output file", metavar="OUTPUT")

        additional_args_list = ["--stream", "--output"]
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

    def init(self, stream, output):
        output = os.path.join(self.pipeline_input_folder,output)

        try:
            url = self.kinesis.get_hls_url(stream)
            outF = open(output, "w")
            outF.writelines(url)
            outF.close()
        except:
            print("Failed to get url for stream: ", stream)
        self.end_init()


if __name__ == "__main__":
    parser = fetch_kinesis_stream.get_parser()
    args = parser[0].parse_args()

    kin = fetch_kinesis_stream(lastprocess = args.last_process, bootstrap_servers=args.bootstrap_servers)
    kin.init(args.stream, args.output)

