from pipelinesource.source import source
from pipelinetypes import p_image
import cv2
import inspect, os, select
import subprocess as sp
import numpy as np
from datetime import datetime, timedelta

class kinesissource(source):
    # output = {"image":p_image}
    output = {"image": "image"}

    def __init__(self, streamfile, topic, bootstrap_servers='localhost:9092'):
        super().__init__(topic=topic, bootstrap_servers=bootstrap_servers)

        self.streamfile = os.path.join(self.pipeline_input_folder, streamfile)
        self.url = self.get_streamurl()

        self.frame_rate = self.get_frame_count()

        self.videowidth, self.videoheight = self.get_frame_size()
        print(self.url, self.frame_rate, self.videoheight, self.videowidth)


    def get_streamurl(self):
        file = open(self.streamfile, "r+")
        url = file.readline()
        file.close()
        return url

    def get_frame_size(self):
        FFPROBE = "ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=s=x:p=0 -i "
        framesizecommand = FFPROBE + self.url

        p = sp.Popen(framesizecommand, shell=True, stdout=sp.PIPE, stderr=sp.STDOUT)

        k = p.stdout.readlines()
        for i in range(len(k)):
            try:
                a,b = k[i].strip().decode('utf-8').split("x")
                try:
                    a,b = int(a), int(b)
                    print("width %s, height %s" % (a,b))
                    return a,b
                except:
                    pass
            except:
                pass

    def get_frame_count(self):
        FFPROBE = "ffprobe -v error -select_streams v:0 -show_entries stream=r_frame_rate -of csv=s=x:p=0 -i "
        framesizecommand = FFPROBE + self.url

        p = sp.Popen(framesizecommand, shell=True, stdout=sp.PIPE, stderr=sp.STDOUT)
        k = p.stdout.readlines()
        for i in range(len(k)):
            try:
                a, b = k[i].strip().decode('utf-8').split("/")

                print("a %s, b %s" % (a, b))
                try:
                    return float(a)/float(b)
                except:
                    pass

            except:
                pass

        # a, b = p.stdout.readlines()[0].strip().decode('utf-8').split("/")
        # print(a,b)
        # return float(a) / float(b)


    @staticmethod
    def get_parser():
        parser, default_args_list = source.default_parser()
        parser.add_argument("-s", "--streamfile", dest="stream",
                            help="specify the name of text file with stream", metavar="STREAM")
        additional_args_list = ["--streamfile"]
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

    def get_pts_time(self, ts):
        splitted = ts.decode('utf-8').split(" ")
        time = None
        for k in splitted:

            if k.startswith("pts_time"):
                time = float(k.split(":")[1])
                return timedelta(seconds=time)
        if not time:
            return timedelta(seconds=0)

    def read_asset(self):
        print('Receiving from %s.....' % (self.url))
        FFMPEG_BIN = "ffmpeg"  # on Linux ans Mac OS
        # pipe = sp.Popen([FFMPEG_BIN, "-i", self.url,
        #                  "-loglevel", "info",  # no text output
        #                  "-an",  # disable audio
        #                  "-f", "image2pipe",
        #                  "-pix_fmt", "bgr24",
        #                  "-vf", "showinfo",
        #                  "-vcodec", "rawvideo", "-"],
        #                 stdin=sp.PIPE, stdout=sp.PIPE, stderr = sp.PIPE, shell=False)
                        # stdin=sp.PIPE, stdout=sp.PIPE)

        pipe = sp.Popen([FFMPEG_BIN, "-i", self.url,
                         # '-r', '5',
                         "-loglevel", "info",  # no text output
                         "-an",  # disable audio
                         "-f", "image2pipe",
                         "-pix_fmt", "bgr24",
                         "-vf", "showinfo",
                         # "-vcodec", "rawvideo", "-"], bufsize=640*480*3 + 10000,
                         "-vcodec", "rawvideo", "-"],
                        stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE, shell=False)

        frameid = 0
        now = datetime.now()
        current_time_stamp = now
        while (pipe.returncode is None):
            # print(i)
            pipe.poll()
            dataend = False

            ready = select.select([pipe.stdout, pipe.stderr], [], [], 1.0)

            if pipe.stderr in ready[0]:

                data = pipe.stderr.readline()
                frame_timestamp = now + self.get_pts_time(data)
                # print(frame_timestamp)
                # print(data)
                # splitted = data.decode('utf-8').split(" ")
                # for k in splitted:
                #     if k.startswith("pts_time"):
                #         time = float(k.split(":")[1])
                #         timestamp = now + timedelta(seconds=time)
                #         print(timestamp)


            if pipe.stdout in ready[0]:
                data = pipe.stdout.read(640 * 480 * 3)

                if len(data) == 640 * 480 * 3:
                    image = np.fromstring(data, dtype='uint8').reshape((480, 640, 3))
                    # cv2.imshow("GoPro",cv2.resize(image, None, fx=0.25, fy=0.25))
                    # cv2.imshow("GoPro", image)
                    # # print("Next Frame")
                    # # framecount += 1
                    # # print(framecount)
                    # if cv2.waitKey(5) == 27:
                    #     break
            #     else:
            #         continue
            # else:
            #     continue

            #------------------------------original--------------------------------------------#
            # while True:
            # raw_image = pipe.stdout.read(640 * 480 * 3)
            # if raw_image:
            #     image = np.fromstring(raw_image, dtype='uint8').reshape((480, 640, 3))
            #     # ts = pipe.stderr.__next__()
            #     # frame_timestamp = now + self.get_pts_time(ts)
            #     frame_timestamp = now
            #     # image = cv2.resize(image, None, fx=0.2, fy=0.2)
            # # print(image.shape)
            # # success, image = self.video.read()
            #
            #     cv2.imshow("test_vid", image)
            #     cv2.waitKey(1)
            #
            #
            # # image = cv2.resize(image, (400,260))
            # else:
            #
            #     # print("Failed reading frame")
            #     print(".", end=" ")
            #     continue
            #     # raise StopIteration
            # ------------------------------original--------------------------------------------#
                    if current_time_stamp == frame_timestamp:
                        continue
                    else:
                        current_time_stamp = frame_timestamp
                    message = {"image": self.arraytodict(image), "frameid": frameid, "time_stamp": frame_timestamp.timestamp(),
                               "framewidth": self.videowidth, "frameheight": self.videoheight}
                    frameid += 1
                    if frameid and not frameid % 50:
                        print("Chunking")
                        self.do_chunk(str(frame_timestamp.timestamp()))

                    print("Frame %s, time %s" % (frameid, frame_timestamp))

                    yield  str(message)




if __name__ == '__main__':

    parser = kinesissource.get_parser()
    args = parser[0].parse_args()
    print(args)
    vidsource = kinesissource(streamfile=args.stream, topic = args.p_topic, bootstrap_servers=args.bootstrap_servers)
    vidsource.run()
