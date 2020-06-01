from pipeline import pipeline
from pipelinetypes import PIPELINE_END_STAGE_INIT, PIPELINE_STAGE_INIT
import time
import os, shutil
import os


class init(pipeline):
    def __init__(self, lastprocess, bootstrap_servers):
        super().__init__(PIPELINE_STAGE_INIT, bootstrap_servers)
        self.lastprocess = lastprocess
        self.create_input_output_dir()
        self.clean_up_delete()


    def create_input_output_dir(self):

        if not os.path.exists(self.pipeline_input_folder):
            os.makedirs(self.pipeline_input_folder)

        if not os.path.exists(self.pipeline_output_folder):
            os.makedirs(self.pipeline_output_folder)



    def clean_up_delete(self):

        # Delete all file in folder
        files = os.listdir(self.pipeline_output_folder)

        for f in files:
            if ".nfs" in f:
                print("skipping ", f)
                continue
            try:
                shutil.rmtree(os.path.join(self.pipeline_output_folder, f))
            except:
                if not os.path.isdir(os.path.join(self.pipeline_output_folder, f)):
                    os.remove(os.path.join(self.pipeline_output_folder, f))
            # os.remove(f)
            print("Removing: %s" % f)

    def end_init(self):
        print("End Init, sleeping 10 secs")
        time.sleep(10)
        if self.lastprocess:
            self.end_stage(PIPELINE_END_STAGE_INIT)
        else:
            pass

        while True:
            time.sleep(1000000)



    def init(self):
        pass

    @staticmethod
    def default_parser():
        default_args_list = ["--producer-bootstrap-server", "--last-process"]
        from argparse import ArgumentParser

        parser = ArgumentParser()

        parser.add_argument("-lp", "--last-process", dest="last_process",
                            help="specify True if this is the last process", metavar="LASTPROCESS", default=False)
        parser.add_argument("-pb", "--producer-bootstrap-server", dest="bootstrap_servers",
                            help="specify the name of the bootstrap_servers", metavar="BOOTSTRAP",
                            default='localhost:9092')

        return parser, default_args_list


