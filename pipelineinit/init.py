from pipeline import pipeline
from pipelinetypes import PIPELINE_END_STAGE_INIT, PIPELINE_STAGE_INIT
import time
import os, shutil

class init(pipeline):
    def __init__(self, lastprocess, bootstrap_servers):
        super().__init__(PIPELINE_STAGE_INIT, bootstrap_servers)
        self.lastprocess = lastprocess
        self.clean_up_delete()

    def clean_up_delete(self):

        # Delete all file in folder
        files = os.listdir(self.pipeline_output_folder)

        for f in files:
            try:
                shutil.rmtree(os.path.join(self.pipeline_output_folder, f))
            except:
                os.remove(os.path.join(self.pipeline_output_folder, f))
            # os.remove(f)
            print("Removing: %s" % f)

    def end_init(self):
        print("End Init, sleeping 10 secs")
        time.sleep(10)
        if self.lastprocess:
            self.end_stage(PIPELINE_END_STAGE_INIT)
        else:
            return 0



    def init(self):
        pass

    @staticmethod
    def default_parser():
        from argparse import ArgumentParser

        parser = ArgumentParser()

        parser.add_argument("-lp", "--last-process", dest="last_process",
                            help="specify True if this is the last process", metavar="LASTPROCESS", default=False)
        parser.add_argument("-cb", "--consumer-bootstrap-server", dest="bootstrap_servers",
                            help="specify the name of the bootstrap_servers", metavar="BOOTSTRAP",
                            default='localhost:9092')

        return parser
