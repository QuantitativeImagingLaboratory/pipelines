from pipeline import pipeline
from pipelinetypes import PIPELINE_END_STAGE_INIT, PIPELINE_STAGE_INIT
import time

class init(pipeline):
    def __init__(self, lastprocess, bootstrap_servers):
        super().__init__(PIPELINE_STAGE_INIT, bootstrap_servers)
        self.lastprocess = lastprocess


    def end_init(self):
        print("End Init, sleeping 10 secs")
        time.sleep(10)
        if self.lastprocess:
            self.end_stage(PIPELINE_END_STAGE_INIT)
        else:
            return 0



    def init(self):
        pass
