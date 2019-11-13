import os
from p_consumer import *
from p_producer import *
from pipelinetypes import PIPELINE_SIGNAL, PIPELINE_END_STAGE_PIPELINE, PIPELINE_END_STAGE_INIT
import time
import uuid


class pipeline:
    is_pipeline_module = False

    def __init__(self, stage, bootstrap_servers):
        self.PIPELINE_STAGE_INIT = "init"
        self.PIPELINE_STAGE_PIPELINE = "pipeline"
        self.PIPELINE_STAGE_TERMINATE = "terminate"

        self.stage = stage

        self.pipeline_input_folder = os.environ.get("PIPELINE_INPUT_FOLDER")
        self.pipeline_output_folder = os.environ.get("PIPELINE_OUTPUT_FOLDER")

        self.pipeline_name = None
        self.pipeline_name = os.environ.get("PIPELINE_NAME")


        self.pipeline_input_folder = os.path.join(self.pipeline_input_folder, self.pipeline_name)
        self.pipeline_output_folder = os.path.join(self.pipeline_output_folder, self.pipeline_name)

        self.outputlog = os.path.join(self.pipeline_output_folder, os.environ.get("PIPELINE_OUTPUT_LOG"))

        self.sever_name = os.environ.get("VISIONFLOWSERVER")
        self.pipeline_contoller_url = os.path.join(self.sever_name, "pipelinecontroller/pipelinecontroller/")

        self.access_token = os.environ.get("ACCESS_TOKEN")

        self.pipeline_signal_topic = "signaltopic"
        self.pipeline_producer = p_producer(topic=self.pipeline_signal_topic, bootstrap_servers=bootstrap_servers)
        self.pipeline_consumer = p_consumer(topic=self.pipeline_signal_topic, bootstrap_servers=bootstrap_servers)

        self.bootstrap_servers = bootstrap_servers
        startflag = False

        con = self.pipeline_consumer.get_next()

        self.broadcast_msg = "None"

        # Videoinfo


        while not startflag:

            if self.stage == self.PIPELINE_STAGE_INIT:
                startflag = True
            else:
                key, msg = next(con)
                if key == PIPELINE_SIGNAL:
                    if msg == PIPELINE_END_STAGE_INIT:
                        if self.stage == self.PIPELINE_STAGE_PIPELINE:

                            startflag = True
                    elif msg == PIPELINE_END_STAGE_PIPELINE:
                        if self.stage == self.PIPELINE_STAGE_TERMINATE:

                            startflag = True



    def end_stage(self, message):
        print("Broadcasting %s" % (message))
        self.pipeline_producer.publish(msg=message, key=PIPELINE_SIGNAL)
        self.pipeline_producer.close()
        return 0


    def get_ouput_log(self):
        return self.outputlog
