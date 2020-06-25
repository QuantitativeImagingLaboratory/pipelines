from p_producer import *
from pipeline import pipeline
from pipelinetypes import KEY_SIGNAL, KEY_MESSAGE, SIGNAL_END, PIPELINE_STAGE_PIPELINE, SIGNAL_CHUNK
import time, os

class source(pipeline):

    def __init__(self, topic, bootstrap_servers):
        self.producer = p_producer(topic=topic, bootstrap_servers=bootstrap_servers)

        super().__init__(PIPELINE_STAGE_PIPELINE, bootstrap_servers)

    def read_asset(self):
        """Specific function to read asset
        end with end transition function call """
        pass



    def do_chunk(self, chunk_name):
        self.publish(SIGNAL_CHUNK, KEY_SIGNAL)
        self.publish(chunk_name, KEY_SIGNAL)
        pass

    def publish(self, message, key=KEY_MESSAGE):
        self.producer.publish(message, key=key)


    def arraytodict(self, inputarray):
        return {"data":inputarray.tobytes(), "shape": inputarray.shape, "dtype":str(inputarray.dtype)}

    def run(self):
        asset = self.read_asset()

        while True:
            try:
                message = next(asset)
                self.publish(message)
            except StopIteration:
                print("Encountered StopIteration Exception")
                self.end_publishing()
                break
            except RuntimeError:
                print("Encountered RuntimeError Exception")
                self.end_publishing()
                break

    def end_publishing(self):
        time.sleep(10)
        self.publish(message=SIGNAL_END, key=KEY_SIGNAL)
        self.pipelineprint("Sleeping 10 sec, ending producer")
        time.sleep(10)
        self.producer.close()
        return 0

    @staticmethod
    def default_parser():
        default_args_list = ["--producer-topic", "--bootstrap-server"]
        from argparse import ArgumentParser

        parser = ArgumentParser()

        parser.add_argument("-pt", "--producer-topic", dest="p_topic",
                            help="specify the name of the topic", metavar="TOPIC")
        parser.add_argument("-pb", "--producer-bootstrap-server", dest="bootstrap_servers",
                            help="specify the name of the bootstrap_servers", metavar="BOOTSTRAP",
                            default='localhost:9092')
        return parser, default_args_list
