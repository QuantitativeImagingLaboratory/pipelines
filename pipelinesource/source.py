from p_producer import *
from pipeline import pipeline
from pipelinetypes import KEY_SIGNAL, KEY_MESSAGE, SIGNAL_END, PIPELINE_STAGE_PIPELINE
import time

class source(pipeline):

    def __init__(self, output, topic, bootstrap_servers):
        self.output = output
        self.producer = p_producer(topic=topic, bootstrap_servers=bootstrap_servers)

        super().__init__(PIPELINE_STAGE_PIPELINE, bootstrap_servers)

    def get_output(self):
        return self.output

    def read_asset(self):
        """Specific function to read asset
        end with end transition function call """
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
                self.end_publishing()
                break
            except RuntimeError:
                self.end_publishing()
                break

    def end_publishing(self):
        self.publish(message=SIGNAL_END, key=KEY_SIGNAL)
        print("Sleeping 10 sec, ending producer")
        time.sleep(10)
        self.producer.close()
        return 0



