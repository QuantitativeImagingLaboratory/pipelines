from p_consumer import *
from pipelinetypes import KEY_SIGNAL,KEY_MESSAGE, SIGNAL_END, PIPELINE_STAGE_PIPELINE
import ast
import numpy as np
from pipeline import pipeline

class sink(pipeline):

    def __init__(self, input, mapping, lastprocessflag, topic, bootstrap_servers):
        super().__init__(PIPELINE_STAGE_PIPELINE, bootstrap_servers)

        self.consumer = p_consumer(topic=topic, bootstrap_servers=bootstrap_servers)
        self.mapping = mapping
        try:
            for val in mapping.values():
                if val not in self.input.keys():
                    raise Exception
        except Exception:
            print("Incompatible input mapping, required input", self.input.keys())

        self.lastprocessflag = lastprocessflag


    def map_input(self, inputmessage):
        message = inputmessage
        #write all
        for key in inputmessage.keys():
            if key in self.mapping.keys():
                continue
            message[key] = inputmessage[key]

        for key in self.mapping.keys():
            try:
                message[self.mapping[key]] = inputmessage[key]
                del message[key]
            except KeyError:
                print("Mapping error: %s does not exist" % (key))

        return message


    def get_input(self):
        return self.input

    def save_asset(self, message):
        pass

    def end_consuming(self):
        pass

    def messagetodict(self, inputmessage):
        message = inputmessage
        return ast.literal_eval(message)

    def arrayfromdict(self, image_str):

        x = np.fromstring(image_str["data"], dtype=image_str["dtype"])
        decoded = x.reshape(image_str["shape"])
        return decoded

    def subscribe(self):
        con = self.consumer.get_next()
        while True:
            key, message = next(con)
            if key == KEY_MESSAGE:
                yield self.map_input(self.messagetodict(message))
            elif key == KEY_SIGNAL:
                if message == SIGNAL_END:
                    self.end_consuming()
                    raise StopIteration

    def run(self):
        sub = self.subscribe()
        while True:
            try:
                message = next(sub)
                self.save_asset(message)
            except StopIteration:
                print("End sink")
                break

    @staticmethod
    def default_parser():
        from argparse import ArgumentParser

        parser = ArgumentParser()

        parser.add_argument("-m", "--mapping", dest="mapping", type=str,
                            help="specify the input mapping", metavar="MAPPING")
        parser.add_argument("-ct", "--topic", dest="topic",
                            help="specify the name of the topic", metavar="TOPIC")
        parser.add_argument("-b", "--bootstrap-server", dest="bootstrap_servers",
                            help="specify the name of the bootstrap_servers", metavar="BOOTSTRAP",
                            default='localhost:9092')
        parser.add_argument("-lp", "--last-process", dest="last_process",
                            help="specify the true if this is the last process", metavar="LASTPROCESS", default=False)

        return parser