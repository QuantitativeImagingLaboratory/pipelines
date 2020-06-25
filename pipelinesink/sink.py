from p_consumer import *
from pipelinetypes import KEY_SIGNAL,KEY_MESSAGE, SIGNAL_END, PIPELINE_STAGE_PIPELINE, SIGNAL_CHUNK, PIPELINE_END_STAGE_PIPELINE
import ast, os, shutil
import numpy as np
from pipeline import pipeline
import time

class sink(pipeline):

    def __init__(self, mapping, lastprocessflag, topic, bootstrap_servers):
        super().__init__(PIPELINE_STAGE_PIPELINE, bootstrap_servers)

        self.consumer = p_consumer(topic=topic, bootstrap_servers=bootstrap_servers)
        self.mapping = mapping
        try:
            for val in mapping.values():
                if val not in self.input.keys():
                    raise Exception
        except Exception:
            self.pipelineprint("Incompatible input mapping, required input " + self.input.keys())

        self.lastprocessflag = lastprocessflag

        self.chunk_folder = None
        self.current_time_stamp = None


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
                self.pipelineprint("Mapping error: %s does not exist" % (key))

        return message

    def folder_delete(self, folder):

        # Delete all file in folder
        files = os.listdir(folder)

        for f in files:
            try:
                shutil.rmtree(os.path.join(folder, f))
            except:
                os.remove(os.path.join(folder, f))
            # os.remove(f)
            self.pipelineprint("Removing: %s" % f)

        self.pipelineprint("Removing Folder: %s" % folder)
        os.rmdir(folder)


    def get_input(self):
        return self.input

    def save_asset(self, message):
        pass

    def end_consuming(self):
        # self.publish(message=SIGNAL_END, key=KEY_SIGNAL)
        # print("Sleeping 10 sec, ending producer")
        # time.sleep(10)
        # self.producer.close()
        #
        #
        # if self.saveoutputflag:
        #     self.write_to_output_log(self.dict_output_log)
        # print("Sink Last Flag", self.lastprocessflag)
        # if self.lastprocessflag:
        #     self.end_stage(PIPELINE_END_STAGE_PIPELINE)
        # else:
        #     return 0

        pass

    def messagetodict(self, inputmessage):
        message = inputmessage
        return ast.literal_eval(message)

    def arrayfromdict(self, image_str):

        x = np.fromstring(image_str["data"], dtype=image_str["dtype"])
        decoded = x.reshape(image_str["shape"])
        return decoded

    def do_chunk(self):
        pass

    def subscribe(self):
        con = self.consumer.get_next()
        while True:
            key, message = next(con)
            self.pipelineprint(
                "----------------------------------------------sink Key 0" + key)
            if key == KEY_MESSAGE:
                self.pipelineprint(
                    "----------------------------------------------sink Key " + key)
                yield self.map_input(self.messagetodict(message))
            elif key == KEY_SIGNAL:
                self.pipelineprint("----------------------------------------------sink Key "+ key + "-------------" + message)
                if message == SIGNAL_END:
                    # self.do_chunk(message)
                    # self.end_publishing()
                    self.end_consuming()
                    raise StopIteration
                elif message == SIGNAL_CHUNK:
                    self.pipelineprint("received chunk message")

                    key, message = next(con)
                    self.pipelineprint(key +", "+ message)


                    if key != KEY_SIGNAL:
                        raise Exception("Chunk Messange with chunkname not received")
                    self.pipelineprint("received chunk message name: " + message)
                    self.do_chunk(message)

    def run(self):
        sub = self.subscribe()
        while True:
            try:
                message = next(sub)
                self.save_asset(message)
            except StopIteration:
                self.pipelineprint("End sink")
                break

    # def run(self):
    #
    #     sub = self.subscribe()
    #
    #     while True:
    #         try:
    #             message = next(sub)
    #
    #             self.save_asset(message)
    #             # processed_message = self.process(message)
    #             #self.publish(processed_message)
    #
    #         except StopIteration:
    #             print("StopIteration")
    #             self.end_publishing()
    #             break
    #         except RuntimeError:
    #             print("RuntimeError")
    #             self.end_publishing()
    #             break


    def end_publishing(self):
        self.pipelineprint()
        time.sleep(10)
        self.publish(message=SIGNAL_END, key=KEY_SIGNAL)
        self.pipelineprint("Sleeping 10 sec, ending producer")
        time.sleep(10)
        self.producer.close()


        if self.saveoutputflag:
            self.write_to_output_log(self.dict_output_log)
        self.pipelineprint("Sink Last Flag "+ self.lastprocessflag)
        if self.lastprocessflag:
            self.end_stage(PIPELINE_END_STAGE_PIPELINE)
        else:
            return 0

    @staticmethod
    def default_parser():
        default_args_list = ["--mapping", "--consumer-topic", "--consumer-bootstrap-server", "--last-process"]

        from argparse import ArgumentParser

        parser = ArgumentParser()

        parser.add_argument("-m", "--mapping", dest="mapping", type=str,
                            help="specify the input mapping", metavar="MAPPING")
        parser.add_argument("-ct", "--consumer-topic", dest="topic",
                            help="specify the name of the topic", metavar="TOPIC")
        parser.add_argument("-cb", "--consumer-bootstrap-server", dest="bootstrap_servers",
                            help="specify the name of the bootstrap_servers", metavar="BOOTSTRAP",
                            default='localhost:9092')
        parser.add_argument("-lp", "--last-process", dest="last_process",
                            help="specify the true if this is the last process", metavar="LASTPROCESS", default=False)

        return parser, default_args_list

    