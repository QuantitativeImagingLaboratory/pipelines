from p_consumer import *
from p_producer import *
from pipelinetypes import KEY_SIGNAL, SIGNAL_END, PIPELINE_END_STAGE_PIPELINE, PIPELINE_STAGE_PIPELINE, SIGNAL_CHUNK
from pipeline import pipeline
import time
import ast
import os, json

class process(pipeline):

    def __init__(self, mapping, saveoutputflag, lastprocessflag, c_topic, p_topic, p_bootstrap_servers, c_bootstrap_servers):
        self.stagename = p_topic

        super().__init__(PIPELINE_STAGE_PIPELINE, p_bootstrap_servers)

        self.producer = p_producer(topic=p_topic, bootstrap_servers=p_bootstrap_servers)
        self.consumer = p_consumer(topic=c_topic, bootstrap_servers=c_bootstrap_servers)

        self.mapping = mapping
        self.saveoutputflag = saveoutputflag
        self.lastprocessflag = lastprocessflag

        if self.lastprocessflag:
            self.saveoutputflag = True

        self.name = self.__class__.__name__
        if saveoutputflag:
            self.outputfilebase = os.path.join(self.pipeline_output_folder,self.name+str(time.time()).replace(".", ""))
            # self.outputfilebase = self.pipeline_output_folder
            self.outputfilebase_relative = self.name

            self.outwriter = None

            self.outputfile = None
            self.dict_output_log = None

            self.initializeoutputwriter()

        try:
            for val in mapping.values():
                if val not in self.input.keys():
                    raise Exception
        except Exception:
            self.pipelineprint("Incompatible input mapping, required input " + self.input.keys())



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
                if key != self.mapping[key]:
                    del message[key]
            except KeyError:
                self.pipelineprint("Mapping error: %s does not exist" % (key))

        return message

    def get_input(self):
        return self.input

    def get_output(self):
        return self.output

    def publish(self, message, key=KEY_MESSAGE):
        self.producer.publish(message, key=key)

    def arraytodict(self, inputarray):
        return {"data": inputarray.tobytes(), "shape": inputarray.shape, "dtype": str(inputarray.dtype)}

    def messagetodict(self, inputmessage):
        message = inputmessage
        return ast.literal_eval(message)

    def create_directory(self, dir):
        if not os.path.exists(dir):
            os.makedirs(dir)

    def do_chunk(self, chunk_name):

        self.outputfilebase = os.path.join(self.pipeline_output_folder, str(chunk_name))
        self.this_chunk_output_log = os.path.join(self.outputfilebase, self.output_log_file)
        self.create_directory(self.outputfilebase)
        self.outputfilebase = os.path.join(self.outputfilebase,  self.__class__.__name__)

        # if LockFile(self.outputfile).is_locked():
        #     LockFile(self.outputfile).release()
        self.outwriter.close()
        self.initializeoutputwriter()

        self.write_to_output_log(self.dict_output_log_relative, self.this_chunk_output_log)
        self.publish(SIGNAL_CHUNK, KEY_SIGNAL)
        self.publish(chunk_name, KEY_SIGNAL)
        pass

    def subscribe(self):
        con = self.consumer.get_next()
        while True:
            key, message = next(con)
            self.pipelineprint("------------------------"+key)
            if key == KEY_MESSAGE:
                yield self.map_input(self.messagetodict(message))
            elif key == KEY_SIGNAL:

                self.pipelineprint("Process Key "+ key + "-------------" + message)
                if message == SIGNAL_END:
                    time.sleep(10)
                    self.end_consuming()
                    raise StopIteration
                elif message == SIGNAL_CHUNK:
                    self.pipelineprint("received chunk message")

                    key, message = next(con)
                    self.pipelineprint(key+" "+ KEY_SIGNAL)


                    if key != KEY_SIGNAL:
                        raise Exception("Chunk Messange with chunkname not received")
                    self.pipelineprint("received chunk message name: " + message)
                    self.do_chunk(message)

    def process(self, inputmessage):
        pass

    def end_consuming(self):
        pass

    def initializeoutputwriter(self):
        pass

    def saveoutput(self):
        pass

    def run(self):

        sub = self.subscribe()

        while True:
            try:
                message = next(sub)

                processed_message = self.process(message)
                self.publish(processed_message)

            except StopIteration:
                self.pipelineprint("StopIteration")
                self.end_publishing()
                break
            except RuntimeError:
                self.pipelineprint("RuntimeError")
                self.end_publishing()
                break

    def write_to_output_log(self, dict_output, file = None):
        if file:
            output_file = file
        else:
            output_file = self.outputlog

        flag_file = os.path.isfile(output_file)

        data = []
        if not flag_file:
            open(output_file, 'w').close()
        else:
            with open(output_file) as f:
                try:
                    data = json.load(f)
                except:
                    data = None

        if data:
            data.extend([dict_output])
        else:
            data = [dict_output]

        json.dump(data, open(output_file, 'w'))

    def end_publishing(self):
        time.sleep(10)
        self.pipelineprint("publishing %s, %s" % (KEY_SIGNAL, SIGNAL_END))
        self.publish(message=SIGNAL_END, key=KEY_SIGNAL)
        self.pipelineprint("Sleeping 10 sec, ending producer")
        time.sleep(10)
        self.producer.close()

        if self.saveoutputflag:
            self.write_to_output_log(self.dict_output_log)
        self.pipelineprint("Last Flag "+ str(self.lastprocessflag))
        if self.lastprocessflag:
            self.end_stage(PIPELINE_END_STAGE_PIPELINE)
        else:
            return 0



    @staticmethod
    def default_parser():
        default_args_list = ["--producer-topic", "--consumer-topic", "--mapping", "--producer-bootstrap-server",
                             "--consumer-bootstrap-server", "--save-output", "--last-process"]
        from argparse import ArgumentParser

        parser = ArgumentParser()

        parser.add_argument("-pt", "--producer-topic", dest="p_topic",
                            help="specify the name of the producer topic", metavar="PTOPIC")
        parser.add_argument("-ct", "--consumer-topic", dest="c_topic",
                            help="specify the name of the consumer topic", metavar="CTOPIC")
        parser.add_argument("-m", "--mapping", dest="mapping", type=str,
                            help="specify the input mapping {output:input}", metavar="MAPPING")
        parser.add_argument("-pb", "--producer-bootstrap-server", dest="p_bootstrap_servers",
                            help="specify the name of the bootstrap_servers for producer", metavar="PBOOTSTRAP",
                            default='localhost:9092')
        parser.add_argument("-cb", "--consumer-bootstrap-server", dest="c_bootstrap_servers",
                            help="specify the name of the bootstrap_servers", metavar="BOOTSTRAP",
                            default='localhost:9092')
        parser.add_argument("-so", "--save-output", dest="save_output",
                            help="specify the true to save output for this process", metavar="SAVEOUTPUT", default=True)
        parser.add_argument("-lp", "--last-process", dest="last_process",
                            help="specify the true if this is the last process", metavar="LASTPROCESS", default=False)

        return parser, default_args_list