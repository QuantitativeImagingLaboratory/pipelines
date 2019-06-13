from p_consumer import *
from p_producer import *
from pipelinetypes import KEY_SIGNAL, SIGNAL_END, PIPELINE_END_STAGE_PIPELINE, PIPELINE_STAGE_PIPELINE
from pipeline import pipeline
import time
import ast
import os, json

class process(pipeline):
    def __init__(self, input, output, mapping, saveoutputflag, lastprocessflag, c_topic, p_topic, p_bootstrap_servers, c_bootstrap_servers):
        self.stagename = p_topic

        super().__init__(PIPELINE_STAGE_PIPELINE, p_bootstrap_servers)

        self.input = input
        self.output = output

        self.producer = p_producer(topic=p_topic, bootstrap_servers=p_bootstrap_servers)
        self.consumer = p_consumer(topic=c_topic, bootstrap_servers=c_bootstrap_servers)

        self.mapping = mapping
        self.saveoutputflag = saveoutputflag
        self.lastprocessflag = lastprocessflag

        if self.lastprocessflag:
            self.saveoutputflag = True


        if saveoutputflag:
            self.outputfilebase = os.path.join(self.pipeline_output_folder,self.__class__.__name__+str(time.time()).replace(".", ""))
            self.outwriter = None

            self.outputfile = None
            self.dict_output_log = None

            self.initializeoutputwriter()

        try:
            for val in mapping.values():
                if val not in self.input.keys():
                    raise Exception
        except Exception:
            print("Incompatible input mapping, required input", self.input.keys())




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
                print("Mapping error: %s does not exist" % (key))

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

                self.end_publishing()
                break
            except RuntimeError:
                self.end_publishing()
                break

    def write_to_output_log(self, dict_output):
        flag_file = os.path.isfile(self.outputlog)

        data = None
        if not flag_file:
            open(self.outputlog, 'w').close()
        else:
            with open(self.outputlog) as f:
                try:
                    data = [json.load(f)]
                except:
                    data = None

        if data:
            data += [dict_output]
        else:
            data = dict_output

        json.dump(data, open(self.outputlog, 'w'))

    def end_publishing(self):
        time.sleep(10)
        self.publish(message=SIGNAL_END, key=KEY_SIGNAL)
        print("Sleeping 10 sec, ending producer")
        time.sleep(10)
        self.producer.close()


        if self.saveoutputflag:
            self.write_to_output_log(self.dict_output_log)
        print("Last Flag", self.lastprocessflag)
        if self.lastprocessflag:
            self.end_stage(PIPELINE_END_STAGE_PIPELINE)
        else:
            return 0

