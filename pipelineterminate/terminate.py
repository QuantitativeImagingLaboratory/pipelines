from pipeline import pipeline
import os, shutil
import requests
import json
from pipelinetypes import PIPELINE_STAGE_TERMINATE, PIPELINE_END_STAGE_TERMINATE

class terminate(pipeline):
    def __init__(self, lastprocess, bootstrap_servers):
        super().__init__(PIPELINE_STAGE_TERMINATE, bootstrap_servers)

        self.lastprocess = lastprocess


    def end_terminate(self):
        if self.lastprocess:
            self.post_completion_to_vision_flow_server()
            self.clean_up_delete()
            self.end_stage(PIPELINE_END_STAGE_TERMINATE)
        else:
            return 0

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

    def post_completion_to_vision_flow_server(self):

        flag_file = os.path.isfile(self.outputlog)
        data = []
        if flag_file:
            with open(self.outputlog) as f:
                data = json.load(f)

        print(data, self.output_folder_s3)
        for output in data:
            for d in output["data"]:
                d["location"] = d["location"].replace(self.pipeline_output_folder, self.output_folder_s3)
        print(data)

        response = requests.get(self.pipeline_contoller_url,
                                headers={'Content-Type': 'application/json',
                                         'Authorization': 'Token {}'.format(self.access_token)})

        response_dict = response.json()
        pipeline_id = None
        for k in response_dict:

            if k["name"] == self.pipeline_name:
                pipeline_id = k["id"]

        if pipeline_id == None:
            print("Pipeline id corresponding to %s, not found" % (self.pipeline_name))

        pipeline_url = os.path.join(self.pipeline_contoller_url, str(pipeline_id) + "/")

        payload_completed = {
            'name': self.pipeline_name,
            'status': 'CO',
            'output': str(data)
        }
        response = requests.put(pipeline_url, json.dumps(payload_completed),
                                headers={'Content-Type': 'application/json',
                                         'Authorization': 'Token {}'.format(self.access_token)})

        return response


    def terminate(self):
        pass
