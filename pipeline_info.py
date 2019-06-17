import pipelinesource.videosource
import pipelineprocess.alert.thresholding
import pipelineprocess.alert.movingaverage
import pipelineprocess.core.count
import pipelineprocess.core.add
import pipelineprocess.highvision.crowdcounting.mcnn.mcnn
import pipelineprocess.highvision.objectdetection.detectors.yolov3.yolov3
import pipelineprocess.highvision.objectdetection.filters.filterbylocation
import pipelineprocess.highvision.objectdetection.filters.filterbyclass
import pipelinesink.videosink
import pipelinesink.csvsink

import pipelineinit.fetchdata.fetch_s3

from pipelinetypes import *

class pipeline_info:

    modules = [
        pipelinesource.videosource.videosource,

        pipelineprocess.alert.thresholding.thresholding, pipelineprocess.alert.movingaverage.movingaverage,

        pipelineprocess.core.count.count, pipelineprocess.core.add.add,

        pipelineprocess.highvision.crowdcounting.mcnn.mcnn.mcnn,

        pipelineprocess.highvision.objectdetection.detectors.yolov3.yolov3.yolov3,
        pipelineprocess.highvision.objectdetection.filters.filterbylocation.filterbylocation,
        pipelineprocess.highvision.objectdetection.filters.filterbyclass.filterbyclass,

        pipelinesink.videosink.videosink,
        pipelinesink.csvsink.csvsink,

    ]

    @staticmethod
    def get_modules():
        return pipeline_info.modules

    @staticmethod
    def get_mappings(outputclass):

        if not hasattr(outputclass, "output"):
            return []
        list_of_modules = []
        for eachclass in pipeline_info.modules:

            if hasattr(eachclass, "input"):
                outputclass_dict = getattr(outputclass, "output")
                eachclass_dict = getattr(eachclass, "input")

                outputclass_key = list(outputclass_dict.keys())[0]
                eachclass_key = list(eachclass_dict.keys())[0]

                for eachtype in pipeline_types:

                    if eachclass_dict[eachclass_key] in eachtype and outputclass_dict[outputclass_key] in eachtype:
                        list_of_modules += [eachclass]


        return list_of_modules

    @staticmethod
    def get_command(class_name):
        return class_name.get_command()

    @staticmethod
    def get_command_info(class_name):
        return class_name.get_command_info()


    @staticmethod
    def get_modules1():
        from subprocess import Popen, PIPE
        from re import search

        def get_classes(directory):
            job = Popen(['egrep', '-ir', '--include=*.py', 'class ', str(directory), ], stdout=PIPE)

            fileout, fileerr = job.communicate()
            fileout = fileout.decode('utf-8')


            if fileerr:
                raise Exception(fileerr)
            while directory[-1] == '/':
                directory = directory[:-1]

            found = []



            for line in fileout.split('\n'):

                match = search('^([^:]+).py:\s*class\s*(\S+)\s*\((\S+)\):', line)
                if match:
                    pypath = match.group(1).replace(directory, '').replace('/', '.')[1:]
                    cls = match.group(2)
                    parents = filter(lambda x: x.strip, match.group(3).split())
                    found.append((pypath, cls, parents,))

            return found

        list_of_classes = get_classes("/home/pmantini/qil-software/pipeline/")

        for cl in list_of_classes:
            try:
                mod = __import__(cl[0].rsplit(".", 1)[0])
                getattr(mod, cl[1])
                print(cl[0].rsplit(".", 1)[0])
            except:
                pass





# if __name__ == "__main__":
#     print(pipeline_info.get_command_info(pipelineprocess.core.add.add))