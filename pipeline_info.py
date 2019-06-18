import pipelineinit.fetchdata.fetch_s3
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
import pipelineterminate.putdata.put_s3




from pipelinetypes import *


class pipeline_info:

    init_modules = [
        {'fetch_s3': {'type': "init", 'class':pipelineinit.fetchdata.fetch_s3}},
        {'videosource': {'type':"source", 'class':pipelinesource.videosource.videosource}}
    ]


    terminate_modules = [
        {'put_s3': {'type': "terminate", 'class': pipelineterminate.putdata.put_s3}},
    ]

    modules = {
        'thresholding': {'type': "process", 'class': pipelineprocess.alert.thresholding.thresholding},
        'movingaverage': {'type': "process", 'class': pipelineprocess.alert.movingaverage.movingaverage},
        'count': {'type': "process", 'class': pipelineprocess.core.count.count},
        'add': {'type': "process", 'class': pipelineprocess.core.add.add},
        'mcnn': {'type': "process", 'class': pipelineprocess.highvision.crowdcounting.mcnn.mcnn.mcnn},
        'yolov3': {'type': "process", 'class': pipelineprocess.highvision.objectdetection.detectors.yolov3.yolov3.yolov3},
        'filterbylocation': {'type': "process", 'class': pipelineprocess.highvision.objectdetection.filters.filterbylocation.filterbylocation},
        'filterbyclass': {'type': "process", 'class': pipelineprocess.highvision.objectdetection.filters.filterbyclass.filterbyclass},
        'videosink': {'type': "process", 'class': pipelinesink.videosink.videosink},
        'csvsink': {'type': "process", 'class': pipelinesink.csvsink.csvsink}
    }

    @staticmethod
    def get_modules():

        return list(pipeline_info.modules.keys())

    @staticmethod
    def get_init_modules():
        mods = [list(k.keys())[0] for k in pipeline_info.init_modules]
        return mods

    @staticmethod
    def get_terminate_modules():
        mods = [list(k.keys())[0] for k in pipeline_info.terminate_modules]
        return mods

    @staticmethod
    def get_class(class_name):
        return pipeline_info.modules[class_name]['class']


    @staticmethod
    def get_mappings(outputclass_name):
        outputclass = pipeline_info.get_class(outputclass_name)

        if not hasattr(outputclass, "output"):
            return []
        list_of_modules = []
        for eachmod in pipeline_info.modules:

            if hasattr(pipeline_info.modules[eachmod]['class'], "input"):
                outputclass_dict = getattr(outputclass, "output")
                eachclass_dict = getattr(pipeline_info.modules[eachmod]['class'], "input")

                outputclass_key = list(outputclass_dict.keys())[0]
                eachclass_key = list(eachclass_dict.keys())[0]

                for eachtype in pipeline_types:

                    if eachclass_dict[eachclass_key] in eachtype and outputclass_dict[outputclass_key] in eachtype:
                        if eachmod != outputclass_name:
                            list_of_modules += [eachmod]


        return list_of_modules

    @staticmethod
    def get_command(class_name):
        mod_class = pipeline_info.get_class(class_name)
        return mod_class.get_command()

    @staticmethod
    def get_command_info(class_name):
        mod_class = pipeline_info.get_class(class_name)
        return mod_class.get_command_info()


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
#     print(pipeline_info.get_command_info('yolov3'))