from resources.resource_s3 import resource_s3
from pipelineterminate.terminate import terminate
import os
from datetime import datetime
import time

class put_s3(terminate):
    def __init__(self, lastprocess, bootstrap_servers, s3dir, inputvideofile, pipeline_name, bucket_name = 'vaaas-media'):
        self.s3 = resource_s3(bucket_name)
        super().__init__(lastprocess, bootstrap_servers)

        self.videofile = inputvideofile
        self.pipeline_name = pipeline_name
        self.s3dir = s3dir

        now = datetime.now().strftime("_%Y%m%d%H%M%S")

        s3_folder_video_name = os.path.join(self.s3dir, self.videofile.split(".")[0])
        self.output_folder_s3 = os.path.join(s3_folder_video_name, pipeline_name + now)

    @staticmethod
    def get_parser():
        parser = terminate.default_parser()
        parser.add_argument("-b", "--bucket", dest="bucket",
                            help="specify the name of the bucket", metavar="BUCKET", default='vaaas-media')
        parser.add_argument("-sd", "--s3-dir", dest="s3dir",
                            help="specify the name of the directory on s3", metavar="DIRECTORY")
        parser.add_argument("-f", "--file", dest="file",
                            help="specify the name of the input video file", metavar="FILE")

        return parser

    def terminate(self):
        print(self.pipeline_output_folder)
        for root, dirs, files in os.walk(self.pipeline_output_folder):
            for file in files:
                print(root, file)
                self.s3.upload(os.path.join(root, file), self.output_folder_s3, file)
        time.sleep(10)
        self.end_terminate()


if __name__ == "__main__":
    parser = put_s3.get_parser()
    args = parser.parse_args()

    s3 = put_s3(s3dir=args.s3dir, inputvideofile=args.file, pipeline_name=args.pipeline_name, bucket_name=args.bucket, lastprocess=args.last_process, bootstrap_servers = args.bootstrap_servers)
    s3.terminate()