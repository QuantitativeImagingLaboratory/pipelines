from resources.resource_s3 import resource_s3
from pipelineinit.init import init
import os

class fetch_s3(init):
    def __init__(self, lastprocess, bootstrap_servers, bucket_name = 'vaaas-media'):
        self.s3 = resource_s3(bucket_name)
        super().__init__(lastprocess, bootstrap_servers)

    def init(self, folder, file, output):
        try:
            self.s3.download(folder, file, output)

        except:
            print("Failed to download: ", file)
        self.end_init()


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("-b", "--bucket", dest="bucket",
                        help="specify the name of the bucket", metavar="BUCKET", default='vaaas-media')
    parser.add_argument("-d", "--dir", dest="dir",
                        help="specify the name of the directory", metavar="DIRECTORY")
    parser.add_argument("-f", "--file", dest="file",
                        help="specify the name of the file", metavar="FILE")
    parser.add_argument("-o", "--output", dest="output",
                        help="specify the name of the output file", metavar="OUTPUT")
    parser.add_argument("-lp", "--last-process", dest="last_process",
                        help="specify True if this is the last process", metavar="LASTPROCESS", default=False)
    parser.add_argument("-cb", "--consumer-bootstrap-server", dest="bootstrap_servers",
                        help="specify the name of the bootstrap_servers", metavar="BOOTSTRAP", default='localhost:9092')

    args = parser.parse_args()

    s3 = fetch_s3(bucket_name=args.bucket, lastprocess = args.last_process, bootstrap_servers=args.bootstrap_servers)
    s3.init(args.dir, args.file, args.output)

