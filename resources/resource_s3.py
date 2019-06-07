import boto3
import botocore

import os


class resource_s3:
    def __init__(self, bucket_name):
        self.bucket = bucket_name
        self.s3 = boto3.resource('s3')

    def download(self, folder, file, output):

        key = os.path.join(folder, file)
        if not output:
            output = file
        try:
            self.s3.Bucket(self.bucket).download_file(key, output)
            print("Successfully downloaded: ", key)
        except botocore.exceptions.ClientError as e:
            print("Failed to download: ", key)
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

    def upload(self, file_on_os, s3_folder, s3_filename):
        file = os.path.join(s3_folder, s3_filename)
        self.s3.Bucket(self.bucket).upload_file(file_on_os, file)
        print("Upload successful: ", file)