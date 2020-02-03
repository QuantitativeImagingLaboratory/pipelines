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


class resource_kinesis:
    def __init__(self):
        aws_access = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")

        self.kvs = boto3.client('kinesisvideo',
                           region_name='us-west-2',
                           aws_access_key_id=aws_access,
                           aws_secret_access_key=aws_secret,

                           )

    def get_endpoint(self, streamName):
        endpoint = self.kvs.get_data_endpoint(
            APIName="GET_HLS_STREAMING_SESSION_URL",
            StreamName=streamName
        )

        return endpoint['DataEndpoint']

    def get_hls_url(self, streamName):
        kvam = boto3.client("kinesis-video-archived-media",
                            endpoint_url=self.get_endpoint(streamName),
                            region_name='us-west-2',
                            )
        # url = kvam.get_hls_streaming_session_url(
        #     StreamName=streamName,
        #     PlaybackMode="LIVE",
        #     Expires=43200
        # )
        url = kvam.get_hls_streaming_session_url(
            StreamName=streamName,
            PlaybackMode="LIVE",
            HLSFragmentSelector={
                'FragmentSelectorType': 'SERVER_TIMESTAMP'
            },
            DisplayFragmentTimestamp='ALWAYS',
            Expires=43200
        )
        print(url)
        return url['HLSStreamingSessionURL']


