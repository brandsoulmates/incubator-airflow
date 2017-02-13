# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import division
#from future import standard_library
#standard_library.install_aliases()
from six import string_types
from yaml import load
import json
import logging
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import math
import os
from StringIO import StringIO

boto3.set_stream_logger('boto3')
logging.getLogger("boto3").setLevel(logging.INFO)

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook

def _parse_aws_config(config_filename):
    
    with open(config_filename,'r') as yamlfile:
        config_dict = load(yamlfile)
    return config_dict

class AwsS3Hook(BaseHook):
    """
    Interact with Λ. This class is a wrapper around the boto library.
    """
    def __init__(self, aws_s3_conn_id='aws_default'):
        self.aws_s3_conn_id = aws_s3_conn_id
        self.aws_s3_conn = self.get_connection(aws_s3_conn_id)
        self.extra_params = self.aws_s3_conn.extra_dejson
        self.profile = self.extra_params.get('profile')
        self._creds_in_conn = 'aws_secret_access_key' in self.extra_params
        self._creds_in_config_file = 'aws_config_file' in self.extra_params
        if self._creds_in_conn:
            self.region = self.extra_params['region_name']
            self._a_key = self.extra_params['aws_access_key_id']
            self._s_key = self.extra_params['aws_secret_access_key']
        elif self._creds_in_config_file:
            self.s3_config_file = self.extra_params['aws_config_file']
        else:
            raise AirflowException("No AWS credentials supplied, no access to s3.")
        self.connection = self.get_conn()

    def __getstate__(self):
        pickled_dict = dict(self.__dict__)
        del pickled_dict['connection']
        return pickled_dict

    def __setstate__(self, d):
        self.__dict__.update(d)
        self.__dict__['connection'] = self.get_conn()

    @staticmethod
    def package_event(event):
        """
        packages json-compliant dicts into something that can be fed into a s3 call
        """
        
        # For now, we just serialize it.
        try:
            return json.dumps(event)
        except:
            raise AirflowException("event dict unable to be serialized as JSON!")

    @staticmethod
    def parse_s3_url(s3_path):
        if s3_path[:5] == "s3://":
            s3_path = s3_path[5:]
        try:
            bucket, key = s3_path.split("/", 1)
            return bucket, key
        except ValueError:
            # No splitting available, return just one
            logging.info("Nothing to split, returning "+s3_path)
            return None, s3_path

    def bucket_location(self,bucket):
        return self.s3.get_bucket_location(Bucket=bucket)["LocationConstraint"]\
                            or 'us-east-1' #us-east-1 returns None :(

    def string_parser(self, s3_path):
        if s3_path[:5] == "s3://":
            s3_path = s3_path[5:]
        try:
            bucket, key = s3_path.split("/", 1)
            return bucket, key
        except ValueError:
            # No splitting available, return just one
            print("Nothing to split, returning "+s3_path)
            return s3_path

    def existsBucket(self,bucket):
        try:
            self.bucket_location(bucket)
            return True
        except ClientError:
            return False

    def get_bucket(self, bucket_name):
        """
        Returns a boto.s3.bucket.Bucket object

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        """
        return self.connection.get_bucket(bucket_name)

    def delSuccess(self, bucket, key):
        success = True
        
        if self._verify_local_s3(bucket):
            try:
                self.local_bucket["s3"].delete_object(Bucket=bucket, Key=key)
            except ClientError as ex:
                # Important to print response, not just fail out
                print(ex.response)
                raise IOError("Unable to delete file "+bucket+"/"+key+" , see logs.")
                success = False
        else:
            success = False
        return success

    def mvSucess(self, bucket, key, new_bucket, new_key = None):
        '''
        This should move a file key from bucket to new_bucket. 
        It returns if it sucessfully moved (copied and deleted) or not.

         example code below
        '''

        #(CopySource, Bucket, Key, ExtraArgs=None, Callback=None, SourceClient=None, Config=None)
        assert ((bucket != new_bucket) or new_key) # Not a move
        if not new_key: new_key = key
        
        cps =  self.cpSuccess(new_bucket, new_key, {'Bucket': bucket,'Key': key})
        if cps:
            return self.delSuccess(bucket, key)
        else:
            return False
    
    def cpSuccess(self,bucket, key, copy_source):
        response = self.s3.copy(CopySource=copy_source, Bucket=bucket, Key=key)
        fct =  response.get("CopyObjectResult", dict()).get("LastModified", None)
        logging.info(response)
        if fct is not None:
            return True
        else:
            return False

    def list_keys(self, bucket, key_prefix, max_files = None, incl_dirs = False):
        
        list_kwargs = {"Bucket":bucket,"Prefix":key_prefix}
        if max_files is not None:
            list_kwargs["MaxKeys"] = max_files
        res = self.s3.list_objects_v2(**list_kwargs)
        files = []
        if 'Contents' in res:
            for val in res['Contents']:
                # Don't include prefixes unless we want to.
                if incl_dirs or not(val['Key'].endswith("/")):
                    files.append(val['Key'])
        return files

    def download_file(self, bucket, key, ofn):
        success = True
        try:
            self.s3.download_file(bucket, key, ofn)
        except ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                success = False
        return success

    def load_file(
            self,
            filename,
            key,
            bucket_name=None,
            replace=False,
            multipart_bytes=5 * (1024 ** 3),
            encryption=None):
        """
        Loads a local file to S3

        :param filename: name of the file to load.
        :type filename: str
        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which to store the file
        :type bucket_name: str
        :param replace: A flag to decide whether or not to overwrite the key
            if it already exists. If replace is False and the key exists, an
            error will be raised.
        :type replace: bool
        :param multipart_bytes: If provided, the file is uploaded in parts of
            this size (minimum 5242880). The default value is 5GB, since S3
            cannot accept non-multipart uploads for files larger than 5GB. If
            the file is smaller than the specified limit, the option will be
            ignored.
        :type multipart_bytes: int
        :param encrypt: If True, the file will be encrypted on the server-side
            by S3 and will be stored in an encrypted form while at rest in S3.
        :type encrypt: bool
        """
        if not bucket_name:
            bucket_name, key = self.parse_s3_url(key)
            if not bucket_name:
                raise AirflowException("No bucket provided, nowhere to upload to.")
        if not replace and self.check_file(bucket_name, key):
            raise ValueError("The key {key} already exists.".format(
                **locals()))

        key_size = os.path.getsize(filename)
        
        if multipart_bytes and key_size >= multipart_bytes:
            # multipart upload
            from filechunkio import FileChunkIO
            mp_params = {"Bucket":bucket_name,
                         "Key":key}
            if encryption: mp_params['ServerSideEncryption'] = encryption
            mp = self.connection.create_multipart_upload(**mp_params)
            total_chunks = int(math.ceil(key_size / multipart_bytes))
            sent_bytes = 0
            try:
                for chunk in range(total_chunks):
                    offset = chunk * multipart_bytes
                    wbytes = min(multipart_bytes, key_size - offset)
                    with FileChunkIO(
                            filename, 'r', offset=offset, bytes=wbytes) as fp:
                        logging.info('Sending chunk {c} of {tc}...'.format(
                            c=chunk + 1, tc=total_chunks))
                        up_params = {"Body":fp, "PartNumber":chunk+1,
                                     "UploadId":mp['UploadId'],
                                     "Bucket":bucket_name,
                                     "Key":key,"ContentLength":wbytes}
                        mp.upload_part(**up_params)
            except:
                self.connection.abort_multipart_upload(Bucket = mp["Bucket"],
                                                       Key = mp["Key"],
                                                       UploadId = mp["UploadId"])
                raise
            self.connection.complete_multipart_upload(Bucket = mp["Bucket"],
                                                       Key = mp["Key"],
                                                       UploadId = mp["UploadId"])
        else:
            # regular upload
            self.connection.upload_file(filename, bucket_name, key)
            
        logging.info("The key {key} now contains"
                     " {key_size} bytes".format(**locals()))

    def load_string(self, string_data,
                    key, bucket_name=None,
                    replace=False):
        """
        Loads a local file to S3

        This is provided as a convenience to drop a file in S3. It uses the
        boto infrastructure to ship a file to s3. It is currently using only
        a single part download, and should not be used to move large files.

        :param string_data: string to set as content for the key.
        :type string_data: str
        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which to store the file
        :type bucket_name: str
        :param replace: A flag to decide whether or not to overwrite the key
            if it already exists
        :type replace: bool
        :param encrypt: If True, the file will be encrypted on the server-side
            by S3 and will be stored in an encrypted form while at rest in S3.
        :type encrypt: bool

        """
        if not bucket_name:
            bucket_name, key = self.parse_s3_url(key)
        if not replace and self.check_file(bucket_name, key):
            raise ValueError("The key {key} already exists.".format(
                **locals()))
        elif not bucket_name:
            raise AirflowException("No bucket provided, nowhere to upload to.")

        fp = StringIO(string_data)
        fp.seek(0)
        self.connection.upload_fileobj(fp, bucket_name, key)
        
        logging.info("The key {key} now contains"
                     " {key_size} bytes".format(**locals()))

    def check_file(self, bucket, key):
        res = self.s3.list_objects(Bucket=bucket, Prefix=key)
        if 'Contents' in res:
            return True
        return False

    def parse_file_json(self, bucket_name, key):
        fp = StringIO()
        self.s3.download_filobj(bucket_name, key, fp)
        fp.seek(0)
        return json.load(fp)
    
    def parse_file_yaml(self, bucket_name, key):
        fp = StringIO()
        self.s3.download_filobj(bucket_name, key, fp)
        fp.seek(0)
        return load(fp)
    
    def load_json(self, json_dict, bucket_name, key, replace = False):
        self.load_string(json.dumps(json_dict), key, bucket_name, replace)
    
    def get_conn(self):
        """
        Returns the boto s3 connection object.
        """
        a_key = s_key = None
        if self._creds_in_config_file:
            config_dict = _parse_aws_config(self.s3_config_file)
            a_key = config_dict['aws_access_key_id']
            s_key = config_dict['aws_secret_access_key']
            region_name = config_dict.get('region_name')
        elif self._creds_in_conn:
            a_key = self._a_key
            s_key = self._s_key
            region_name = self.region_name

        config_params = {"signature_version":"s3v4"}
        if region_name:
            config_params['region_name'] = region_name

        connection = boto3.client('s3',
            aws_access_key_id=a_key,
            aws_secret_access_key=s_key,
            config = Config(**config_params))
        return connection
    