## a suite of functions desgined to provide
## high-level interface to execute common 
## s3 resource related operation
##
## access aws resource's api requires aws access key
## stored at "~/.aws/credentials"
##
## Author: Yi Zhang <beingzy@gmail.com>
## Date: DEC/16/2015
##
import os
import sys
import re  
import threading
import boto3
from boto3.s3.transfer import S3Transfer

def s3_list_all_buckets(print_out=False):
    """ list all buckets' name of associated account """
    s3 = boto3.resource('s3')
    bucket_names = []
    for ii, bucket in enumerate(s3.buckets.all()):
        bucket_names.append(bucket.name)
        if print_out:
            print( "-- {}: {} \n".format(ii+1, bucket.name) )
    return(bucket_names)
	
def s3_create_new_bucket(name, **kwargs):
    """ create a new bucket """
    s3 = boto3.resource('s3')
    # check if bucket exists already
    exist_buckets = s3_list_all_buckets(print_out=False)
    if name in exist_buckets:
        print("WARNING: Same name had been found for an existing bucket !")
        return

    # create bucket
    resp = s3.create_bucket(Bucket=name, **kwargs)
    return 
	
def s3_upload_data(file_path, bucket_name, object_name, **kwargs):
    """ create a new object in S3's specified bucket 
	    to store provided local data 
    """
    tansfer = S3Transfer(boto3.client('s3'))
    transfer.upload_file( filename=file_path, bucket=bucket_name, key=object_name, 
        callback=ProgressPercentage(file_path) )
    return
	
def s3_download_data(file_path, bucket_name, object_name, **kwargs):
    """ download s3's object and save it to file_path
	"""
    tansfer = S3Transfer(boto3.client('s3'))
    transfer.upload_file( filename=file_path, bucket=bucket_name, key=object_name, 
        callback=ProgressPercentage(file_path) )
    return
	    
class ProgressPercentage(object):
    # source code is eferrenced on boto3 official doc: 
	#    http://boto3.readthedocs.org/en/latest/_modules/boto3/s3/transfer.html
    def __init__(self, filename):
        self._filename = filename 
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # to simplify we'll assuem this is hooked up 
		# to a single filename. 
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = ( self._seen_so_far * 1.0 / self._size ) * 100
            sys.stdout.write(
				"\rUploading: {} {:.2f} mb / {:.2f} mb (Progress: {:.2f}%, Speed: {:.2f} kb)".format(self._filename, self._seen_so_far / 1000000,
				                               self._size / 1000000, percentage, bytes_amount / 1000)
            )
            sys.stdout.flush()
		
	

	
	
