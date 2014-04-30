'''
Created on Mar 6, 2013

@author: Trey Duskin <trey@wilcoweb.net>

Copyright 2013 Maldivica (www.maldivica.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.key import Key
from boto.exception import S3ResponseError, S3CreateError
from robot.api import logger

class Keywords(object):
    
    def Set_S3_Credentials(self, access_key, secret_key, location=None):
        """
        Initializes a connection to an S3 API endpoint
        
        Must be called before any other keywords are used
        
        _access_key_ is the AWS access key
        _secret_key_ is the AWS secret key
        _location_ is the S3 API endpoint.  If not set, defaults to AWS, can 
                   be in the format https://host:port or http://host:port
        """
        if location:
            host, port, is_secure, path = self._parse_s3_location(location)
            
            logger.debug("Initializing S3Connection to host %s" % host)
            self._conn = S3Connection(access_key, secret_key,
                                      host=host, port=port, is_secure=is_secure,
                                      calling_format=OrdinaryCallingFormat(),
                                      validate_certs=False,
                                      path=path)
        else:
            logger.debug("Initializing S3Connection to AWS default")
            self._conn = S3Connection(access_key, secret_key,
                                      calling_format=OrdinaryCallingFormat(),
                                      validate_certs=False,
                                      path=path)

    def _parse_s3_location(self, location):
        is_secure=True
        host=location
        port=None
        
        split_location = location.split(':')
        
        if len(split_location) == 3:
            #three-part location, assume proto://host:port
            if split_location[0] == 'http':
                is_secure = False
            elif split_location[0] == 'https':
                is_secure = True
            else:
                raise Exception("Unknown method in location - %s" %
                                split_location[0])

            path = split_location[1].lstrip('/')
            port = int(split_location[2])
     
        elif len(split_location) == 2:
            #two part, could either be proto://host or host:port
            if split_location[0] == 'http':
                is_secure = False
                path = split_location[1].lstrip('/')
            elif split_location[0] == 'https':
                is_secure = True
                path = split_location[1].lstrip('/')
            else:
                #gotta be host:port          
                path = location.split(':')[0]
                port = int(location.split(':')[1])
                
        elif len(split_location) == 1:
            #one part, has to be a host
            path = location
            
        else:
            raise Exception("Can't parse location %s" % location)
        
        split_path = path.split('/',1)
        
        if len(split_path) > 1:
            path = split_path[1]
            host = split_path[0]
        else:
            host = split_path[0]
            path = ''

        return host, port, is_secure, path
        
    def Get_Bucket_List(self):
        """
        Returns a list of buckets in the S3 account
        """
        bucket_list = self._conn.get_all_buckets()
        
        result = [ b.name for b in bucket_list ]
        return result
    
    def Get_Object_List(self, bucket, prefix=None, exclude=None):
        """
        Returns a list of objects in _bucket_
        
        _prefix_ (optional): prefix used to filter results
        _exclude_ (optional): don't include results matching string
        """
        target_bucket = self._conn.get_bucket(bucket)
        objects = target_bucket.list(prefix=prefix)
        
        if exclude:
            object_list = [ obj.name.rstrip('/') for obj in objects 
                           if exclude not in obj.name ]
        else:
            object_list = [ obj.name.rstrip('/') for obj in objects ]
        return object_list
    
    def Get_Object_Metatags(self, obj, bucket):
        """
        Returns a dictionary of metatag keys and values for the given object
        
        _obj_: target object
        _bucket_: bucket containing object
        """
        s3bucket = self._conn.get_bucket(bucket)
        try:
            s3obj = Key(s3bucket, obj)
            logger.debug("Read 1 byte of object %s" % obj)
            s3obj.read(size=1)
        except S3ResponseError, err:
            if err.status == 404:
                dirobj = obj + '/'
                logger.debug("Could not find %s, trying %s" % (obj, dirobj))
                s3obj = Key(s3bucket, dirobj)
                logger.debug("Read 1 byte of object %s" % dirobj)
                s3obj.read(size=1)
        
        
        metadata_dict = s3obj.metadata
        #object metadata dict does not include normal object parameters
        #like last-modified, so we grab it manually and stuff it in the result
        
        metadata_dict['last-modified'] = s3obj.last_modified
        
        return metadata_dict
    
    def Delete_Bucket(self, bucket):
        """
        Deletes target bucket
        
        _bucket_: bucket to delete
        """
        bucketobject = self._conn.get_bucket(bucket)
        
        for obj in bucketobject.list():
            logger.debug("Deleting object %s" % obj.name)
            obj.delete()
        
        logger.debug("Deleting empty bucket %s" % bucket)
        self._conn.delete_bucket(bucket)
        
    def Empty_Bucket(self, bucket):
        """
        Deletes all objects in bucket, but leaves bucket in place.
        
        If bucket does not exist, it will be created.
        
        _bucket_: bucket to empty
        """
        try:
            bucketobject = self._conn.get_bucket(bucket)
            for obj in bucketobject.list():
                logger.debug("Deleting object %s" % obj.name)
                obj.delete()
        except S3ResponseError, err:
            if err.error_code == 'NoSuchBucket':
                logger.debug("No such bucket %s, creating" % bucket)
                self._conn.create_bucket(bucket)
            else:
                raise
            
    def Create_Bucket(self, bucket):
        """
        Creates a bucket
        
        _bucket_: name of bucket to create
        """
        logger.debug("Creating bucket %s" % bucket)
        self._conn.create_bucket(bucket)


        
                
                    
        
        
        
        
        
