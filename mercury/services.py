#!/usr/bin/env python

import os, sys
import uuid
import time
import json
import copy
from contextlib import contextmanager
from collections import namedtuple

import requests
import boto3

from snap import common

s3_auth_error_mesage = '''
S3ServiceObject must pe passed the "aws_key_id" and "aws_secret_key"
parameters if the "auth_via_iam" init param is not set (or is False).'''

kinesis_auth_error_mesage = '''
KinesisServiceObject must pe passed the "aws_key_id" and "aws_secret_key"
parameters if the "auth_via_iam" init param is not set (or is False).'''


class S3Key(object):
    def __init__(self, bucket_name, s3_object_path):
        self.bucket = bucket_name
        self.folder_path = self.extract_folder_path(s3_object_path)
        self.object_name = self.extract_object_name(s3_object_path)
        self.object_path = s3_object_path

    def extract_folder_path(self, s3_key_string):
        if s3_key_string.find('/') == -1:
            return ''
        key_tokens = s3_key_string.split('/')
        return '/'.join(key_tokens[0:-1])

    def extract_object_name(self, s3_key_string):
        if s3_key_string.find('/') == -1:
            return s3_key_string
        return s3_key_string.split('/')[-1]

    def __str__(self):
        return self.object_path

    @property
    def uri(self):
	    return os.path.join('s3://', self.bucket, self.object_path)


class S3Service(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('local_temp_path', 'region')
        kwreader.read(**kwargs)

        self.local_tmp_path = kwreader.get_value('local_temp_path')
        self.region = kwreader.get_value('region')
        self.s3session = None
        self.aws_access_key_id = None
        self.aws_secret_access_key = None

        # we set this to True if we are initializing this object from inside an AWS Lambda,
        # because in that case we do not require the aws credential parameters to be set.
        # The default is False, which is what we want when we are creating this object
        # in a normal (non-AWS-Lambda) execution context: clients must pass in credentials.
        should_authenticate_via_iam = kwargs.get('auth_via_iam', False)

        if not should_authenticate_via_iam:
            log.info("NOT authenticating via IAM. Setting credentials now.")
            self.aws_access_key_id = kwargs.get('aws_key_id')
            self.aws_secret_access_key = kwargs.get('aws_secret_key')
            if not self.aws_secret_access_key or not self.aws_access_key_id:
                raise Exception(s3_auth_error_mesage)           
            self.s3client = boto3.client('s3',
                                         aws_access_key_id=self.aws_access_key_id,
                                         aws_secret_access_key=self.aws_secret_access_key)
        else:
            self.s3client = boto3.client('s3', region_name=self.region)
 

    def upload_object(self, local_filename, bucket_name, bucket_path=None):
        s3_path = None
        with open(local_filename, 'rb') as data:
            base_filename = os.path.basename(local_filename)
            if bucket_path:
                s3_path = os.path.join(bucket_path, base_filename)
            else:
                s3_path = base_filename

            self.s3client.upload_fileobj(data, bucket_name, s3_path)

        return S3Key(bucket_name, s3_path)

    def upload_json(self, data_dict, bucket_name, bucket_path):
        binary_data = bytes(json.dumps(data_dict), 'utf-8')
        self.s3client.put_object(Body=binary_data, 
                                 Bucket=bucket_name, 
                                 Key=bucket_path)


    def upload_bytes(self, bytes_obj, bucket_name, bucket_path):        
        self.s3client.put_object(Body=bytes_obj, Bucket=bucket_name, Key=bucket_path)
        return S3Key(bucket_name, bucket_path).uri
    
    
    def download_object(self, bucket_name, s3_key_string):
        s3_object_key = S3Key(bucket_name, s3_key_string)
        local_filename = os.path.join(self.local_tmp_path, s3_object_key.object_name)
        with open(local_filename, "wb") as f:
            self.s3client.download_fileobj(bucket_name, s3_object_key.object_path, f)

        return local_filename
    

    def download_json(self, bucket_name, s3_key_string):
        #s3_object_key = S3Key(s3_key_string)

        obj = self.s3client.get_object(Bucket=bucket_name, Key=s3_key_string)
        return json.loads(obj['Body'].read().decode('utf-8'))


import sqlalchemy as sqla
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy_utils import UUIDType


class DBTask(object):
    def __init__(self, name='anonymous DB task'):
        self.name = name
        self._id = None

    def _get_uuid(self):
        # can be overridden in descendant classes
        return uuid.uuid4()

    def __str__(self):
        return 'dbtask[%s]:%s' % (self.name, self.uuid)

    @property
    def uuid(self):
        if not self._id:
            self._id = self._get_uuid()
        return self._id
        

class ConnectionPoolManager(object):
    def __init__(self, hostname, db_name, username, password, port='5432'):
        #self.connect_string = 'postgresql://'+user+':'+password+'@'+host+':'+port+'/'+dbname
        #conn = create_engine(connect_string, pool_size=20, max_overflow_size=0)
        self.hostname = hostname
        self.dbname = db_name
        self.username = username
        self.password = password
        self.port = port
        self.connection_pool = pool.QueuePool(self._getconn,
                                             max_overflow=10,
                                             pool_size=5)
        self.connection_table = {}
        
    
    def _get_connection(self): # can override in subclasses
        c = psycopg2.connect(username=self.username,
                            host=self.hostname,
                            dbname=self.dbname,
                            password=self.password,
                            port=self.port)
        return c

    
    def get_connection(self, db_task):
        conn = self._get_connection()
        self.connection_table[db_task.uuid()] = conn
        return conn
    

@contextmanager
def get_connection(connection_pool_mgr, db_task):    
    conn = connection_mgr.get_connection(db_task)
    yield conn
    connection_mgr.close_connection(db_task)


class RedshiftServiceObject(object):
    def __init__(self, **kwargs):    
        self.host = kwargs['host']
        self.db_name = kwargs['database']
        self.port = kwargs['port']
        self.username = kwargs['username']
        self.schema = kwargs['schema']
        password = kwargs['password']
        
        self.metadata = None
        self.engine = None
        self.session_factory = None
        self.Base = None
                
        url_template = '{db_type}://{user}:{passwd}@{host}:{port}/{database}'
        db_url = url_template.format(db_type='redshift+psycopg2',
                                     user=self.username,
                                     passwd=password,
                                     host=self.host,
                                     port=self.port,
                                     database=self.db_name)
        retries = 0
        connected = False
        while not connected and retries < 3:
            try:
                print('### Connecting to Redshift DB...', file=sys.stderr)                
                self.engine = sqla.create_engine(db_url)
                
                print('### created engine, performing reflection...', file=sys.stderr)
                self.metadata = MetaData(schema=self.schema)                
                self.metadata.reflect(bind=self.engine)                
                self.Base = automap_base(bind=self.engine, metadata=self.metadata)
                                
                print('### automapped data types, preparing base...', file=sys.stderr)
                self.Base.prepare(self.engine, reflect=True)                
                self.session_factory = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)

                print('### created session factory', file=sys.stderr)
                connected = True
                print('### Connected to Redshift DB at %s.' % self.host, file=sys.stderr)
                
            except Exception as err:
                print(err)
                print(err.__class__.__name__)
                print(err.__dict__)
                time.sleep(1)
                retries += 1
            
        if not connected:
            raise Exception('!!! Unable to connect to Redshift db on host %s at port %s.' % (self.host, self.port))
        

    @contextmanager
    def txn_scope(self):
        session = self.session_factory()        
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


    @contextmanager    
    def connect(self):
        connection = self.engine.connect()
        try:
            yield connection
        finally:
            connection.close()


class PostgreSQLService(object):
    def __init__(self, **kwargs):
        
        self.db_name = kwargs['database']
        self.host = kwargs['host']
        self.port = int(kwargs.get('port', 5432))
        self.username = kwargs['username']
        self.password = kwargs['password']        
        self.schema = kwargs['schema']
        self.metadata = None
        self.engine = None
        self.session_factory = None
        self.Base = None
                
        url_template = '{db_type}://{user}:{passwd}@{host}/{database}'
        db_url = url_template.format(db_type='postgresql+psycopg2',
                                     user=self.username,
                                     passwd=self.password,
                                     host=self.host,
                                     port=self.port,
                                     database=self.db_name)
        
        retries = 0
        connected = False
        while not connected and retries < 3:
            try:
                self.engine = sqla.create_engine(db_url, echo=False)
                self.metadata = MetaData(schema=self.schema)
                self.Base = automap_base(bind=self.engine, metadata=self.metadata)
                self.Base.prepare(self.engine, reflect=True)
                self.metadata.reflect(bind=self.engine)
                self.session_factory = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)
                connected = True
                print('### Connected to PostgreSQL DB.', file=sys.stderr)
                
            except Exception as err:
                print(err)
                print(err.__class__.__name__)
                print(err.__dict__)
                time.sleep(1)
                retries += 1
            
        if not connected:
            raise Exception('!!! Unable to connect to PostgreSQL db on host %s at port %s.' % (self.host, self.port))

    @contextmanager
    def txn_scope(self):
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


    @contextmanager    
    def connect(self):
        connection = self.engine.connect()
        try:
            yield connection
        finally:
            connection.close()


class SimpleRedshiftService(object):
    def __init__(self, **kwargs):    
        self.host = kwargs['host']        
        self.db_name = kwargs['database']        
        self.port = kwargs['port']        
        self.username = kwargs['username']
        self.schema = kwargs['schema']
        password = kwargs['password']        

        self.metadata = None
        self.engine = None
        self.session_factory = None
        self.Base = None
                
        url_template = '{db_type}://{user}:{passwd}@{host}:{port}/{database}'
        db_url = url_template.format(db_type='redshift+psycopg2',
                                     user=self.username,
                                     passwd=password,
                                     host=self.host,
                                     port=self.port,
                                     database=self.db_name)
        retries = 0

        connected = False
        while not connected and retries < 3:
            try:
                print('### Connecting to Redshift DB...', file=sys.stderr)                
                self.engine = sqla.create_engine(db_url)
                
                #self.Base.prepare(self.engine, reflect=True)
                self.session_factory = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)
                print('### created session factory', file=sys.stderr)
                connected = True
                print('### Connected to Redshift DB.', file=sys.stderr)
                
            except Exception as err:
                print(err, file=sys.stderr)
                print(err.__class__.__name__, file=sys.stderr)
                print(err.__dict__, file=sys.stderr)
                time.sleep(1)
                retries += 1
            
        if not connected:
            raise Exception('!!! Unable to connect to Redshift db on host %s at port %s.' % (self.host, self.port))
        

    @contextmanager
    def txn_scope(self):
        session = self.session_factory()        
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


    @contextmanager    
    def connect(self):
        connection = self.engine.connect()
        try:
            yield connection
        finally:
            connection.close()




class KinesisServiceObject(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('stream',
                                           'region')
        kwreader.read(**kwargs)
        self.stream_name = kwreader.get_value('stream')
        self.region = kwreader.get_value('region')
        should_authenticate_via_iam = kwargs.get('auth_via_iam', False)

        if not should_authenticate_via_iam:
            key_id = kwargs.get('aws_key_id')
            secret_key = kwargs.get('aws_secret_key')
            if not key_id or not secret_key:
                raise Exception(kinesis_auth_error_mesage)

            self.kinesis_client = boto3.client('kinesis',
                                               aws_access_key_id=key_id,
                                               aws_secret_access_key=secret_key,
                                               region_name=self.region)
        else:
            self.kinesis_client = boto3.client('kinesis', region_name=self.region)


    def generate_partition_key(self, record):
        return str(uuid.uuid4())

    
    def bulk_write(self, record_dict_array, stream_name):
        input_records = []
        for record in record_dict_array:
            pkey = self.generate_partition_key(record)
            data = json.dumps(record).encode()   # defaults to utf-8
            input_records.append({'Data': data, 'PartitionKey': pkey})
        return self.kinesis_client.put_records(Records=input_records, StreamName=stream_name)


    def write(self, record_dict, stream_name):
        pkey = self.generate_partition_key(record_dict)
        return self.kinesis_client.put_record(StreamName=stream_name,
                                              Data=json.dumps(record_dict).encode(),
                                              PartitionKey=pkey)


CognitoUserAttribute = namedtuple('CognitoUserAttribute', 'name value')
COGNITO_AUTH_ERROR_MESSAGE = 'You must provide a valid set of AWS credentials to start this service.'

class AWSCognitoService(object):
    def __init__(self, **kwargs):     
        kwreader = common.KeywordArgReader('user_pool_id', 'client_id', 'aws_region')
        kwreader.read(**kwargs)
        self.user_pool_id = kwreader.get_value('user_pool_id')
        self.client_id = kwreader.get_value('client_id')
        self.aws_region = kwreader.get_value('aws_region')
        self.client_secret = kwargs.get('client_secret')
        
        should_authenticate_via_iam = kwargs.get('auth_via_iam', False)

        if not should_authenticate_via_iam:
            key_id = kwargs.get('aws_key_id')
            secret_key = kwargs.get('aws_secret_key')
            if not key_id or not secret_key:
                raise Exception(COGNITO_AUTH_ERROR_MESSAGE)
        
            self.cognito_client = boto3.client('cognito-idp',
                                               aws_access_key_id=key_id,
                                               aws_secret_access_key=secret_key,
                                               region_name=self.aws_region)
        else:
            self.cognito_client = boto3.client('cognito-idp', region_name=self.aws_region)


    def generate_secret_hash(self, username):
        if not self.client_secret:
            raise Exception('Cognito client was spun up without specifying a client secret. Cannot create a valid secret hash.')
        message = username + self.client_id
        digest = hmac.new(self.client_secret.encode(),
                          message.encode(),
                          digestmod=hashlib.sha256).digest()
        signature = base64.b64encode(digest).decode()
        return signature

    
    def generate_temp_password(self):
        return 'foobarpassword'

            
    def change_initial_password(self, username, new_password, session_key):
        payload = {}
        payload['ChallengeName'] = 'NEW_PASSWORD_REQUIRED'
        payload['ClientId'] = self.client_id
        payload['UserPoolId'] = self.user_pool_id
        payload['ChallengeResponses'] = {
            'NEW_PASSWORD': new_password,
            'USERNAME': username,
            'SECRET_HASH': self.generate_secret_hash(username)
            }
        payload['Session'] = session_key
        return self.cognito_client.admin_respond_to_auth_challenge(**payload)


    def reset_password(self, username):
        payload = {
            'Username': username,
            'UserPoolId': self.user_pool_id
            }
        return self.cognito_client.admin_reset_user_password(**payload)


    def force_verify_email(self, username):        
        payload = {
            'UserPoolId': self.user_pool_id,
            'Username': username,
            'UserAttributes': [{ 'Name': 'email_verified', 'Value': 'true' }]
        }
        return self.cognito_client.admin_update_user_attributes(**payload)


    def verify_named_attribute(self, attr_name, access_token, code):
        payload = {}
        payload['AccessToken'] = access_token
        payload['AttributeName'] = attr_name
        payload['Code'] = code
        return self.cognito_client.verify_user_attribute(**payload)

    
    def get_verification_code_for_named_attribute(self, attr_name, access_token):
        return self.cognito_client.get_user_attribute_verification_code(AccessToken=access_token,
                                                                        AttributeName=attr_name)

    
    def lookup_user(self, username):
        try:
            result = self.cognito_client.admin_get_user(UserPoolId=self.user_pool_id, Username=username)
            return result
        except botocore.errorfactory.UserNotFoundException as err:
            return {}


    def user_create(self, username, attribute_list=[], **kwargs):
        payload = {}
        payload['DesiredDeliveryMediums'] = ['EMAIL'] # how to send invitation message to new user
        payload['ForceAliasCreation'] = False
        payload['MessageAction'] = 'SUPPRESS' # re-send confirmation message if user already exists
        payload['TemporaryPassword'] = kwargs.get('password')  or self.generate_temp_password()
        payload['UserAttributes'] = [{'Name': attr.name, 'Value': attr.value} for attr in attribute_list]
        payload['Username'] = username
        payload['UserPoolId'] = self.user_pool_id
        # skip ValidationData parameter for now; may be required later
        return self.cognito_client.admin_create_user(**payload)


    def forgot_password(self, username):
        payload = {}
        payload['ClientId'] = self.client_id
        payload['Username'] = username
        payload['SecretHash'] = self.generate_secret_hash(username)
        return self.cognito_client.forgot_password(**payload)

    
    def user_login(self, username, password, **kwargs):
        payload = {}
        payload['UserPoolId'] = self.user_pool_id
        payload['ClientId'] = self.client_id
        payload['AuthFlow'] =  'ADMIN_NO_SRP_AUTH'
        payload['AuthParameters'] = {
            'USERNAME': username,
            'PASSWORD': password,
            'SECRET_HASH': self.generate_secret_hash(username)
        }

        return self.cognito_client.admin_initiate_auth(**payload)
        # TODO: status = CognitoAuthStatus(response) and return the status object
        

class AWSEmailService(object):
    def __init__(self, **kwargs):
        self.region = kwargs.get('aws_region')
        self.charset = 'utf-8'
        self.ses_client = boto3.client('ses', region_name=self.region)


    def _create_message(self, sender, recipient_list, subject, body):
        # Create a multipart/alternative child container.
        email_message = MIMEMultipart('mixed')
        email_message['Subject'] = subject
        email_message['From'] = sender
        email_message['To'] = ', '.join(recipient_list)
        # Encode the text and HTML content and set the character encoding. This step is
        # necessary if you're sending a message with characters outside the ASCII range.
        textpart = MIMEText(body.encode(self.charset), 'plain', charset)
        htmlpart = MIMEText(body.encode(self.charset), 'html', charset)

        msg_body = MIMEMultipart('alternative')
        # Add the text and HTML parts to the child container.
        msg_body.attach(textpart)
        msg_body.attach(htmlpart)
        email_message.attach(msg_body)

        return email_message
        

    def _create_attachment(self, filename):
        with open(filename, 'rb') as f:
            att = MIMEApplication(f.read())
            att.add_header('Content-Disposition','attachment',filename=os.path.basename(filename))
        return att


    def send(self,
             sender_address,
             recipient_list,
             subject,
             body,
             attachment_filename=None):

        message = self.create_message(sender_address, recipient_list, subject, body)
        if attachment_filename:
            attachment = self._create_attachment(attachment_filename)
            message.attach(attachment)
        
        try:
            response = client.send_raw_email(
                Source=sender_address,
                Destinations=[r for r in recipient_list],
                RawMessage={
                    'Data':message.as_string(),
                },
                ConfigurationSetName=CONFIGURATION_SET
            )
            return response['MessageId']
        except ClientError as e:        
            raise e # or return an error code
