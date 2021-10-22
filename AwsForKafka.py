import json
import datetime
import boto3
from kafka import KafkaProducer

TOPIC_NAME = 'TOPICO_TESTE'
SERVER = 'localhost:9092'
S3_BUCKET_SOURCE = 'bucketentrada'
S3_BUCKET_NAME_FILE = ''
# ----

AUTO_OFFSET_RESET = 'earliest'
S3_SERVICE = 's3'
JSON = '.json'
ARVRO = '.avro'


class Aws:
    def __init__(self, service_name):
        self.service = boto3.resource(service_name)
        self.S3 = S3(self.service)

    def get_service(self):
        return self.service


class S3:
    def __init__(self, service):
        self.service = service

    def get_all_buckets(self):
        return self.service.buckets.all()

    def get_data_object_bucket_by_name(self, bucket_name, key):
        data = self.service.Object(bucket_name, key)
        return data.get()['Body'].read().decode('utf-8')

    def obj_last_modified(myobj):
        return myobj.last_modified

    def get_last_file_name_by_bucket(self, bucket_name):
        def obj_last_modified(myobj):
            return myobj.last_modified

        file_name = ''
        my_bucket = self.service.Bucket(bucket_name)
        file_last = sorted(my_bucket.objects.all(), key=obj_last_modified, reverse=True)[0]

        if (JSON in file_last.key) or (ARVRO in file_last.key):
            file_name = file_last.key

        return file_name


class KafkaProd:
    def __init__(self, bootstrap_servers, value_serializer):
        self.bootstrap_servers = bootstrap_servers
        self.value_serializer = value_serializer

    def send_message(self, topic, message):
        kafka_prod = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=self.value_serializer
        )
        kafka_prod.send(topic, message)


def serializer(message):
    return json.dumps(message).encode('utf-8')


try:
    aws_consumer = Aws(S3_SERVICE)
    last_file = aws_consumer.S3.get_last_file_name_by_bucket(S3_BUCKET_SOURCE)

    if last_file == '':
        print(f'No Files To Process. = {str(TOPIC_NAME)} @ {datetime.datetime.now()}')
    else:
        json_input = aws_consumer.S3.get_data_object_bucket_by_name(S3_BUCKET_SOURCE, last_file)
        producer = KafkaProd(SERVER, serializer)
        print(f'Sending Message To Topic. = {str(TOPIC_NAME)} @ {datetime.datetime.now()} | Message = {str(json_input)}')
        producer.send_message(TOPIC_NAME, json_input)
        print(f'Message Sent Successfully To Topic. = {str(TOPIC_NAME)} @ {datetime.datetime.now()}')

except Exception as e:
    print(e)
