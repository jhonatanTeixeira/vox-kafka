import importlib
import json
from concurrent.futures.thread import ThreadPoolExecutor
from logging import getLogger
from threading import Thread

from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer, KafkaClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.producer.future import FutureRecordMetadata

logger = getLogger('kafka-client')

KAFKA = {}

if importlib.find_loader('django.conf'):
    from django.conf import settings

    KAFKA = settings.KAFKA
else:
    logger.debug('Not a django environment')

created_topics = []


def reset_created_topics_cache():
    created_topics.clear()


class KafkaManager:
    def __init__(self, serializer=lambda v: json.dumps(v).encode('utf-8'),
                 deserializer=lambda m: json.loads(m.decode('utf-8')), **configs):
        self._admin = None
        self._producer = None
        self.serializer = serializer
        self.deserializer = deserializer
        self.configs = {**KAFKA, **configs}

        if 'consumers' in self.configs:
            del self.configs['consumers']

    @property
    def admin(self) -> KafkaAdminClient:
        if self._admin is None:
            self._admin = KafkaAdminClient(**self.configs)

        return self._admin

    def create_client(self) -> KafkaClient:
        return KafkaClient(**self.configs)

    def create_producer(self, **kafka_configs):
        return KafkaProducer(value_serializer=self.serializer, **{**self.configs, **kafka_configs})

    @property
    def producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(value_serializer=self.serializer, **self.configs)

        return self._producer

    def create_consumer(self, topic, group_id, **kafka_configs):
        if 'value_deserializer' not in kafka_configs:
            kafka_configs['value_deserializer'] = self.deserializer

        return KafkaConsumer(topic, group_id=group_id, **{**self.configs, **kafka_configs})


kafka_manager = KafkaManager()


def create_kafka_topic(topic, num_partitions=1, replication_factor=1):
    try:
        if topic not in created_topics:
            created_topics.append(topic)
            kafka_manager.admin.create_topics([NewTopic(topic, num_partitions, replication_factor)])
    except TopicAlreadyExistsError:
        logger.debug(f'topic {topic} already exists')
        pass


def send_kafka_message_async(topic, payload, partition=None, create_topic=True, num_partitions=1, replication_factor=1,
                             callback=None, err_callback=None) -> FutureRecordMetadata:
    if create_topic:
        create_kafka_topic(topic, num_partitions, replication_factor)

    future = kafka_manager.producer.send(topic, value=payload, partition=partition)

    def on_error(exception):
        logger.debug(str(exception), stack_info=True)

        if err_callback is not None:
            err_callback(exception)

    if callback is not None:
        future.add_callback(callback, payload)

    future.add_errback(on_error, payload)

    return future


def send_kafka_message(topic, payload, partition=None, create_topic=True, num_partitions=1, replication_factor=1):
    return send_kafka_message_async(topic, payload, partition, create_topic, num_partitions, replication_factor)\
        .get(timeout=60)


def receive_kafka_messages(topic, group_id='internal_consumer', create_topic=True, num_partitions=1,
                           replication_factor=1, **kafka_configs):
    if create_topic:
        logger.debug(f'Creating topic {topic}')
        create_kafka_topic(topic, num_partitions, replication_factor)

    return kafka_manager.create_consumer(topic, group_id, **kafka_configs)


def consume_kafka_messages(topic, callback, group_id='internal_consumer', create_topic=True,
                           num_partitions=1, replication_factor=1, **kafka_configs):
    def on_consume(payload):
        logger.debug(f'Consuming message for topic {topic}', extra=payload)
        callback(payload)

    [on_consume(payload) for payload in receive_kafka_messages(topic, group_id, create_topic, num_partitions,
                                                               replication_factor, **kafka_configs)]


def consume_kafka_messages_async(topic, callback, group_id='internal_consumer', max_threads=10, create_topic=True,
                                 num_partitions=1, replication_factor=1, **kafka_configs):
    def on_consume(payload):
        logger.debug(f'Consuming message for topic {topic}', extra=payload)
        callback(payload)

    executor = ThreadPoolExecutor(max_workers=max_threads)
    executor.map(on_consume, receive_kafka_messages(topic, group_id, create_topic, num_partitions, replication_factor,
                                                    **kafka_configs))


class KafkaConsumerThread(Thread):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.args = args
        self.kwargs = kwargs

    def run(self):
        consume_kafka_messages(*self.args, **self.kwargs)


class KafkaConsumerThreadManager:
    def __init__(self):
        self.threads = []

    def is_healthy(self):
        for thread in self.threads:
            if not thread.is_alive():
                return False

        return True

    def add_thread(self, thread: KafkaConsumerThread):
        self.threads.append(thread)

    def init_threads(self):
        for thread in self.threads:
            if not thread.is_alive():
                thread.start()


kafka_thread_manager = KafkaConsumerThreadManager()


def kafka_listener(topic, **kafka_configs):
    def decorator(listener):
        if 'consumers' in KAFKA:
            options = {}

            if topic in KAFKA['consumers']:
                options = KAFKA['consumers'][topic]

        thread = KafkaConsumerThread(topic=topic, callback=listener, **{**options, **kafka_configs})
        thread.daemon = True
        kafka_thread_manager.add_thread(thread)
        kafka_thread_manager.init_threads()

    return decorator
