from time import sleep
from unittest import TestCase
from unittest.mock import patch, call, MagicMock

from django.conf import settings
from health_check.exceptions import HealthCheckException

settings.configure(
    DEBUG=True,
    KAFKA={
        'bootstrap_servers': 'kafka',
        'consumers': {
            'foo': {
                'group_id': 'some-group',
                'max_poll_records': 1,
                'create_topic': False,
            },
            'bar': {
                'group_id': 'other-group',
                'max_poll_records': 10,
                'create_topic': False,
            },
        }
    },
    KAFKA_REPLICAS=[
        {
            'bootstrap_servers': 'kafka2',
        }
    ],
    DATABASES={'default': {
        'ENGINE': 'sqlite'
    }},
    APP_NAME='foo',
    APP_VERSION='1.0',
    HEALTH_CHECK_PUGINS=[
        'database',
        'kafka',
    ],
    INSTALLED_APPS=[
        'vox_kafka',
    ],
    ORGANIZATION='riachuelo',
)


class TestMessages(TestCase):
    @patch('vox_kafka.kafka_client.KafkaAdminClient')
    @patch('vox_kafka.kafka_client.KafkaProducer')
    @patch('vox_kafka.kafka_client.NewTopic')
    def test_should_dispatch_message(self, NewTopicMock, KafkaProducerMock, KafkaAdminClientMock):
        from vox_kafka import kafka_client

        kafka_client.send_kafka_message('some-topic', {'foo': 'bar'})
        KafkaAdminClientMock().create_topics.assert_called_with([NewTopicMock('some-topic', 1, 1)])
        KafkaProducerMock().send.assert_called_with('some-topic', value={'foo': 'bar'}, partition=None)

        future_mock = MagicMock()
        KafkaProducerMock().send.return_value = future_mock
        kafka_client.send_kafka_message_async('some-topic', {'foo': 'bar'}, callback=lambda a: a,
                                              err_callback=lambda b: b)
        future_mock.add_callback.assert_called()
        future_mock.add_errback.assert_called()


class TestHealthCheckBackends(TestCase):
    @patch('vox_kafka.healthcheck.backends.kafka_manager')
    def test_should_healthcheck_kafka(self, kafka_manager):
        from vox_kafka.healthcheck.backends import KafkaHealthCheck

        KafkaHealthCheck().check_status()
        kafka_manager.create_client().bootstrap_connected.assert_called()

        kafka_manager.create_client().bootstrap_connected.side_effect = Exception('error')

        with self.assertRaises(HealthCheckException):
            KafkaHealthCheck().check_status()

    @patch('vox_kafka.healthcheck.backends.kafka_manager')
    @patch('vox_kafka.kafka_client.KafkaConsumerThread')
    def test_should_check_consumer_threads_health(self, KafkaConsumerThread, kafka_manager):
        from vox_kafka.kafka_client import kafka_listener, kafka_thread_manager
        from vox_kafka.healthcheck.backends import KafkaHealthCheck

        @kafka_listener('foo')
        def listener():
            sleep(0.5)

        kafka_manager.admin.list_consumer_groups.return_value = ['foo', 'bar']
        KafkaConsumerThread().is_alive.return_value = False

        with self.assertRaises(HealthCheckException):
            KafkaHealthCheck().check_status()

        kafka_thread_manager.threads = []


class TestKafkaClient(TestCase):

    @patch('vox_kafka.kafka_client.KafkaAdminClient')
    @patch('vox_kafka.kafka_client.KafkaProducer')
    @patch('vox_kafka.kafka_client.KafkaConsumer')
    def test_kafka_manager(self, KafkaConsumer, KafkaProducer, KafkaAdminClient):
        from vox_kafka.kafka_client import KafkaManager
        manager = KafkaManager()

        admin = manager.admin
        producer = manager.producer
        consumer = manager.create_consumer('foo', 'bar')

        KafkaAdminClient.assert_called_with(bootstrap_servers='kafka')
        KafkaProducer.assert_called()
        KafkaConsumer.assert_called()

    @patch('vox_kafka.kafka_client.kafka_manager')
    @patch('vox_kafka.kafka_client.NewTopic')
    def test_should_send_message(self, NewTopic, kafka_manager):
        from vox_kafka.kafka_client import send_kafka_message, reset_created_topics_cache

        reset_created_topics_cache()

        topic = 'foo'
        num_partitions = 1
        replication_factor = 1

        send_kafka_message('foo', True, num_partitions, replication_factor)

        kafka_manager.admin.create_topics.assert_called()
        NewTopic.assert_called_with(topic, num_partitions, replication_factor)

    @patch('vox_kafka.kafka_client.kafka_manager')
    @patch('vox_kafka.kafka_client.NewTopic')
    @patch('vox_kafka.kafka_client.KafkaConsumer')
    def test_should_consume_messages(self, KafkaConsumer, NewTopic, kafka_manager):
        from vox_kafka.kafka_client import consume_kafka_messages
        topic = 'foo'
        group = 'bar'
        num_partitions = 3
        replication_factor = 3

        values = [
            {'id': 1},
            {'id': 2},
        ]

        KafkaConsumer.return_value = values
        callback = lambda message: self.assertTrue(message in values)

        consume_kafka_messages(topic, callback, group, True, num_partitions, replication_factor)

        kafka_manager.admin.create_topics.assert_called()
        NewTopic.assert_called_with(topic, num_partitions, replication_factor)


class KafkaThreadTest(TestCase):

    @patch('vox_kafka.kafka_client.KafkaConsumer')
    def test_should_run_thread(self, KafkaConsumer):
        from vox_kafka.kafka_client import KafkaConsumerThread
        KafkaConsumer.return_value = [{'id': 1}]

        callback = MagicMock()
        thread = KafkaConsumerThread(topic='foo', group_id='group1', callback=callback, max_poll_records=1,
                                     create_topic=False, value_deserializer=callback)
        thread.run()

        KafkaConsumer.assert_called_with('foo', bootstrap_servers='kafka', group_id='group1',
                                         max_poll_records=1, value_deserializer=callback)
        callback.assert_called()

    @patch('vox_kafka.kafka_client.receive_kafka_messages')
    def test_should_listen_consumer(self, receive_kafka_messages):
        from vox_kafka.kafka_client import kafka_listener, kafka_thread_manager

        effects = [
            [
                {'id': 1},
                {'id': 2},
            ],
            [
                {'id': 3},
                {'id': 4},
            ],
            [
                {'id': 5},
                {'id': 6},
            ],
        ]

        receive_kafka_messages.side_effect = effects

        @kafka_listener('foo')
        def listener_one(payload):
            self.assertTrue(payload in effects[0])
            sleep(0.5)

        @kafka_listener('bar')
        def listener_two(payload):
            self.assertTrue(payload in effects[1])
            sleep(0.5)

        @kafka_listener('bar', group_id='extra-group')
        def listener_three(payload):
            self.assertTrue(payload in effects[2])
            sleep(0.5)

        for thread in kafka_thread_manager.threads:
            thread.join()

        receive_kafka_messages.assert_has_calls([
            call('foo', 'some-group', False, 1, 1, max_poll_records=1),
            call('bar', 'other-group', False, 1, 1, max_poll_records=10),
            call('bar', 'extra-group', False, 1, 1, max_poll_records=10),
        ])

        kafka_thread_manager.threads = []
