from health_check.backends import BaseHealthCheckBackend
from health_check.exceptions import HealthCheckException

from vox_kafka.kafka_client import kafka_manager, kafka_thread_manager


class KafkaHealthCheck(BaseHealthCheckBackend):
    critical_service = True

    def check_status(self):
        try:
            kafka_manager.create_client().bootstrap_connected()
        except Exception as e:
            raise HealthCheckException(str(e))

        if not kafka_thread_manager.is_healthy():
            raise HealthCheckException('kafka consumer threads not ok')

    def identifier(self):
        return 'Kafka'

