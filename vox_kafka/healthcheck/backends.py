from logging import getLogger

from health_check.backends import BaseHealthCheckBackend
from health_check.exceptions import HealthCheckException

from vox_kafka.kafka_client import kafka_manager, kafka_thread_manager

logger = getLogger('healthcheck')


class KafkaHealthCheck(BaseHealthCheckBackend):
    critical_service = True

    def check_status(self):
        try:
            kafka_manager.create_client().bootstrap_connected()
        except Exception as e:
            logger.error(str(e), stack_info=True, exc_info=True)
            raise HealthCheckException(str(e))

        if not kafka_thread_manager.is_healthy():
            logger.error('Kafka threads error', stack_info=True, exc_info=True)
            raise HealthCheckException('kafka consumer threads not ok')

    def identifier(self):
        return 'Kafka'

