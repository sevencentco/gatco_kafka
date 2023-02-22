import asyncio
import confluent_kafka
from confluent_kafka import KafkaException

from time import time
from threading import Thread

__version__ = '0.1.0'

class AIOProducer:
    def __init__(self, configs, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._producer = confluent_kafka.Producer(configs)
        self._cancelled = False
        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()

    def _poll_loop(self):
        while not self._cancelled:
            self._producer.poll(0.1)

    def close(self):
        self._cancelled = True
        self._poll_thread.join()

    def produce(self, topic, value):
        """
        An awaitable produce method.
        """
        result = self._loop.create_future()

        def ack(err, msg):
            if err:
                self._loop.call_soon_threadsafe(result.set_exception, KafkaException(err))
            else:
                self._loop.call_soon_threadsafe(result.set_result, msg)
        self._producer.produce(topic, value, on_delivery=ack)
        return result

    def produce2(self, topic, value, on_delivery):
        """
        A produce method in which delivery notifications are made available
        via both the returned future and on_delivery callback (if specified).
        """
        result = self._loop.create_future()

        def ack(err, msg):
            if err:
                self._loop.call_soon_threadsafe(
                    result.set_exception, KafkaException(err))
            else:
                self._loop.call_soon_threadsafe(
                    result.set_result, msg)
            if on_delivery:
                self._loop.call_soon_threadsafe(
                    on_delivery, err, msg)
        self._producer.produce(topic, value, on_delivery=ack)
        return result


class Producer:
    def __init__(self, configs):
        self._producer = confluent_kafka.Producer(configs)
        self._cancelled = False
        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()

    def _poll_loop(self):
        while not self._cancelled:
            self._producer.poll(0.1)

    def close(self):
        self._cancelled = True
        self._poll_thread.join()

    def produce(self, topic, value, on_delivery=None):
        self._producer.produce(topic, value, on_delivery=on_delivery)

class _KafkaState(object):
    """Remembers configuration for the (db, app) tuple."""

    def __init__(self, kafka):
        self.kafka = kafka

class Kafka(object):
    app = None
    producer = None
    config = {}
    
    def __init__(self, app=None):
        if app is not None:
            self.init_app(app)

    def init_app(self, app, config=None):
        self.app = app
        if config is None:
            self.config = {
                "bootstrap.servers": app.config.get("KAFKA_BOOTSTRAP_SERVERS")
            }
        else:
            self.config = config
        
        @app.listener('after_server_start')
        async def notify_server_started(app, loop):
            self.producer = AIOProducer(self.config, loop=loop)
            print('Server successfully started!')

        @app.listener('before_server_stop')
        async def notify_server_stopping(app, loop):
            self.producer.close()
            print('Server shutting down!')
        
        
        if (not hasattr(app, 'extensions')) or (app.extensions is None):
            app.extensions = {}
        app.extensions['kafka'] = _KafkaState(self)
    

    def get_app(self, reference_app=None):
        """Helper method that implements the logic to look up an
        application."""

        if reference_app is not None:
            return reference_app

        if self.app is not None:
            return self.app

        raise RuntimeError(
            'No application found. Either work inside a view function or push'
            ' an application context.'
        )