import sys
import os

from confluent_kafka import Consumer, KafkaException, KafkaError



# Consumer configuration
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
conf = {
    'bootstrap.servers':'rocket-01.srvs.cloudkafka.com:9094,'+\
                         'rocket-02.srvs.cloudkafka.com:9094,'+\
                         'rocket-03.srvs.cloudkafka.com:9094',
    'group.id': '4o4fqf1b-consumers',
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'},
    'security.protocol': 'SASL_SSL',
'sasl.mechanisms': 'SCRAM-SHA-256',
    'sasl.username': '4o4fqf1b',
    'sasl.password': 'CW2XXq3FmmmEqAN2fwHGYFbZ18qinEK4'
}

c = Consumer(**conf)

def listen(topics, callback):
    c.subscribe(topics)
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # Error or event
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                      (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    # Error
                    raise KafkaException(msg.error())
            else:
                (topic, key, value, partition, offset) = (msg.topic(), msg.key(), msg.value(), msg.partition(), msg.offset())
                callback(topic, key, value, partition, offset)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    # Close down consumer to commit final offsets.
    c.close()
