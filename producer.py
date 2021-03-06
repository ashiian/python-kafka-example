import sys
import os

from confluent_kafka import Producer


topic = '4o4fqf1b-requests'

# Consumer configuration
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
conf = {
    'bootstrap.servers':'rocket-01.srvs.cloudkafka.com:9094,'+\
                         'rocket-02.srvs.cloudkafka.com:9094,'+\
                         'rocket-03.srvs.cloudkafka.com:9094',
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'},
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-256',
    'sasl.username': '4o4fqf1b',
    'sasl.password': 'CW2XXq3FmmmEqAN2fwHGYFbZ18qinEK4'
}

p = Producer(**conf)

def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d]\n' %
                          (msg.topic(), msg.partition()))

def produce(topic, key, value):
    try:
        p.produce(topic, key=key, value=value, callback=delivery_callback)
    except BufferError as e:
        sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                          len(p))
    p.poll(0)

