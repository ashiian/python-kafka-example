from producer import produce
from consumer import listen


def handleResponse(topic, key, value, partition, offset):
  print('key: ' + key)
  print('value: ' + value)


produce(topic='requests', key='123123123', value='https://www.google.com')
listen(topics=['responses'], callback=handleResponse)