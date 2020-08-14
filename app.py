from producer import produce
from consumer import listen

print('lal')

def handleResponse(topic, key, value, partition, offset):
    print('key: ' + key.decode("utf-8"))
    print('value: ' + value.decode("utf-8"))


produce(topic='4o4fqf1b-requests-files', key='123123123', value='https://classifai.net/blog/content/images/2019/11/Screen-Shot-2019-11-09-at-5.40.41-PM2.png')
listen(topics=['4o4fqf1b-responses-files'], callback=handleResponse)
