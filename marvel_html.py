from confluent_kafka import Consumer, Producer, KafkaException
import ccloud_lib
import sys
import json
import os
import requests

# to reset offsets:
# kafka-consumer-groups.sh --bootstrap-server pkc-lzvrd.us-west4.gcp.confluent.cloud:9092
# --command-config config.properties --group marvel_html --topic marvel-url --reset-offsets
# --to-earliest --execute

SCRAPESTACK_API_KEY = os.getenv("SCRAPESTACK_API_KEY")


def get_html(url):
    params = {'access_key': SCRAPESTACK_API_KEY, 'url': url, 'render_js': 1}
    scrapestack_url = 'http://api.scrapestack.com/scrape'
    response = requests.get(scrapestack_url, params=params)
    return response.content.decode("utf-8")


if __name__ == "__main__":
    conf = ccloud_lib.read_ccloud_config('/Users/alexwoolford/.confluent/python.config')
    conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    conf['group.id'] = 'marvel_html'

    consumer = Consumer(conf)
    producer = Producer(conf)

    consumer.subscribe(['marvel-url'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                url_record = json.loads(msg.value())
                url = url_record.get('url')
                html = get_html(url)
                url_record['html'] = html
                producer.produce('marvel-html', json.dumps(url_record), url)
                consumer.commit()

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
