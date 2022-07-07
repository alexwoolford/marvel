import json
from confluent_kafka import Producer
import ccloud_lib


def publish_urls():

    conf = ccloud_lib.read_ccloud_config('/Users/alexwoolford/.confluent/python.config')
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)

    character_urls_filehandle = open("character_urls.txt", "r")
    for line in character_urls_filehandle.readlines():
        url = line.strip()
        url_json = json.dumps({'url': url})
        producer.produce('marvel-url', url_json, url)
        producer.flush()


if __name__ == "__main__":
    publish_urls()
