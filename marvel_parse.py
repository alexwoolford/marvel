# import bs4.element
from confluent_kafka import Consumer, Producer, KafkaException
import ccloud_lib
import json
import sys
from bs4 import BeautifulSoup
import os
from neo4j import GraphDatabase

# to reset offsets:
# kafka-consumer-groups.sh --bootstrap-server pkc-lzvrd.us-west4.gcp.confluent.cloud:9092
# --command-config config.properties --group marvel_parse --topic marvel-html --reset-offsets
# --to-earliest --execute

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE")


def parse_html(html):
    character_properties = dict()
    try:
        soup = BeautifulSoup(html)
        character_name = soup.find("span", {"class": "masthead__headline"}).text
        if character_name is not None:
            character_properties['name'] = character_name.strip()
        # bio_rail = soup.find("div", {"class": "railExploreBio"})
        # if isinstance(bio_rail, bs4.element.Tag):
        #     labels = bio_rail.find_all("p", {"class", "bioheader__label"})
        #     for label in labels:
        #         bio_key = label.text
        #         bio_value = label.nextSibling.text
        #         character_properties[bio_key] = bio_value
        related_character_links = list()
        related_group_links = list()
        all_links = soup.find_all("a", href=True)
        for link in all_links:
            if link['href'].startswith("/characters/"):
                related_character_links.append("https://www.marvel.com" + link.get('href'))
            if link['href'].startswith("/teams-and-groups/"):
                related_group_links.append("https://www.marvel.com" + link.get('href'))
        character_properties["related_character_links"] = related_character_links
        character_properties["related_group_links"] = related_group_links

    except Exception as err:
        sys.stderr.write(str(err) + "\n")

    return character_properties


if __name__ == "__main__":

    # Neo4j DB connection
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session(database=NEO4J_DATABASE) as session:

        conf = ccloud_lib.read_ccloud_config('/Users/alexwoolford/.confluent/python.config')
        conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
        conf['group.id'] = "marvel_parse"

        consumer = Consumer(conf)
        producer = Producer(conf)

        consumer.subscribe(["marvel-html"])

        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    html_record = json.loads(msg.value())
                    url = html_record.get("url")
                    html = html_record.get("html")
                    character = parse_html(html)
                    character["url"] = url
                    if "related_character_links" in character.keys():
                        for related_character_link in character['related_character_links']:
                            session.run("""
                                        MERGE(c:Character {url: $url})
                                        SET c.name = $name
                                        MERGE(r:Character {url: $related_character_link})
                                        MERGE(c)-[:REFERENCES]->(r)
                                        """,
                                        url=url,
                                        name=character["name"],
                                        related_character_link=related_character_link)
                    if "related_group_links" in character.keys():
                        for related_character_link in character['related_group_links']:
                            session.run("""
                                        MERGE(g:Group {url: $group_url})
                                        MERGE(c:Character {url: $url})
                                        MERGE(c)-[:REFERENCES]->(g)
                                        """,
                                        group_url=related_character_link,
                                        url=url)

        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')

        finally:
            # Close down consumer to commit final offsets.
            consumer.close()
