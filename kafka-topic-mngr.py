#!/usr/bin/env python
#
# Author Simone Roselli <simone.roselli@plista.com>
#

import yaml, ast, sys
import subprocess

from kazoo.client import KazooClient

topic_name = 'test-topic4'
zk_conn = '192.168.99.100:32771'

kafka_bin_path = '/Users/zmo/Src/kafka/bin'
kafka_conf_path = '/etc/kafka/topics'
kafka_doc = 'http://kafka.apache.org/documentation.html#brokerconfigs'

# Load the YAML topic values as dictionary
try:
    with open(topic_name + '.yaml', 'r') as stream:
        topic_yaml_cnf = yaml.load(stream)
except IOError:
    print("ERROR: Missing \"%s" + '.yaml\"' + " file in %s") % (topic_name, kafka_conf_path)
    sys.exit(2)

class KafkaTopicMngr(object):     
    def __init__(self, yaml_cnf, topic, zk_conn):
        self.yaml_cnf = yaml_cnf
        self.topic = topic
        self.zk_conn = zk_conn
        # TODO: KafkaTopicCheck class.
        #self.set_yaml_cnf, self.set_live_cnf = set(yaml_cnf.keys()), set(live_cnf.keys())
        #self.intersect =self.set_yaml_cnf.intersection(self.set_live_cnf)

    def exists(self):
        """
        Check if the topic already exists
        """
        zk = KazooClient(hosts=self.zk_conn, read_only=True)
        zk.start()
        if zk.exists('/brokers/topics/' + self.topic) == None:
            zk.stop() 

            return False 

    def create(self):
        """
        Create the topic.
        Retrieve 'replication' and 'partitions' values from
        the YAML file
        """
        try:
            repl_factor = "%(replication)s" % self.yaml_cnf
            partitions  = "%(partitions)s" % self.yaml_cnf

            print("Creating new topic \"%s --replication %s --partitions %s\" ..") % (self.topic, repl_factor, partitions)
            subprocess.call([kafka_bin_path + "/kafka-topics.sh", 
                "--zookeeper", self.zk_conn, 
                "--create",
                "--replication", repl_factor,
                "--partitions", partitions,
                "--topic", self.topic
            ])

        except KeyError as e:
            print "Conf %s not found in %s" % (e, self.topic + '.yaml')
            sys.exit(2)

    @staticmethod
    def _valid_conf(conf, value, topic):
        """
        Ensure a given configuration is valid.
        NOTE: Currently the script 'kafka-topics.sh' doesn't handle exit statuses
        properly and it always returns 0. I'm forced to encapsulate stderr
        in this trivial way, trying to match an 'error' substr inside of it.
        This is enough at the moment to determine if a configuration is a valid one.
        """
        is_valid = subprocess.check_output([kafka_bin_path + "/kafka-topics.sh",
            "--zookeeper", zk_conn, 
            "--alter",
            "--config", "%s=%s" % (conf, value),
            "--topic", topic], stderr=subprocess.STDOUT
        )    

        if 'error' in is_valid.lower():
            return False

    def setup(self):
        """
        Config the topic with the values contained in the Yaml file.
        NOTE: Skip 'replication' and/or 'partition' conf here.  In the future we can
        better handle this, probably creating a deditated YAML files for the
        options.
        """
        for conf, value in self.yaml_cnf.items():
            if conf == 'replication' or conf == 'partitions':
                continue
            else:
                valid_conf = KafkaTopicMngr._valid_conf(conf, value, self.topic)
                null = open('/dev/null', 'w')
                show_configs = {}
                if valid_conf != False:
                    show_configs.update({conf:value})
                    subprocess.call([kafka_bin_path + "/kafka-topics.sh", 
                        "--zookeeper", self.zk_conn, 
                        "--alter",
                        "--config", "%s=%s" % (conf, value),
                        "--topic", self.topic], stdout=null
                    )    
                else:
                    print("ERROR: \"%s\" is not a valid configuration, please refer to \n \"%s\"") % (conf, kafka_doc)
                    sys.exit(2)

                print("Topic '%s' configured with %r") % (self.topic, show_configs)


# Interface
if __name__ == "__main__":
    mngr = KafkaTopicMngr(topic_yaml_cnf, topic_name, zk_conn)
    if mngr.exists() == False:
        mngr.create()
        mngr.setup()
    else:
        mngr.setup()
