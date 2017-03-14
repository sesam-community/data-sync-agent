import logging
import os
import sys
import json
import requests
from urllib.parse import urljoin
from time import sleep

"""
{
    "_id": "data-sync-microservice",
    "name": "Name of microservice",
    "type": "system:microservice",
    "docker": {
        "image": "sesam/data-sync-agent:latest",
        "port": 5000,
        "memory": 128,
        "environment": {
            "MASTER_NODE": {
                "_id" : "m1",
                "endpoint" : "https://m1.sesam.cloud",
                "jwt_token" : "msnfskfksni",
            },
            "SLAVE_NODES": [
                {
                    "_id" : "s1",
                    "endpoint" : "https://s1.sesam.cloud",
                    "jwt_token" : "msnfskfklrl464ni"
                },
                {
                    "_id" : "s2",
                    "endpoint" : "https://s2.sesam.cloud",
                    "jwt_token" : "msnfskfklrl464ni"
                }
            ]
        }
    },
    "use_https": false,
    "verify_ssl": false,
    "username": None,
    "password": None,
    "authentication": "basic",
    "connect_timeout": 60,
    "read_timeout": 7200
}
"""


def assert_slave_system(master_node, system_config):
    """
    Post the system config to the master node through the API
    :param master_node:
    :param system_config:
    :return:
    """

    master_api_url = urljoin(master_node["endpoint"], "/api/systems")
    headers = {
        "Authorization": "Bearer " + master_node["jwt_token"]
    }

    payload = json.dumps(system_config)

    # Check if the system already exists
    rd = requests.get(master_api_url + "/" + system_config["_id"], headers=headers)

    if rd.status_code == 404:
        # POST it
        rd = requests.post(master_api_url, headers=headers, data=payload)
    elif rd.status_code == 200:
        # PUT it
        rd = requests.put(master_api_url + "/" + system_config["_id"], headers=headers, data=payload)

    if rd.status_code != 200:
        raise AssertionError("Could not create or update system in master at URL %s: %s" % (master_api_url,
                                                                                           str(system_config)))

def assert_slave_systems(master_node, slave_nodes):

    for slave_node in slave_nodes:
        system_config = {
                "_id": "slave-%s" % slave_node["_id"],
                "name": slave_node["_id"],
                "type": "system:url",
                "url_pattern": slave_node["endpoint"]
                "verify_ssl": True,
                "jwt_token": slave_node["api-token"],
                "authentication": "jwt",
                "connect_timeout": 60,
                "read_timeout": 7200
        }

        assert_slave_system(master_node, system_config)

def get_slave_node_datasets(slave_node):
    """
    Get the datasets we want to sync from the slave by reading its effective config
    :return:
    """
    pass

def get_slave_datasets(slave_nodes):
    for slave_node in slave_nodes:
        get_slave_node_datasets(slave_node)


def assert_sync_pipes(master_node, slave_nodes):
    """
    Make sure all pipes that should exist does and all that refer to old datasets are deleted
    :param master_node:
    :param slave_nodes:
    :return:
    """

    # Get existing pipes for each slave and remove the ones that no longer exist in the slave and create/update the ones
    # that do
    pass

if __name__ == '__main__':
    format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger = logging.getLogger('data-sync-agent-service')

    # Log to stdout
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(stdout_handler)

    logger.setLevel(logging.DEBUG)

    # Read config from env vars

    if "MASTER_NODE" not in os.environ:
        logger.error("MASTER_NODE configuration missing!")
        sys.exit(1)

    master_node = json.loads(os.environ["MASTER_NODE"])

    if not "SLAVE_NODES" in os.environ:
        logger.error("SLAVE_NODES configuration missing!")
        sys.exit(1)

    slave_nodes = json.loads(os.environ["SLAVE_NODES"])

    # Setup phase - create systems for all slaves in the master

    assert_slave_systems(master_node, slave_nodes)

    while True:
        # Get datasets to sync
        get_slave_datasets(slave_nodes)

        # Crate or update the pipes
        assert_sync_pipes(master_node, slave_nodes)

        #sleep for a while
        sleep(5*60)
