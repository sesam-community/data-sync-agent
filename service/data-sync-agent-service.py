import logging
import os
import sys
import json
import requests
from urllib.parse import urljoin
from time import sleep
import sesamclient

logger = None

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
                "url_pattern": slave_node["endpoint"],
                "verify_ssl": True,
                "jwt_token": slave_node["api_token"],
                "authentication": "jwt",
                "connect_timeout": 60,
                "read_timeout": 7200
        }

        if "api_connection" not in slave_node:
            slave_node["api_connection"] = sesamclient.Connection(sesamapi_base_url=slave_node["endpoint"] + "/api",
                                                                  jwt_auth_token=slave_node["api_token"])

        assert_slave_system(master_node, system_config)


def get_slave_node_datasets(slave_node):
    """
    Get the datasets we want to sync from the slave by reading its effective config
    :return:
    """

    old_datasets = slave_node.get("datasets", [])[:]

    all_source_datasets = []
    all_sink_datasets = []
    for pipe in slave_node["api_connection"].get_pipes():
        source = pipe.config["effective"].get("source")
        sink = pipe.config["effective"].get("sink")

        sink_datasets = sink.get("datasets", sink.get("dataset"))
        if sink_datasets:
            if not isinstance(sink_datasets, list):
                all_sink_datasets.append(sink_datasets)
            else:
                all_sink_datasets.extend(sink_datasets)

        source_datasets = source.get("datasets", source.get("dataset"))
        if source_datasets:
            if not isinstance(source_datasets, list):
                source_datasets = [source_datasets]

            # Clean datasets, in case there are "merge" ones
            if source.get("type") == "merge":
                source_datasets = [ds.rpartition(" ")[0] for ds in source_datasets if ds]

            all_source_datasets.extend(source_datasets)

    all_sink_datasets = set(all_sink_datasets)
    all_source_datasets = set(all_source_datasets)

    # These datasets should exist as pipes in the master
    slave_node["datasets"] = all_sink_datasets.difference(all_source_datasets)

    # These datasets used to exist but don't anymore, we must remove the associated pipes from master
    slave_node["datasets_to_delete"] = set(old_datasets).difference(slave_node["datasets"])


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

    for slave_node in slave_nodes:
        logger.info("Processing slave %s" % slave_node["_id"])

        # Delete pipes whose dataset used to exist in the slave but has been deleted since the last time we checked
        for dataset in slave_node.get("datasets_to_delete", []):
            # Check if it exists first, don't try to delete non-existing datasets
            pipe_id = "%s-from-slave-%s" % (dataset, slave_node["_id"])
            if master_node["api_conection"].get_pipe(pipe_id):
                logger.info("Removing pipe '%s' from master because the dataset "
                            "in the slave has been removed" % pipe_id)
                master_node["api_conection"].delete_pipe(pipe_id)

        # If the pipe doesn't exist, add it
        for dataset in slave_node.get("datasets", []):
            pipe_id = "%s-from-slave-%s" % (dataset, slave_node["_id"])

            pipe_config = {
                "_id": pipe_id,
                "type": "pipe",
                "source": {
                    "type": "json",
                    "system": "slave-%s" % slave_node["_id"],
                    "url": "datasets/%s" % dataset,
                    "supports_since": True,
                    "is_chronological": True
                },
                "sink": {
                    "type": "dataset",
                    "dataset": dataset
                }
            }

            pipe = master_node["api_connection"].get_pipe(pipe_id)
            if pipe is not None:
                # The pipe exists, so update it (in case someone has modified it)
                pipe.modify(pipe_config)
                logger.info("Modyfying existing pipe '%s' in the master" % pipe_id)
            else:
                # New pipe - post it
                master_node["api_connection"].add_pipes(pipe_id, [pipe_config])
                logger.info("Adding new sync pipe '%s' to the master" % pipe_id)


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
        # Get datasets we want to sync
        get_slave_datasets(slave_nodes)

        # Create, delete or update the pipes
        assert_sync_pipes(master_node, slave_nodes)

        # Sleep for a while then go again
        sleep(5*60)
