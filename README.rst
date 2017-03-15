===============
data-sync-agent
===============

Microservice that consumes config from slave nodes and ensures their datasets are synced to a master node

::

  $ export MASTER_NODE="{..}"
  $ export SLAVE_NODES="[..]"
  $ python3 service/data-sync-agent-service.py

Configuration in Sesam:

::

    {
        "_id": "data-sync-microservice",
        "name": "Name of microservice",
        "type": "system:microservice",
        "docker": {
            "image": "sesam/data-sync-agent:latest",
            "port": 5000,
            "memory": 128,
            "environment": {
                "OVERWRITE_MASTER_SYSTEMS": "false",
                "OVERWRITE_MASTER_PIPES": "false",
                "DELETE_MASTER_PIPES": "true",
                "UPDATE_INTERVAL": "300",
                "MASTER_NODE": {
                    "_id" : "m1",
                    "endpoint" : "https://m1.sesam.cloud",
                    "jwt_token" : "msnfskfksni",
                },
                "SLAVE_NODES": [
                    {
                        "_id" : "s1",
                        "endpoint" : "https://s1.sesam.cloud",
                        "jwt_token" : "msnfskfklrl464ni",
                        "sync_interval": 300
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
