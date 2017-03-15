============================
Data sync agent microservice
============================

Microservice that consumes config from slave nodes and ensures their datasets are synced to a master node

System configuration in Sesam:

::

    {
        "_id": "data-sync-microservice",
        "name": "Name of data sync microservice",
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
                    "jwt_token" : "fklrl464nimsnfskfklrl464nimskfklrl464nimsnfskfkfklrl464nimsnfskf4nimsnfskfklrl464n",
                },
                "SLAVE_NODES": [
                    {
                        "_id" : "s1",
                        "endpoint" : "https://s1.sesam.cloud",
                        "jwt_token" : "msnfskfklrl464nimsnfskfklrl464nimsnfskfklrl464nimsnfskfklrl44nimsnfskfklrl464ni",
                        "sync_interval": 300
                    },
                    {
                        "_id" : "s2",
                        "endpoint" : "https://s2.sesam.cloud",
                        "jwt_token" : "msnfskfklrl464nimsnfskfklrl464nimsnfskfklrl464nimsnfskfklrl464nimsnfskfklrl464n"
                    }
                ]
            }
        }
    }

