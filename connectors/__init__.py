"""Connector seam: where real middleware connectors plug in.

The demo ships with a simulated estate (see sim/). A production deployment
replaces the simulation with real connectors implementing the framework in
connectors.base. Importing this package registers every connector in
``CONNECTORS`` (name -> (ConnectorSpec, factory)).
"""

from .base import (
    CONNECTORS,
    Connector,
    ConnectorError,
    ConnectorSpec,
    register_connector,
    resolve_secret,
)
from . import ibmmq, kafka, linux_host, tomcat
from . import (
    apache,
    artemis,
    docker,
    elasticsearch,
    haproxy,
    jboss,
    kubernetes,
    mongodb,
    mysql,
    nginx,
    oracle_db,
    postgres,
    rabbitmq,
    redis,
    tibco_ems,
    weblogic,
    websphere,
)
from .ibmmq import IBMQConnector
from .kafka import KafkaConnector
from .linux_host import LinuxHostConnector
from .tomcat import TomcatConnector

__all__ = [
    "CONNECTORS",
    "Connector",
    "ConnectorError",
    "ConnectorSpec",
    "IBMQConnector",
    "KafkaConnector",
    "LinuxHostConnector",
    "TomcatConnector",
    "apache",
    "artemis",
    "docker",
    "elasticsearch",
    "haproxy",
    "ibmmq",
    "jboss",
    "kafka",
    "kubernetes",
    "linux_host",
    "mongodb",
    "mysql",
    "nginx",
    "oracle_db",
    "postgres",
    "rabbitmq",
    "redis",
    "tibco_ems",
    "tomcat",
    "weblogic",
    "websphere",
    "register_connector",
    "resolve_secret",
]
