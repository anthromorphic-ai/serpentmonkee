# _METADATA_:Version: 20
# _METADATA_:Timestamp: 2021-01-17 21:26:27.467968+00:00
# _METADATA_:MD5: d2943166f73cfa9cdd09868079cd3808
# _METADATA_:Publish:                                                                       None


# _METADATA_:
import logging
from datetime import datetime, timedelta, timezone
from neo4j import GraphDatabase, basic_auth, __version__ as neoVersion
from neo4j.exceptions import ServiceUnavailable
import uuid

from serpentmonkee import UtilsMonkee as um
from CypherTransaction import CypherTransactionBlock, CypherTransactionBlockWorker
from CypherQueue import CypherQueue, CypherQueues
import redis

from PubSubMonkee import PubSubMonkee


class sNeoDriver:  # --------------------------------------------------------------------
    def __init__(self, neoDriver, redisClient, publisher, projectId, topicId, sqlClient=None, callingCF=None):
        self.neoDriver = neoDriver
        self.driverUuid = None
        self.driverStartedAt = None
        self.sqlClient = sqlClient
        self.callingCF = callingCF
        self.redisClient = redisClient
        self.db_fb = None
        self.cypherQueues = self.makeCypherQueues()
        self.asyncStatements = []
        self.pubsub = PubSubMonkee(publisher, projectId, topicId)

    def makeCypherQueues(self):
        cQH = CypherQueue("cypherQ_High")
        cQM = CypherQueue("cypherQ_Medium")
        cQL = CypherQueue("cypherQ_Low")
        wQ = CypherQueue("cypherWorking")
        compQ = CypherQueue("cypherDone")
        queues = [cQH, cQM, cQL]
        return CypherQueues(redisClient=self.redisClient,
                            cQueues=queues, workingQ=wQ, completedQ=compQ, fb_db=self.db_fb)

    def get_uuid(self):
        return str(uuid.uuid4())

    def makeNeoDriver(self, neo_uri, neo_user, neo_pass):
        if neo_uri is not None:
            self.driverUuid = self.get_uuid()
            self.driverStartedAt = datetime.now(timezone.utc)
            if neoVersion[0] == '4':
                self.neoDriver = GraphDatabase.driver(
                    uri=neo_uri,
                    auth=basic_auth(
                        user=neo_user,
                        password=neo_pass,
                    ),
                    max_transaction_retry_time=2
                    # max_connection_lifetime=200,
                    # encrypted=True,
                )
            if neoVersion[0] == '1':
                self.neoDriver = GraphDatabase.driver(
                    uri=neo_uri,
                    auth=basic_auth(
                        user=neo_user,
                        password=neo_pass,
                    ),
                    # max_connection_lifetime=200,
                    encrypted=True,
                    max_retry_time=2)
            self.cypherWorker = CypherTransactionBlockWorker(
                self.neoDriver, self.cypherQueues, sqlClient=self.sqlClient, pubsub=self.pubsub)
