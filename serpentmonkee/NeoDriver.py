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
import redis

import serpentmonkee.UtilsMonkee as um
from serpentmonkee.CypherQueue import CypherQueue, CypherQueues
from serpentmonkee.CypherTransaction import CypherTransactionBlockWorker
from serpentmonkee.PubSubMonkee import PubSubMonkee


class NeoDriver:  # --------------------------------------------------------------------
    def __init__(self, neoDriver=None, redisClient=None, publisher=None, projectId=None, topicId=None, callingCF=None, maxConnectionLifetime=3600,
                 maxTransactionRetryTime=2):
        self.neoDriver = neoDriver
        self.driverUuid = None
        self.driverStartedAt = None
        self.callingCF = callingCF
        self.redisClient = redisClient
        self.db_fb = None
        # self.cypherQueues = self.makeCypherQueues()
        self.asyncStatements = []
        self.pubsub = PubSubMonkee(publisher, projectId, topicId)
        self.maxConnectionLifetime = maxConnectionLifetime
        self.maxTransactionRetryTime = maxTransactionRetryTime

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
                    max_transaction_retry_time=self.maxTransactionRetryTime,
                    max_connection_lifetime=self.maxConnectionLifetime,

                )
            if neoVersion[0] == '1':
                self.neoDriver = GraphDatabase.driver(
                    uri=neo_uri,
                    auth=basic_auth(
                        user=neo_user,
                        password=neo_pass,
                    ),
                    max_connection_lifetime=self.maxConnectionLifetime,
                    encrypted=True,
                    max_retry_time=self.maxTransactionRetryTime)
