# _METADATA_:Version: 29
# _METADATA_:Timestamp: 2020-10-15 22:45:49.214813+00:00
# _METADATA_:MD5: 087277b51196c7a2a79f2a014f20f0e2
# _METADATA_:Publish:       None

# _METADATA_:
from datetime import datetime, timedelta, timezone
from serpentmonkee import NeoMonkee, UtilsMonkee as um
import json
from neo4j.exceptions import ServiceUnavailable, CypherSyntaxError
import logging


class CypherQueue:
    def __init__(self, queueName):
        self.queueName = queueName


class CypherQueues:
    def __init__(self, redisClient, cQueues):
        self.cQueues = cQueues
        self.redisClient = redisClient
        self.cQNames = []
        for q in self.cQueues:
            self.cQNames.append(q.queueName)

    def pushCypherQueryToQueue(self, ctb, queueName):
        serial_ = json.dumps(ctb.instanceToSerial(), cls=um.RoundTripEncoder)
        self.redisClient.rpush(queueName, serial_)

    def pushCtbToWaitingQ(self, ctBlock):
        """
        Pushes (RPUSH) the given ctb back to one of the queues
        """
        ctBlock.numRetries += 1
        ctBlock.lastUpdatedAt = datetime.now(timezone.utc)
        if len(self.cQueues) == 3:
            if ctBlock.priority == 'H':
                self.pushCypherQueryToQueue(ctBlock, self.cQueues[0].queueName)
            elif ctBlock.priority == 'M':
                self.pushCypherQueryToQueue(ctBlock, self.cQueues[1].queueName)
            else:
                self.pushCypherQueryToQueue(ctBlock, self.cQueues[2].queueName)
        elif len(self.cQueues) == 2:
            if ctBlock.priority == 'H':
                self.pushCypherQueryToQueue(ctBlock, self.cQueues[0].queueName)
            else:
                self.pushCypherQueryToQueue(ctBlock, self.cQueues[1].queueName)
        elif len(self.cQueues) >= 1:
            self.pushCypherQueryToQueue(ctBlock, self.cQueues[0].queueName)
