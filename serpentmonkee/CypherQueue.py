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
    def __init__(self, redisClient, cQueues, workingQ, completedQ):
        self.cQueues = cQueues
        self.workingQ = workingQ
        self.completedQ = completedQ
        self.redisClient = redisClient
        self.cQNames = []
        for q in self.cQueues:
            self.cQNames.append(q.queueName)

    def getQLens(self):
        lenString = ""
        for q in self.cQNames:
            l = self.redisClient.llen(q)
            lenString += f'Q={q} len={l}, \n'

        lenString += f'Q=Working len={self.redisClient.llen(self.workingQ.queueName)}, \n'
        lenString += f'Q=Completed len={self.redisClient.llen(self.completedQ.queueName)}, \n'
        print(lenString)

    def pushCypherQueryToQueue(self, ctb, queueName):
        serial_ = json.dumps(ctb.instanceToSerial(), cls=um.RoundTripEncoder)
        self.redisClient.rpush(queueName, serial_)

    def pushCtbToWorkingQ(self, ctbSerial, isLeft=False):
        if isLeft:
            self.redisClient.lpush(self.workingQ.queueName, ctbSerial)
        else:
            self.redisClient.rpush(self.workingQ.queueName, ctbSerial)

    def removeCtbFromWorkingQ(self, ctbSerial):
        removed = self.redisClient.lrem(self.workingQ.queueName, 0, ctbSerial)
        return removed

    def popCtbSerialFromWorkingQ(self):
        """
        Fetches (LPOP) the next ctb from the queues
        """
        popped = self.redisClient.blpop(
            self.workingQ.queueName, 0.01)
        # popped = self.redisClient.lpop(self.queueName)

        if not popped:
            print("WorkingQ IS EMPTY_________________________________________")
        else:
            dataFromRedis = json.loads(popped[1], cls=um.RoundTripDecoder)

            #print(f"Data read from WORKING Q:{dataFromRedis}")
            return dataFromRedis
        return None

    def pushCtbToWaitingQ(self, ctBlock):
        """
        Pushes (RPUSH) the given ctb back to one of the queues
        """
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

    def pushCtbToCompletedQ(self, ctBlock):
        """
        Pushes (RPUSH) the given ctb back to one of the queues
        """
        ctBlock.lastUpdatedAt = datetime.now(timezone.utc)
        self.pushCypherQueryToQueue(ctBlock, self.completedQ.queueName)
