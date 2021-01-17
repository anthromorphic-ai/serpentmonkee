# _METADATA_:Version: 20
# _METADATA_:Timestamp: 2021-01-17 21:26:27.472349+00:00
# _METADATA_:MD5: bb0d3ddab94c18b6a97f3384b655233b
# _METADATA_:Publish:                                                                              None


# _METADATA_:
from datetime import datetime, timedelta, timezone
import serpentmonkee.UtilsMonkee as um
import json
from neo4j.exceptions import ServiceUnavailable, CypherSyntaxError
import logging


class CypherQueue:
    def __init__(self, queueName):
        self.queueName = queueName


class CypherQueues:
    def __init__(self, redisClient, cQueues, workingQ, completedQ, fb_db=None):
        self.cQueues = cQueues  # Low, Medium, High
        self.workingQ = workingQ
        self.completedQ = completedQ
        self.redisClient = redisClient
        self.cQNames = []
        self.fb_db = fb_db
        for q in self.cQueues:
            self.cQNames.append(q.queueName)
        self.totalInWaitingQueues = 0
        self.totalInWorkingQueue = 0

    def killQueues(self):
        print('KILLING ALL QUEUES')
        for q in self.cQNames:
            self.redisClient.delete(q)

        self.redisClient.delete(self.workingQ.queueName)
        self.redisClient.delete(self.completedQ.queueName)

    def getQLens(self):
        self.totalInWaitingQueues = 0
        self.totalInWorkingQueue = 0
        lenString = "Q LENGTHS: "
        for q in self.cQNames:
            l = self.redisClient.llen(q)
            self.totalInWaitingQueues += l
            lenString += f'Q={q} len={l},  '

        self.totalInWorkingQueue = self.redisClient.llen(
            self.workingQ.queueName)
        lenString += f'Q=Working len={self.totalInWorkingQueue},  '

        lenString += f'Q=Completed len={self.redisClient.llen(self.completedQ.queueName)},  '
        print(lenString)

    def pushCypherQueryToQueue(self, ctb, queueName, isCompleted=False, jumpTheQ=False):
        print('XXX pushCypherQueryToQueue')
        serial_ = json.dumps(ctb.instanceToSerial(), cls=um.RoundTripEncoder)
        if isCompleted:
            ctb.registerChangeInSql('toCompleted')
        else:
            ctb.registerChangeInSql('toWaiting')

        if jumpTheQ:
            self.redisClient.lpush(queueName, serial_)
        else:
            self.redisClient.rpush(queueName, serial_)
        print('XXX self.redisClient.rpush done')

    def pushCtbToWorkingQ(self, ctb, isLeft=False):
        serial_ = json.dumps(ctb.instanceToSerial(), cls=um.RoundTripEncoder)
        print(f'pushCtbToWorkingQ. serial_={serial_}')
        ctb.registerChangeInSql('toWorking')
        if isLeft:
            self.redisClient.lpush(self.workingQ.queueName, serial_)
        else:
            self.redisClient.rpush(self.workingQ.queueName, serial_)

    def removeCtbFromWorkingQ(self, ctbSerial):
        removed = self.redisClient.lrem(self.workingQ.queueName, 0, ctbSerial)
        print(
            f'removeCtbFromWorkingQ: removed={removed}, ctbSerial= {ctbSerial}')
        return removed

    def popCtbSerialFromWorkingQ(self):
        """
        Fetches (LPOP) the next ctb from the queues
        """
        popped = self.redisClient.blpop(
            self.workingQ.queueName, 1)
        # popped = self.redisClient.lpop(self.queueName)

        if not popped:
            print("WorkingQ IS EMPTY_________________________________________")
        else:
            dataFromRedis = json.loads(popped[1], cls=um.RoundTripDecoder)

            #print(f"Data read from WORKING Q:{dataFromRedis}")
            return dataFromRedis
        return None

    def copyCtbSerialFromWorkingQ(self):
        """
        Copies (LRANGE) the first ctb from the queue
        """
        qLen = self.redisClient.llen(self.workingQ.queueName)
        if qLen == 0:
            print("WorkingQ IS EMPTY_________________________________________")
        else:
            copiedList = self.redisClient.lrange(
                self.workingQ.queueName, 0, qLen)

            if len(copiedList) > 0:
                dataFromRedis = json.loads(
                    copiedList[0], cls=um.RoundTripDecoder)

                return dataFromRedis
        return None

    def writeToFB(self, fbCollection, uid, dict_):

        fbCollection.document(uid).set(dict_)

    def pushCtbToWaitingQ(self, ctBlock, jumpTheQ=False):
        """
        Pushes (RPUSH) the given ctb back to one of the queues
        """
        print('XXX pushCtbToWaitingQ: {}'.format(ctBlock.json))
        if self.fb_db is not None:
            self.writeToFB(fbCollection=self.fb_db.collection('apps/wicPlaypen/queues'),
                           uid=ctBlock.transactionUid, dict_=ctBlock.json)
        ctBlock.lastUpdatedAt = datetime.now(timezone.utc)
        if len(self.cQueues) == 3:
            if ctBlock.priority == 'H':
                self.pushCypherQueryToQueue(
                    ctBlock, self.cQueues[0].queueName, jumpTheQ=jumpTheQ)
            elif ctBlock.priority == 'M':
                self.pushCypherQueryToQueue(
                    ctBlock, self.cQueues[1].queueName, jumpTheQ=jumpTheQ)
            else:
                self.pushCypherQueryToQueue(
                    ctBlock, self.cQueues[2].queueName, jumpTheQ=jumpTheQ)
        elif len(self.cQueues) == 2:
            if ctBlock.priority == 'H':
                self.pushCypherQueryToQueue(
                    ctBlock, self.cQueues[0].queueName, jumpTheQ=jumpTheQ)
            else:
                self.pushCypherQueryToQueue(
                    ctBlock, self.cQueues[1].queueName, jumpTheQ=jumpTheQ)
        elif len(self.cQueues) >= 1:
            self.pushCypherQueryToQueue(
                ctBlock, self.cQueues[0].queueName, jumpTheQ=jumpTheQ)

    def pushCtbToCompletedQ(self, ctBlock):
        """
        Pushes (RPUSH) the given ctb back to one of the queues
        """
        ctBlock.lastUpdatedAt = datetime.now(timezone.utc)
        self.pushCypherQueryToQueue(
            ctBlock, self.completedQ.queueName, isCompleted=True)
