# _METADATA_:Version: 11
# _METADATA_:Timestamp: 2021-03-12 01:41:55.898641+00:00
# _METADATA_:MD5: 86081db65f59e768b13b39f82fddc114
# _METADATA_:Publish:                                                                 None
# _METADATA_:

from datetime import datetime, timedelta, timezone
import random
import sqlalchemy
from sqlalchemy.sql.expression import bindparam
import json
import logging
import time
import pg8000


import serpentmonkee.UtilsMonkee as mu
from serpentmonkee.MonkeeSqlMessenger import MonkeeSQLblock


class MonkeeSQLblockWorker:
    def __init__(self, environmentName, sqlBHandler, sqlClient):
        self.sqlBHandler = sqlBHandler
        self.environmentName = environmentName
        self.sqlClient = sqlClient
        self.topic_id = 'sql_worker'
        self.topic_path = self.sqlBHandler.pubsub.topic_path(
            self.environmentName, self.topic_id)

    def syncRunSQL(self, sql):
        with self.sqlClient.connect() as conn:
            try:
                conn.execute(
                    sql
                )
            except Exception as e:
                print(repr(e))

    def executeBlock(self, sqlBlock):
        with self.sqlClient.connect() as conn:
            try:
                conn.execute(
                    sqlBlock.query,
                    sqlBlock.insertList
                )

            except Exception as e:  # except pg8000.core.ProgrammingError:
                sqlBlock.numRetries += 1
                if sqlBlock.tryAgain():
                    self.sqlBHandler.toQ(sqlB=sqlBlock)
                    err = f'{sqlBlock.numRetries} fails. Retrying SQL: {sqlBlock.query} | {sqlBlock.insertList} | {repr(e)}'
                    logging.error(err)
                else:
                    err = f'!! {sqlBlock.numRetries} fails. Abandoning SQL: {sqlBlock.query} | {sqlBlock.insertList} | {repr(e)}'
                    logging.error(err)

    def popNextBlock(self, priority):
        if priority == 'H':
            theQ = self.sqlBHandler.sqlQname_H
        elif priority == 'M':
            theQ = self.sqlBHandler.sqlQname_M
        elif priority == 'L':
            theQ = self.sqlBHandler.sqlQname_L

        popped = self.sqlBHandler.redis_client.blpop(theQ, 1)
        if not popped:
            print(
                f"SQL_Q {priority} is EMPTY_________________________________________")
        else:
            dataFromRedis = json.loads(popped[1], cls=mu.RoundTripDecoder)
            return dataFromRedis, False
        return None, True

    def getQLens(self, priority):
        if priority == 'H':
            theQ = self.sqlBHandler.sqlQname_H
        elif priority == 'M':
            theQ = self.sqlBHandler.sqlQname_M
        elif priority == 'L':
            theQ = self.sqlBHandler.sqlQname_L

        return self.sqlBHandler.redis_client.llen(theQ)

    def sendFlare(self, messageData='awaken'):
        data = messageData.encode("utf-8")
        future = self.sqlBHandler.pubsub.publish(self.topic_path, data)
        res = future.result()
        print(f"Published message {res} to {self.topic_path}.")

    def sortBatch(self, batch):
        retDict = {}
        for line in batch:
            query = mu.getval(line, "query")
            if query:
                if query not in retDict:
                    retDict[query] = []
                retDict[query].append(line["insertList"])
        return retDict

    def goToWork(self, forHowLong=60, inactivityBuffer=10, batchSize=50):
        print(f'XXX goingToWork. ForHowLong={forHowLong}')
        priorities = ['H', 'M', 'L']
        startTs = datetime.now(timezone.utc)
        i = 0
        howLong = 0
        # High Priority

        for priority in priorities:
            queuesAreEmpty = False
            while howLong <= forHowLong - inactivityBuffer and not queuesAreEmpty:
                i += 1
                k = 0
                batch = []
                while not queuesAreEmpty and k < batchSize:
                    sqlB, queuesAreEmpty = self.popNextBlock(priority=priority)
                    if sqlB:
                        batch.append(sqlB)
                    k += 1
                sortedBatches = self.sortBatch(batch)

                for sb in sortedBatches:
                    sqb = MonkeeSQLblock(
                        query=sb, insertList=sortedBatches[sb])
                    self.executeBlock(sqb)

                howLong = mu.dateDiff(
                    'sec', startTs, datetime.now(timezone.utc))
                print(f'sqlw Running for how long: {howLong}')
                qlen = self.getQLens(priority=priority)

        if howLong >= forHowLong - inactivityBuffer and qlen > 0:
            # numFlares = self.cypherQueues.totalInWaitingQueues / 10
            for k in range(3):
                print(f'sending flare (max 3) {k}')
                self.sendFlare()
                time.sleep(0.5)
