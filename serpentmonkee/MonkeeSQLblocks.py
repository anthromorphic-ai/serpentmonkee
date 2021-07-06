# _METADATA_:Version: 11
# _METADATA_:Timestamp: 2021-03-12 01:41:55.898641+00:00
# _METADATA_:MD5: 86081db65f59e768b13b39f82fddc114
# _METADATA_:Publish:                                                                 None
# _METADATA_:

from datetime import datetime, timedelta, timezone
import random
import sqlalchemy
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.orm import Session
import json
import logging
import time
import pg8000

import serpentmonkee.UtilsMonkee as mu
#from serpentmonkee.MonkeeSqlMessenger import MonkeeSQLblock


class MonkeeSQLblockHandler:
    def __init__(
            self,
            environmentName,
            redis_client,
            pubsub):
        self.environmentName = environmentName
        self.redis_client = redis_client

        self.pubsub = pubsub
        self.topic_id = 'sql_worker'
        self.sqlQname_H = 'sqlWaiting_high'
        self.sqlQname_M = 'sqlWaiting_medium'
        self.sqlQname_L = 'sqlWaiting_low'
        self.sqlQs = [self.sqlQname_H, self.sqlQname_M, self.sqlQname_L]
        if self.pubsub:
            self.topic_path = self.pubsub.topic_path(
                self.environmentName, self.topic_id)

    def sendFlare(self, messageData='awaken'):
        data = messageData.encode("utf-8")
        if self.pubsub:
            future = self.pubsub.publish(self.topic_path, data)
            future.result()

    def toQ(self, sqlB, priority='L'):
        if priority == 'L':
            sqlQname = self.sqlQname_L
        elif priority == 'M':
            sqlQname = self.sqlQname_M
        elif priority == 'H':
            sqlQname = self.sqlQname_H
        else:
            sqlQname = self.sqlQname_L

        serial_ = json.dumps(sqlB.instanceToSerial(), cls=mu.RoundTripEncoder)
        self.redis_client.rpush(sqlQname, serial_)
        self.sendFlare()

    def killQueue(self):
        print('KILLING QUEUE')
        self.redis_client.delete(self.sqlQname_H)
        self.redis_client.delete(self.sqlQname_M)
        self.redis_client.delete(self.sqlQname_L)

    def getQLens(self):
        lenString = "Q LENGTHS: "
        for q in self.sqlQs:
            l = self.redis_client.llen(q)
            lenString += f'Q={q} len={l},  '
        print(lenString)


class MonkeeSQLblock:
    def __init__(
            self,
            query=None,
            insertList=[],
            queryTypeId=None,
            numRetries=0,
            maxRetries=30,
            soloExecution=0,
            lastExecAttempt=None,
            transactionStatements=[]):

        self.query = query
        self.insertList = insertList
        self.createdAt = datetime.now(timezone.utc)
        self.queryTypeId = queryTypeId
        self.numRetries = numRetries
        self.maxRetries = maxRetries
        self.soloExecution = soloExecution
        self.lastExecAttempt = lastExecAttempt

        self.statements = transactionStatements
        if len(transactionStatements) >= 1:
            self.isTransaction = 1
        else:
            self.isTransaction = 0
        self.transactionSqb = []
        self.serial_ = self.instanceToSerial()

    def instanceToSerial(self):
        self.transactionSqb = []
        for i in self.statements:
            self.transactionSqb.append(i.instanceToSerial())
        self.serial_ = {"isTransaction": self.isTransaction, "query": self.query, "insertList": self.insertList, "queryTypeId": self.queryTypeId, "numRetries": self.numRetries, "maxRetries": self.maxRetries,
                        "soloExecution": self.soloExecution, "lastExecAttempt": self.lastExecAttempt, "transactionSqb": self.transactionSqb}
        return self.serial_

    def retryAgain(self):
        print(f'retryAgain: {self.numRetries} / {self.maxRetries}')
        return int(self.numRetries) <= int(self.maxRetries)

    def makeFromSerial(self, serial_):
        self.isTransaction = mu.getval(serial_, "isTransaction", 0)
        if self.isTransaction == 0:
            self.query = mu.getval(serial_, "query")
            self.insertList = mu.getval(serial_, "insertList")
            self.queryTypeId = mu.getval(serial_, "queryTypeId")
            self.numRetries = mu.getval(serial_, "numRetries")
            self.maxRetries = mu.getval(serial_, "maxRetries")
            self.soloExecution = mu.getval(serial_, "soloExecution")
            self.lastExecAttempt = mu.getval(serial_, "lastExecAttempt")
            self.serial_ = self.instanceToSerial()
        elif self.isTransaction == 1:
            self.statements = self.query = mu.getval(serial_, "statements", [])
            for statement in self.statements:
                sqb = MonkeeSQLblock()
                sqb.query = mu.getval(statement, "query")
                sqb.insertList = mu.getval(statement, "insertList")
                sqb.queryTypeId = mu.getval(statement, "queryTypeId")
                sqb.numRetries = mu.getval(statement, "numRetries")
                sqb.maxRetries = mu.getval(statement, "maxRetries")
                sqb.soloExecution = mu.getval(statement, "soloExecution")
                sqb.lastExecAttempt = mu.getval(statement, "lastExecAttempt")
                sqb.serial_ = sqb.instanceToSerial()
                self.transactionSqb.append(sqb)


class MonkeeSQLblockWorker:
    def __init__(self, environmentName, sqlBHandler, sqlClient, reportCollectionRef=None):
        self.sqlBHandler = sqlBHandler
        self.environmentName = environmentName
        self.sqlClient = sqlClient
        self.topic_id = 'sql_worker'
        self.reportCollectionRef = reportCollectionRef
        if self.sqlBHandler.pubsub:
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
                # if sqlBlock is a list of sqlBlocks, run it as one transaction
                if isinstance(sqlBlock, list):
                    with conn.begin():
                        for block in sqlBlock:
                            theBlock = block
                            conn.execute(
                                block.query,
                                block.insertList
                            )
                else:
                    theBlock = sqlBlock
                    conn.execute(
                        sqlBlock.query,
                        sqlBlock.insertList
                    )
                    conn.commit()

            except BrokenPipeError as e:
                logging.info(repr(e))
                theBlock.numRetries += 1
                theBlock.lastExecAttempt = datetime.now()
                if theBlock.retryAgain():
                    # if this failed insertList is a batch, add each element of the batch separately and flag each for soloExecution
                    if len(theBlock.insertList) >= 1 and isinstance(theBlock.insertList[0], list):
                        for element in theBlock.insertList:
                            sqlB = MonkeeSQLblock(
                                query=theBlock.query, insertList=element, numRetries=sqlBlock.numRetries, soloExecution=1, lastExecAttempt=sqlBlock.lastExecAttempt)
                            self.sqlBHandler.toQ(sqlB=sqlB)
                            print(
                                f'theBlock.numRetries = {theBlock.numRetries}')
                    elif len(theBlock.insertList) >= 1:

                        self.sqlBHandler.toQ(sqlB=sqlBlock)

                    err = f'{theBlock.numRetries} fails | {repr(e)} | Retrying SQL: {theBlock.query} | {theBlock.insertList} '
                    logging.info(err)
                else:
                    err = f'!! {theBlock.numRetries} fails | {repr(e)} | Abandoning SQL: {theBlock.query} | {theBlock.insertList}'
                    logging.error(err)

                self.sqlClient.dispose()

            except Exception as e:
                logging.info(repr(e))
                theBlock.numRetries += 1
                theBlock.lastExecAttempt = datetime.now()
                if theBlock.retryAgain():
                    # if this failed insertList is a batch, add each element of the batch separately and flag each for soloExecution
                    if len(theBlock.insertList) >= 1 and isinstance(theBlock.insertList[0], list):
                        for element in theBlock.insertList:
                            sqlB = MonkeeSQLblock(
                                query=theBlock.query, insertList=element, numRetries=theBlock.numRetries, soloExecution=1, lastExecAttempt=sqlBlock.lastExecAttempt)
                            self.sqlBHandler.toQ(sqlB=sqlB)
                            print(
                                f'theBlock.numRetries = {theBlock.numRetries}')
                    elif len(theBlock.insertList) >= 1:

                        self.sqlBHandler.toQ(sqlB=sqlBlock)

                    err = f'{theBlock.numRetries} fails | {repr(e)} | Retrying SQL: {theBlock.query} | {theBlock.insertList}'
                    logging.info(err)
                else:
                    err = f'!! {theBlock.numRetries} fails | {repr(e)} | Abandoning SQL: {theBlock.query} | {theBlock.insertList}'
                    logging.error(err)

                self.sqlClient.dispose()

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
            numRetries = mu.getval(dataFromRedis, "numRetries", 0)
            lastExecAttempt = mu.getval(dataFromRedis, "lastExecAttempt")
            if numRetries == 0:
                return dataFromRedis, False
            elif lastExecAttempt and datetime.now() >= lastExecAttempt + timedelta(seconds=5 * numRetries):
                return dataFromRedis, False
            else:
                sqlB = MonkeeSQLblock()
                sqlB.makeFromSerial(serial_=dataFromRedis)
                self.sqlBHandler.toQ(sqlB, priority=priority)

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
        retList = []
        transactions = []
        for line in batch:
            isTransaction = mu.getval(line, "isTransaction", 0)

            if isTransaction == 0:
                query = mu.getval(line, "query")
                # soloExecution = flagging this element to be executed on its own, i.e. not as part of a batch
                soloExecution = mu.getval(line, "soloExecution", 0)
                numRetries = mu.getval(line, "numRetries", 0)
                maxRetries = mu.getval(line, "maxRetries", 0)
                lastExecAttempt = mu.getval(line, "lastExecAttempt")
                if query:
                    if soloExecution == 0:
                        if query not in retDict:
                            retDict[query] = []
                        retDict[query].append(line["insertList"])
                    elif soloExecution == 1:
                        retList.append(
                            [query, line["insertList"], numRetries, maxRetries, soloExecution, lastExecAttempt])
            elif isTransaction == 1:
                # TODO: add the line's queries to an atomic transaction block. transactions is a list of [query, line["insertList"], numRetries, maxRetries, soloExecution, lastExecAttempt] lines that are all executed as one transaction
                print('adding to transaction block...')
                sqbs = mu.getval(line, 'transactionSqb', [])
                transaction_i = []
                for sqb in sqbs:
                    transaction_i.append([sqb["query"], sqb["insertList"], sqb["numRetries"],
                                          sqb["maxRetries"], sqb["soloExecution"], sqb["lastExecAttempt"]])
                transactions.append(transaction_i)

        for q in retDict:
            retList.append([q, retDict[q], 0, 30, 0, lastExecAttempt])

        return retList, transactions

    def reportOnQueues(self):

        if self.reportCollectionRef:
            priorities = ['H', 'M', 'L']
            qDict = {'qCheckTime': datetime.now(timezone.utc)}
            for priority in priorities:
                qArray = []

                if priority == 'H':
                    theQ = self.sqlBHandler.sqlQname_H
                elif priority == 'M':
                    theQ = self.sqlBHandler.sqlQname_M
                elif priority == 'L':
                    theQ = self.sqlBHandler.sqlQname_L

                qlen = self.getQLens(priority)
                qlenKey = f'{priority}__len'
                qcontentKey = f'{priority}_content'

                qDict[qlenKey] = qlen

                qcontents = self.sqlBHandler.redis_client.lrange(
                    theQ, -10, -1)
                for element in qcontents:
                    elementParsed = json.loads(
                        element, cls=mu.RoundTripDecoder)
                    qArray.append(elementParsed)

                qDict[qcontentKey] = qArray
            docUid = str(1625607464 * 3 - int(time.time()))

            self.reportCollectionRef.document(docUid).set(qDict)

    def goToWork(self, forHowLong=60, inactivityBuffer=10, batchSize=50):
        print(f'XXX goingToWork. ForHowLong={forHowLong}')
        priorities = ['H', 'M', 'L']
        startTs = datetime.now(timezone.utc)
        i = 0
        howLong = 0
        self.reportOnQueues()
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
                sortedBatches, transactions = self.sortBatch(batch)

                for sb in sortedBatches:
                    sqb = MonkeeSQLblock(
                        query=sb[0], insertList=sb[1], numRetries=sb[2], maxRetries=sb[3], soloExecution=sb[4], lastExecAttempt=sb[5])
                    self.executeBlock(sqb)

                for transactionBatch in transactions:
                    transactionBlock = []
                    for transactionElement in transactionBatch:
                        sqb = MonkeeSQLblock(
                            query=transactionElement[0], insertList=transactionElement[1], numRetries=transactionElement[2], maxRetries=transactionElement[3], soloExecution=transactionElement[4], lastExecAttempt=transactionElement[5])
                        transactionBlock.append(sqb)
                    self.executeBlock(transactionBlock)

                howLong = mu.dateDiff(
                    'sec', startTs, datetime.now(timezone.utc))
                #print(f'sqlw Running for how long: {howLong}')
                qlen = self.getQLens(priority=priority)
                if qlen == 0:
                    queuesAreEmpty = True
                else:
                    queuesAreEmpty = False

        if howLong >= forHowLong - inactivityBuffer and qlen > 0:
            # numFlares = self.cypherQueues.totalInWaitingQueues / 10
            for k in range(3):
                print(f'sending flare (max 3) {k}')
                self.sendFlare()
                time.sleep(0.5)
