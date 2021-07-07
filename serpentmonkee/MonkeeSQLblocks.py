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
from pg8000 import ProgrammingError, IntegrityError

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
    """

    """

    def __init__(
            self,
            query=None,
            insertList=[],
            queryTypeId=None,
            numRetries=0,
            maxRetries=25,
            soloExecution=0,
            lastExecAttempt=None,
            transactionStatements=[],
            transactionSqb=[]):

        self.query = query
        self.insertList = insertList
        self.createdAt = datetime.now(timezone.utc)
        self.queryTypeId = queryTypeId
        self.numRetries = numRetries
        self.maxRetries = maxRetries
        self.soloExecution = soloExecution
        self.lastExecAttempt = lastExecAttempt

        self.statements = transactionStatements
        if len(transactionStatements) >= 1 or len(transactionSqb) >= 1:
            self.isTransaction = 1
        else:
            self.isTransaction = 0
        self.transactionSqb = transactionSqb
        self.serial_ = self.instanceToSerial()

    def instanceToSerial(self):
        if self.transactionSqb == []:
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
            self.statements = mu.getval(serial_, "statements", [])
            self.transactionSqb = mu.getval(serial_, "transactionSqb", [])
            self.numRetries = mu.getval(serial_, "numRetries")
            self.maxRetries = mu.getval(serial_, "maxRetries")
            self.lastExecAttempt = mu.getval(serial_, "lastExecAttempt")
            if len(self.statements) > 0 and len(self.transactionSqb) == 0:

                for statement in self.statements:
                    sqb = MonkeeSQLblock()
                    sqb.query = mu.getval(statement, "query")
                    sqb.insertList = mu.getval(statement, "insertList")
                    sqb.queryTypeId = mu.getval(statement, "queryTypeId")
                    sqb.numRetries = mu.getval(statement, "numRetries")
                    sqb.maxRetries = mu.getval(statement, "maxRetries")
                    sqb.soloExecution = mu.getval(statement, "soloExecution")
                    sqb.lastExecAttempt = mu.getval(
                        statement, "lastExecAttempt")
                    sqb.serial_ = sqb.instanceToSerial()
                    self.transactionSqb.append(sqb)
        self.instanceToSerial()


class MonkeeSQLblockWorker:
    def __init__(self, environmentName, sqlBHandler, sqlClient, reportCollectionRef=None, fb_db=None):
        self.sqlBHandler = sqlBHandler
        self.environmentName = environmentName
        self.sqlClient = sqlClient
        self.topic_id = 'sql_worker'
        self.reportCollectionRef = reportCollectionRef
        self.fb_db = fb_db
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

    def persistErrorDetailInFB(self, exception, attempt):
        lst = dir(exception)
        dict_ = {'attempt': attempt, 'createdAt': datetime.now(timezone.utc)}
        if 'orig' in lst:
            try:
                dict_['orig_args'] = exception.orig.args[0]
            except:
                dict_['orig_args_M'] = 'Unable to get this'
        if 'args' in lst:
            dict_['args'] = str(exception.args)
        if 'code' in lst:
            dict_['code'] = exception.code
        if 'statement' in lst:
            dict_['statement'] = exception.statement
        if 'params' in lst and exception.params:
            dict_['params'] = list(exception.params)
        if 'connection_invalidated' in lst:
            dict_['connection_invalidated'] = exception.connection_invalidated
        if 'detail' in lst:
            dict_['detail'] = exception.detail

        if self.fb_db:
            destDoc = self.fb_db.collection('logging/sqlQ/errors').document()
            destDoc.set(dict_)

    def executeBlock(self, sqlBlock, priority='L'):
        try:
            with self.sqlClient.connect() as conn:

                sqbs = sqlBlock.transactionSqb
                if sqbs != []:
                    with conn.begin():
                        for sqb in sqbs:
                            conn.execute(
                                sqb['query'],
                                sqb['insertList']
                            )
                else:
                    conn.execute(
                        sqlBlock.query,
                        sqlBlock.insertList
                    )
                    conn.commit()

                    """
                        except BrokenPipeError as e:
                            logging.info(repr(e))
                            sqlBlock.numRetries += 1
                            sqlBlock.lastExecAttempt = datetime.now()
                            if sqlBlock.retryAgain():
                                # if this failed insertList is a batch, add each element of the batch separately and flag each for soloExecution
                                if len(sqlBlock.insertList) >= 1 and isinstance(sqlBlock.insertList[0], list):
                                    for element in sqlBlock.insertList:
                                        sqlB = MonkeeSQLblock(
                                            query=sqlBlock.query, insertList=element, numRetries=sqlBlock.numRetries, soloExecution=1, lastExecAttempt=sqlBlock.lastExecAttempt)
                                        self.sqlBHandler.toQ(sqlB=sqlB, priority=priority)
                                        print(
                                            f'sqlBlock.numRetries = {sqlBlock.numRetries}')
                                elif len(sqlBlock.insertList) >= 1:

                                    self.sqlBHandler.toQ(sqlB=sqlBlock, priority=priority)

                                err = f'{sqlBlock.numRetries} fails | {repr(e)} | Retrying SQL: {sqlBlock.query} | {sqlBlock.insertList} '
                                logging.info(err)
                            else:
                                err = f'!! {sqlBlock.numRetries} fails | {repr(e)} | Abandoning SQL: {sqlBlock.query} | {sqlBlock.insertList}'
                                logging.error(err)

                            self.sqlClient.dispose()"""

        except Exception as e:
            logging.info(repr(e))
            self.persistErrorDetailInFB(e, sqlBlock.numRetries)
            sqlBlock.numRetries += 1
            sqlBlock.lastExecAttempt = datetime.now()
            if sqlBlock.retryAgain():
                # if this failed insertList is a batch, add each element of the batch separately and flag each for soloExecution
                if len(sqlBlock.insertList) >= 1 and isinstance(sqlBlock.insertList[0], list):
                    for element in sqlBlock.insertList:
                        sqlB = MonkeeSQLblock(
                            query=sqlBlock.query, insertList=element, numRetries=sqlBlock.numRetries, soloExecution=1, lastExecAttempt=sqlBlock.lastExecAttempt)
                        self.sqlBHandler.toQ(
                            sqlB=sqlB, priority=priority)
                        print(
                            f'sqlBlock.numRetries = {sqlBlock.numRetries}')
                else:

                    self.sqlBHandler.toQ(sqlB=sqlBlock, priority=priority)

                err = f'{sqlBlock.numRetries} fails | {repr(e)} | Retrying SQL: {sqlBlock.query} | {sqlBlock.insertList}'
                logging.info(err)
                print(err)
            else:
                err = f'!! {sqlBlock.numRetries} fails | {repr(e)} | Abandoning SQL: {sqlBlock.query} | {sqlBlock.insertList}'
                logging.error(err)
                print(err)

            self.sqlClient.dispose()

    def executeBlock_OLD(self, sqlBlock):

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
            elif lastExecAttempt and datetime.now() >= lastExecAttempt + timedelta(seconds=1.5 ** numRetries):
                return dataFromRedis, False
            else:
                sqlB = MonkeeSQLblock()
                sqlB.makeFromSerial(serial_=dataFromRedis)
                time.sleep(1)
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
        if self.sqlBHandler.pubsub:
            future = self.sqlBHandler.pubsub.publish(self.topic_path, data)
            res = future.result()
            print(f"Published message {res} to {self.topic_path}.")

    def sortBatch(self, batch):
        batchDict = {}
        batchList = []
        transactionList = []
        for line in batch:
            isTransaction = mu.getval(line, "isTransaction", 0)
            # The reason that non-transactions are broken up out of their sqbs is so that similar queries can be batched,
            # meaning that all sqbs in this batch where the query part is the same, but only the parameters differ are bundled together and executed as one.
            if isTransaction == 0:
                query = mu.getval(line, "query")

                soloExecution = mu.getval(line, "soloExecution", 0)
                numRetries = mu.getval(line, "numRetries", 0)
                maxRetries = mu.getval(line, "maxRetries", 0)
                lastExecAttempt = mu.getval(line, "lastExecAttempt")
                if query:
                    if soloExecution == 0:
                        if query not in batchDict:
                            batchDict[query] = []
                        batchDict[query].append(line["insertList"])
                    elif soloExecution == 1:  # soloExecution = flagging this element to be executed on its own, i.e. not as part of a batch
                        batchList.append(
                            [query, line["insertList"], numRetries, maxRetries, soloExecution, lastExecAttempt])
            elif isTransaction == 1:
                # if this is a transaction, there is no batching. This implies that the sqb (containing the transaction) can be passed through directly.
                # TODO: add the line's queries to an atomic transaction block. transactionList is a list of [query, line["insertList"], numRetries, maxRetries, soloExecution, lastExecAttempt] lines that are all executed as one transaction
                """sqbs = mu.getval(line, 'transactionSqb', [])
                transaction_i = []
                for sqb in sqbs:
                    transaction_i.append([sqb["query"], sqb["insertList"], sqb["numRetries"],
                                          sqb["maxRetries"], sqb["soloExecution"], sqb["lastExecAttempt"]])
                transactionList.append(transaction_i)
                """
                transactionList.append(line)

        for q in batchDict:
            batchList.append([q, batchDict[q], 0, 30, 0, lastExecAttempt])

        return batchList, transactionList

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
                sortedBatches, transactionList = self.sortBatch(batch)

                for sb in sortedBatches:
                    sqb = MonkeeSQLblock(
                        query=sb[0], insertList=sb[1], numRetries=sb[2], maxRetries=sb[3], soloExecution=sb[4], lastExecAttempt=sb[5])
                    self.executeBlock(sqb, priority=priority)

                for transaction_i in transactionList:
                    # transaction_i is a sqb, probably with more than one transactionSqb entry
                    sqb = MonkeeSQLblock(
                        query=transaction_i['query'], insertList=transaction_i['insertList'],
                        numRetries=transaction_i['numRetries'], maxRetries=transaction_i['maxRetries'],
                        soloExecution=transaction_i['soloExecution'], lastExecAttempt=transaction_i['lastExecAttempt'],
                        transactionSqb=transaction_i['transactionSqb'])
                    self.executeBlock(sqb, priority=priority)

                    """transactionBlock = []
                    for transactionElement in transaction_i:
                        sqb = MonkeeSQLblock(
                            query=transactionElement[0], insertList=transactionElement[1], numRetries=transactionElement[2], maxRetries=transactionElement[3], soloExecution=transactionElement[4], lastExecAttempt=transactionElement[5])
                        transactionBlock.append(sqb)
                    self.executeBlock(transactionBlock)"""
                    # TODO: do not pass an array (txBlock) to executeBlock. Pass the sqb and let executeBlock unpack the sqb's array

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
