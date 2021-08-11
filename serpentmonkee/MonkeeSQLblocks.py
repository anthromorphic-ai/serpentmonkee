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
from serpentmonkee.MonkeeRedis import MonkeeRedis


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
        self.sqlQname_ERR = 'sqlWaiting_error'
        self.sqlQs = [self.sqlQname_H, self.sqlQname_M, self.sqlQname_L]
        if self.pubsub:
            self.topic_path = self.pubsub.topic_path(
                self.environmentName, self.topic_id)
        if self.redis_client:
            self.redis = MonkeeRedis(cfName=None, fb_db=None, redisClient=self.redis_client,
                                     inDebugMode=False)
        else:
            self.redis = None

    def sendFlare(self, messageData='awaken', flareDeadspaceSeconds=10):
        """
        Sends a "flare" (a message to the pubsub topic path = self.topic_path) which will spark the SQL worker to jump into action.
        A flare will only be sent if there's been no flare sending in the last (flareDeadspaceSeconds) seconds (defaults to 10).
        """
        flareSendable = False
        rightNow = datetime.now(timezone.utc)
        data = messageData.encode("utf-8")
        if self.redis_client:
            # Get the last flare-send time
            lastFlare = self.redis.get_project_human_val(
                'SQLHandlerFlareSendTime')
            if not lastFlare:
                flareSendable = True
            # If the seconds-elapsed since this is longer than flareDeadspaceSeconds, send another one. else: eat it.
            elif mu.dateDiff('second', lastFlare, rightNow) >= flareDeadspaceSeconds:
                flareSendable = True

        if self.pubsub and flareSendable:
            self.redis.set_project_human_val(
                'SQLHandlerFlareSendTime', 'datetime', rightNow)
            print(f'sending flare!')
            future = self.pubsub.publish(self.topic_path, data)
            future.result()

    def toQ(self, sqlB, priority='L'):
        """
        Sends the given (sqlB) to the (priority) SQL queue.
        - priority can be one of ['H', 'M', 'L', 'ERR']
        """
        if priority == 'L':
            sqlQname = self.sqlQname_L
        elif priority == 'M':
            sqlQname = self.sqlQname_M
        elif priority == 'H':
            sqlQname = self.sqlQname_H
        elif priority == 'ERR':
            sqlQname = self.sqlQname_ERR
        else:
            sqlQname = self.sqlQname_L

        serial_ = json.dumps(sqlB.instanceToSerial(), cls=mu.RoundTripEncoder)
        self.redis_client.rpush(sqlQname, serial_)
        self.sendFlare()

    def killQueue(self):
        """
        Kills all queues.
        """
        print('KILLING QUEUES')
        self.redis_client.delete(self.sqlQname_H)
        self.redis_client.delete(self.sqlQname_M)
        self.redis_client.delete(self.sqlQname_L)
        self.redis_client.delete(self.sqlQname_ERR)

    def getQLens(self):
        """
        Print all the queues' lengths
        """
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
            transactionSqb=[],
            priority='',
            remainInErrQ=None):

        self.query = query
        self.insertList = insertList
        self.createdAt = datetime.now()
        self.queryTypeId = queryTypeId
        self.numRetries = numRetries
        self.maxRetries = maxRetries
        self.soloExecution = soloExecution
        self.lastExecAttempt = lastExecAttempt
        self.priority = priority
        self.remainInErrorQUntil = remainInErrQ

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
                        "soloExecution": self.soloExecution, "lastExecAttempt": self.lastExecAttempt, "transactionSqb": self.transactionSqb, 'priority': self.priority,
                        "remainInErrorQUntil": self.remainInErrorQUntil}
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
            self.priority = mu.getval(serial_, "priority")
            self.remainInErrorQUntil = mu.getval(
                serial_, "remainInErrorQUntil")
            self.serial_ = self.instanceToSerial()
        elif self.isTransaction == 1:
            self.statements = mu.getval(serial_, "statements", [])
            self.transactionSqb = mu.getval(serial_, "transactionSqb", [])
            self.numRetries = mu.getval(serial_, "numRetries")
            self.maxRetries = mu.getval(serial_, "maxRetries")
            self.lastExecAttempt = mu.getval(serial_, "lastExecAttempt")
            self.priority = mu.getval(serial_, "priority")
            self.remainInErrorQUntil = mu.getval(
                serial_, "remainInErrorQUntil")
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

    def persistErrorDetailInFB(self, exception, attempt, willBeRetried=True):
        lst = dir(exception)
        dict_ = {'attempt': attempt, 'createdAt': datetime.now(),
                 'willBeRetried': willBeRetried, '_type': str(type(exception))}
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
            destDoc = self.fb_db.collection(
                'logging/sqlQ/errors').document(mu.makeAscendingUid())
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
                    # conn.commit()

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

        except sqlalchemy.exc.ProgrammingError as e:
            self.logSqlErrorNoRetry(e, sqlBlock)

        except Exception as e:
            # This last-resort except clause will cause the statement to be retried.
            self.logSqlErrorAndRetry(e, sqlBlock, priority=priority)

        finally:
            print(
                'serpentmonkee.MonkeeSQLblocks.MonkeeSQLblockWorker.executeBlock.finally')

    def logSqlErrorNoRetry(self, e, sqlBlock):
        """
        Logs the Exception (e) and saves a record of it in Firebase. **Does not queue the SQL block for a retry**
        """
        logging.error(repr(e))
        self.persistErrorDetailInFB(
            e, sqlBlock.numRetries, willBeRetried=False)

    def logSqlErrorAndRetry(self, e, sqlBlock, priority='L', attemptsBeforeSentToErrQ=2):
        """
        Logs the Exception (e) and saves a record of it in Firebase. **If the SQL block's number of attempts < the max number of retries, the SQL block is queued for a retry**
        """
        logging.info(repr(e))
        print(f'EXCEPTION: type = {type(e)}')
        self.persistErrorDetailInFB(e, sqlBlock.numRetries)

        sqlBlock.numRetries += 1
        sqlBlock.lastExecAttempt = datetime.now()
        if sqlBlock.retryAgain():
            # if this failed insertList is a batch, add each element of the batch separately and flag each for soloExecution
            if len(sqlBlock.insertList) >= 1 and isinstance(sqlBlock.insertList[0], list):
                for element in sqlBlock.insertList:
                    sqlB = MonkeeSQLblock(
                        query=sqlBlock.query, insertList=element, numRetries=sqlBlock.numRetries, soloExecution=1, lastExecAttempt=sqlBlock.lastExecAttempt,
                        priority=priority)
                    if sqlBlock.numRetries <= attemptsBeforeSentToErrQ:
                        self.sqlBHandler.toQ(
                            sqlB=sqlB, priority=priority)
                    else:  # if the number of retries is attemptsBeforeSentToErrQ+, the sqb goes to the error queue
                        sqlB.remainInErrorQUntil = datetime.now(
                        ) + timedelta(seconds=1.5 ** sqlBlock.numRetries)
                        self.sqlBHandler.toQ(
                            sqlB=sqlB, priority='ERR')

                    print(
                        f'sqlBlock.numRetries = {sqlBlock.numRetries}')
            else:
                sqlBlock.priority = priority
                if sqlBlock.numRetries <= attemptsBeforeSentToErrQ:
                    self.sqlBHandler.toQ(sqlB=sqlBlock, priority=priority)
                else:  # if the number of retries is attemptsBeforeSentToErrQ+, the sqb goes to the error queue
                    sqlBlock.remainInErrorQUntil = datetime.now(
                    ) + timedelta(seconds=1.5 ** sqlBlock.numRetries)
                    self.sqlBHandler.toQ(
                        sqlB=sqlBlock, priority='ERR')

            err = f'{sqlBlock.numRetries} fails | {repr(e)} | Retrying SQL: {sqlBlock.query} | {sqlBlock.insertList}'
            logging.info(err)
            print(err)
        else:
            err = f'!! {sqlBlock.numRetries} fails | {repr(e)} | Abandoning SQL: {sqlBlock.query} | {sqlBlock.insertList}'
            logging.error(err)
            print(err)

    def popNextBlock(self, priority):
        if priority == 'H':
            theQ = self.sqlBHandler.sqlQname_H
        elif priority == 'M':
            theQ = self.sqlBHandler.sqlQname_M
        elif priority == 'L':
            theQ = self.sqlBHandler.sqlQname_L
        elif priority == 'ERR':
            theQ = self.sqlBHandler.sqlQname_ERR

        popped = self.sqlBHandler.redis_client.blpop(theQ, 1)
        if not popped:
            print(
                f"SQL_Q {priority} is EMPTY_________________________________________")
        else:
            dataFromRedis = json.loads(popped[1], cls=mu.RoundTripDecoder)
            numRetries = mu.getval(dataFromRedis, "numRetries", 0)
            lastExecAttempt = mu.getval(dataFromRedis, "lastExecAttempt")
            remainInErrorQUntil = mu.getval(
                dataFromRedis, "remainInErrorQUntil", datetime.now())
            if numRetries == 0:
                return dataFromRedis, False
            # elif lastExecAttempt and datetime.now() >= lastExecAttempt + timedelta(seconds=1.5 ** numRetries):
            elif priority in ['H', 'M', 'L'] and lastExecAttempt and datetime.now() >= lastExecAttempt + timedelta(seconds=2 * numRetries):
                return dataFromRedis, False
            elif priority in ['ERR'] and remainInErrorQUntil and datetime.now() >= remainInErrorQUntil:
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
        elif priority == 'ERR':
            theQ = self.sqlBHandler.sqlQname_ERR

        return self.sqlBHandler.redis_client.llen(theQ)

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
                transactionList.append(line)

        for q in batchDict:
            batchList.append([q, batchDict[q], 0, 30, 0, lastExecAttempt])

        return batchList, transactionList

    def reportOnQueues(self):

        if self.reportCollectionRef:
            priorities = ['H', 'M', 'L']
            qDict = {'qCheckTime': datetime.now()}
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
            docUid = mu.makeAscendingUid()

            self.reportCollectionRef.document(docUid).set(qDict)

    def goToWork(self, forHowLong=60, inactivityBuffer=10, batchSize=50):
        print(f'XXX goingToWork. ForHowLong={forHowLong} s')
        priorities = ['H', 'M', 'L']
        startTs = datetime.now()
        i = 0
        howLong = 0
        self.reportOnQueues()

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

                howLong = mu.dateDiff(
                    'sec', startTs, datetime.now())
                #print(f'sqlw Running for how long: {howLong}')
                qlen = self.getQLens(priority=priority)
                if qlen == 0:
                    queuesAreEmpty = True
                else:
                    queuesAreEmpty = False

        if howLong >= forHowLong - inactivityBuffer and qlen > 0:
            numFlares = 3
            for k in range(numFlares):
                self.sqlBHandler.sendFlare()

    def goToWorkOnErrQ(self, forHowLong=60, inactivityBuffer=10):
        """
        An agent which pops elements from the ERR queue and repatriates these elements back to their original queues if they have served their time
        in the error queue
        """
        print(f'XXX goingToWork. ForHowLong={forHowLong} s')
        i = 0
        howLong = 0
        numErrorElements = self.getQLens(priority='ERR')
        queuesAreEmpty = False

        while howLong <= forHowLong - inactivityBuffer and i <= numErrorElements and not queuesAreEmpty:
            i += 1
            k = 0
            sqlB, queuesAreEmpty = self.popNextBlock(priority='ERR')

            if sqlB:
                sqb = MonkeeSQLblock()
                sqb.makeFromSerial(sqlB)
                self.sqlBHandler.toQ(sqlB=sqb, priority=sqb.priority)

            qlen = self.getQLens(priority='ERR')
            if qlen == 0:
                queuesAreEmpty = True
            else:
                queuesAreEmpty = False

        if howLong >= forHowLong - inactivityBuffer and qlen > 0:
            # Unsure if the ERR Q needs flares. Do not currently think so.
            numFlares = 0
            for k in range(numFlares):
                print(f'sending flare (max {numFlares}) {k}')
                self.sqlBHandler.sendFlare()
