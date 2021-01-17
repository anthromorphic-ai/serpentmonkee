# _METADATA_:Version: 20
# _METADATA_:Timestamp: 2021-01-17 21:26:27.470108+00:00
# _METADATA_:MD5: 3a4c1f7133dc2092dfa07607ac23584c
# _METADATA_:Publish:                                                                       None
# _METADATA_:
import requests
from dateutil import parser
import json
from datetime import datetime, timezone
import time
import sys
import random
import uuid
import copy
import UtilsMonkee as um
from neo4j.exceptions import CypherSyntaxError, ServiceUnavailable, ClientError
from PubSubMonkee import PubSubMonkee
import logging
# --------------------------------------------------------------------


class CypherTransactionBlock:
    def __init__(self,
                 priority=None,
                 statements=None,
                 transactionUid=None,
                 origin=None,
                 callingCF=None,
                 sqlClient=None):
        self.createdAt = datetime.now(timezone.utc)
        self.numRetries = 0
        self.lastUpdatedAt = datetime.now(timezone.utc)
        self.priority = priority
        self.statements = statements
        self.transactionUid = transactionUid
        self.origin = origin
        self.runTime = None
        self.status = None
        self.errors = None
        self.durations = []
        self.callingCF = callingCF
        self.timeInQ = None
        self.setJson()
        self.sqlClient = sqlClient
        self.registerChangeInSql('create')
        self.redisKey = ''
        self.qTable = 'monkee.q'
        self.qLogTable = 'monkee.q_log'

    def setJson(self):
        self.json = {
            "priority": self.priority,
            "numRetries": self.numRetries,
            "createdAt": self.createdAt,
            "lastUpdatedAt": self.lastUpdatedAt,
            "uid": self.transactionUid,
            "origin": self.origin,
            "statements": self.statements,
            "runTime": self.runTime,
            "status": self.status,
            "errors": self.errors,
            "durations": self.durations,
            "callingCF": self.callingCF,
            "timeInQ": self.timeInQ
        }

    def instanceToSerial(self):
        return json.dumps(self.json, cls=um.RoundTripEncoder)

    def makeFromSerial(self, serial):
        if not isinstance(serial, dict):
            dict_ = json.loads(serial, cls=um.RoundTripDecoder)
        else:
            dict_ = serial
        self.priority = um.getval(dict_, "priority")
        self.numRetries = um.getval(dict_, "numRetries")
        self.createdAt = um.getval(dict_, "createdAt")
        self.lastUpdatedAt = um.getval(dict_, "lastUpdatedAt")
        self.transactionUid = um.getval(dict_, "uid")
        self.origin = um.getval(dict_, "origin")
        self.statements = um.getval(dict_, "statements")
        self.runTime = um.getval(dict_, "runTime")
        self.status = um.getval(dict_, "status")
        self.errors = um.getval(dict_, "errors")
        self.durations = um.getval(dict_, "durations")
        self.callingCF = um.getval(dict_, "callingCF")
        self.timeInQ = um.getval(dict_, "timeInQ")
        self.setJson()

    def registerChangeInSql(self, newState, error=None):
        """
        Registers the change in the state of this CTB in the monkee.q table in SQL
        """
        if self.transactionUid is not None:
            sqlInsertQuery = (
                """ INSERT INTO """ + self.qLogTable + """(q_uid, status)
            values ( %s,%s )
            """
            )
            with self.sqlClient.connect() as conn:
                conn.execute(
                    sqlInsertQuery,
                    [
                        self.transactionUid,
                        newState
                    ],
                )

            if newState == 'create':
                sqlInsertQuery = (
                    """ INSERT INTO """ + self.qTable + """(q_uid, ctb_serial, created_at)
                select %s,%s,%s
                where not exists (select * from """ + self.qTable + """ where q_uid=%s)"""
                )
                with self.sqlClient.connect() as conn:
                    conn.execute(
                        sqlInsertQuery,
                        [
                            self.transactionUid,
                            json.dumps(self.json, cls=um.RoundTripEncoder),
                            self.createdAt,
                            self.transactionUid
                        ],
                    )
            elif newState == 'toWaiting':
                sqlQuery = (
                    """ UPDATE """ + self.qTable + """ 
                    SET waiting_q_at = %s
                    WHERE q_uid = %s"""
                )
                with self.sqlClient.connect() as conn:
                    conn.execute(
                        sqlQuery,
                        [
                            datetime.now(timezone.utc),
                            self.transactionUid

                        ],
                    )
            elif newState == 'toWorking':
                sqlQuery = (
                    """ UPDATE """ + self.qTable + """ 
                    SET working_q_at = %s
                    WHERE q_uid = %s"""
                )
                with self.sqlClient.connect() as conn:
                    conn.execute(
                        sqlQuery,
                        [
                            datetime.now(timezone.utc),
                            self.transactionUid

                        ],
                    )

            elif newState == 'toCompleted':
                sqlQuery = (
                    """ UPDATE """ + self.qTable + """ 
                    SET compl_q_at = %s
                    WHERE q_uid = %s"""
                )
                with self.sqlClient.connect() as conn:
                    conn.execute(
                        sqlQuery,
                        [
                            datetime.now(timezone.utc),
                            self.transactionUid

                        ],
                    )

            elif newState == 'executeStart':
                sqlQuery = (
                    """ UPDATE """ + self.qTable + """ 
                    SET exec_started_at = %s
                    WHERE q_uid = %s"""
                )
                with self.sqlClient.connect() as conn:
                    conn.execute(
                        sqlQuery,
                        [
                            datetime.now(timezone.utc),
                            self.transactionUid

                        ],
                    )
            elif newState == 'executeEnd':
                sqlQuery = (
                    """ UPDATE """ + self.qTable + """ 
                    SET exec_completed_at = %s
                    WHERE q_uid = %s"""
                )
                with self.sqlClient.connect() as conn:
                    conn.execute(
                        sqlQuery,
                        [
                            datetime.now(timezone.utc),
                            self.transactionUid

                        ],
                    )
            elif newState == 'error':
                sqlQuery = (
                    """ UPDATE """ + self.qTable + """ 
                    SET errors = %s
                    WHERE q_uid = %s"""
                )
                with self.sqlClient.connect() as conn:
                    conn.execute(
                        sqlQuery,
                        [
                            error,
                            self.transactionUid

                        ],
                    )

            elif newState == 'outOfWorkingQ':
                sqlQuery = (
                    """ UPDATE """ + self.qTable + """ 
                    SET out_of_working_q_at = %s
                    WHERE q_uid = %s"""
                )
                with self.sqlClient.connect() as conn:
                    conn.execute(
                        sqlQuery,
                        [
                            datetime.now(timezone.utc),
                            self.transactionUid

                        ],
                    )


class QueueCopyToSql:
    def __init__(self, sqlClient, redisClient, queueName, batchSize=100, selectionA=1):
        self.sqlClient = sqlClient
        self.redisClient = redisClient
        self.queueName = queueName
        self.batchSize = batchSize
        self.selectionA = selectionA

        # self.goToWork()

    # def goToWork(self, forHowLong=30):

    #    qLen = self.redisClient.llen(self.queueName)
    #    qList = self.redisClient.lrange(self.queueName, 0, qLen)
    #    lenqlist = len(qList)


class CypherTransactionBlockWorker:
    def __init__(self, neoDriver, cypherQueues, sqlClient, pubsub):
        self.createdAt = datetime.now(timezone.utc)
        self.neoDriver = neoDriver
        self.cypherQueues = cypherQueues
        self.sqlClient = sqlClient
        self.pubsub = pubsub

    def goToWork(self, forHowLong=60, inactivityBuffer=5):
        print(f'XXX goToWork. ForHowLong={forHowLong}')
        startTs = datetime.now(timezone.utc)
        i = 0
        howLong = 0
        queuesAreEmpty = False
        while howLong <= forHowLong - inactivityBuffer and not queuesAreEmpty:
            i += 1
            self.cypherQueues.getQLens()

            if self.cypherQueues.totalInWorkingQueue >= 10:
                self.lookForExpiredWorkingBlocks()

            ctb = self.popBlockFromWaitingQueues()

            if ctb:
                stm = ctb.statements
                print(f'XXX got CTB from Q statements =  {stm}')
                print(ctb.transactionUid)
                self.executeBlock(ctb)
            else:
                queuesAreEmpty = True
                self.lookForExpiredWorkingBlocks()

            howLong = um.dateDiff('sec', startTs, datetime.now(timezone.utc))
            print(f'Running for how long: {howLong}')

        if howLong >= forHowLong - inactivityBuffer and self.cypherQueues.totalInWaitingQueues > 0:
            numFlares = self.cypherQueues.totalInWaitingQueues / 10

            for k in range(int(numFlares)):
                print(f'sending flare {k}')
                self.pubsub.publish_message('awaken')

    def popBlockFromWaitingQueues(self):
        """
        Fetches (LPOP) the next ctb from the queues
        """
        print(f'XXX popBlockFromWaitingQueues:')
        popped = self.cypherQueues.redisClient.blpop(
            self.cypherQueues.cQNames, 1)
        # popped = self.redisClient.lpop(self.queueName)

        if not popped:
            print("QUEUES ARE EMPTY_________________________________________")
        else:
            dataFromRedis = json.loads(popped[1], cls=um.RoundTripDecoder)

            print(f"Data read from waitingQ:{dataFromRedis}")
            ctb = CypherTransactionBlock(
                priority=None, statements=None, transactionUid=None, origin=None, sqlClient=self.sqlClient)
            ctb.makeFromSerial(dataFromRedis)
            if ctb.statements:
                ctb.lastUpdatedAt = datetime.now(timezone.utc)
                ctb.setJson()
                self.pushBlockToWorkingQueue(ctb)
            return ctb

    def pushBlockToWorkingQueue(self, ctb, isLeft=False):
        self.cypherQueues.pushCtbToWorkingQ(ctb, isLeft)

    def removeBlockFromWorkingQueue(self, ctbSerial):
        rem = self.cypherQueues.removeCtbFromWorkingQ(ctbSerial)
        print(f' {rem} instances removed from workingQ')
        return rem

    def popBlockFromWorkingQueue(self):
        ser = self.cypherQueues.popCtbSerialFromWorkingQ()
        if ser:
            ctb = CypherTransactionBlock(
                priority=None, statements=None, transactionUid=None, origin=None, sqlClient=self.sqlClient)
            ctb.makeFromSerial(ser)
            return ctb
        return None

    def copyBlockFromWorkingQueue(self):
        ser = self.cypherQueues.copyCtbSerialFromWorkingQ()
        if ser:
            ctb = CypherTransactionBlock(
                priority=None, statements=None, transactionUid=None, origin=None, sqlClient=self.sqlClient)
            ctb.makeFromSerial(ser)
            return ctb
        return None

    def lookForExpiredWorkingBlocks(self, expiryInSeconds=60):
        wb = self.copyBlockFromWorkingQueue()
        if wb:
            matchingSerial = json.dumps(
                wb.instanceToSerial(), cls=um.RoundTripEncoder)

            if wb.statements:

                timeDiff = um.dateDiff('s', wb.lastUpdatedAt,
                                       datetime.now())
                print(f' Age of workingQ item ={timeDiff}')
                if timeDiff >= expiryInSeconds:
                    stmt = wb.statements
                    print(
                        f'Picking up item from workingQ. Statements = {stmt}')

                    rem = self.removeBlockFromWorkingQueue(matchingSerial)
                    if rem > 0:
                        wb.registerChangeInSql('outOfWorkingQ')
                        wb.registerChangeInSql('RecycledToWaiting')
                        wb.lastUpdatedAt = datetime.now()
                        wb.numRetries += 1
                        self.cypherQueues.pushCtbToWaitingQ(
                            ctBlock=wb, jumpTheQ=True)

    def executeBlock(self, ctBlock: CypherTransactionBlock):
        """
        Executes all statments in the ctb as one transaction.
        Returns success boolean
        """
        matchingSerial = json.dumps(
            ctBlock.instanceToSerial(), cls=um.RoundTripEncoder)
        if ctBlock.statements:
            try:
                ctBlock.registerChangeInSql('executeStart')
                with self.neoDriver.session() as session:
                    startTs = datetime.now(timezone.utc)
                    print('to NEO: {}'.format(ctBlock.statements))
                    _, durations = session.write_transaction(
                        self._statementsAsTransaction, ctBlock.statements)

                    print('back from  NEO')
                    endTs = datetime.now(timezone.utc)
                    elapsedSec = um.dateDiff('sec', startTs, endTs)
                    ctBlock.runTime = elapsedSec
                    ctBlock.durations = durations
                    ctBlock.status = 'done'
                    ctBlock.registerChangeInSql('executeEnd')

                    rem = self.removeBlockFromWorkingQueue(matchingSerial)
                    if rem > 0:
                        ctBlock.registerChangeInSql('outOfWorkingQ')
                    self.cypherQueues.pushCtbToCompletedQ(ctBlock)

                    return True
            except CypherSyntaxError as e:
                print('CypherSyntaxError')
                logging.error(repr(e))
                ctBlock.numRetries += 1
                ctBlock.status = 'CypherSyntaxError'
                ctBlock.errors = repr(e)
                rem = self.removeBlockFromWorkingQueue(matchingSerial)
                if rem > 0:
                    ctBlock.registerChangeInSql('outOfWorkingQ')
                self.cypherQueues.pushCtbToCompletedQ(ctBlock)
                ctBlock.registerChangeInSql('error', repr(e))
                return False

            except ServiceUnavailable as e:
                print('ServiceUnavailable')
                logging.error(repr(e))
                ctBlock.numRetries += 1
                ctBlock.status = 'ServiceUnavailable'
                ctBlock.errors = repr(e)
                rem = self.removeBlockFromWorkingQueue(matchingSerial)
                if rem > 0:
                    ctBlock.registerChangeInSql('outOfWorkingQ')
                self.cypherQueues.pushCtbToWaitingQ(ctBlock)
                ctBlock.registerChangeInSql('error', repr(e))
                return False

            except ClientError as e:
                print('ClientError')
                logging.error(repr(e))
                ctBlock.numRetries += 1
                ctBlock.status = 'ClientError'
                ctBlock.errors = repr(e)
                rem = self.removeBlockFromWorkingQueue(matchingSerial)
                if rem > 0:
                    ctBlock.registerChangeInSql('outOfWorkingQ')
                ctBlock.setJson()
                self.cypherQueues.pushCtbToCompletedQ(ctBlock)
                return False

            except Exception as e:
                logging.error(repr(e))
                rem = self.removeBlockFromWorkingQueue(matchingSerial)
                if rem > 0:
                    ctBlock.registerChangeInSql('outOfWorkingQ')
                self.cypherQueues.pushCtbToWaitingQ(ctBlock)
                return False

    def _statementsAsTransaction(self, tx, statements):
        results = []
        duration = []
        statementList = []
        for statement in statements:
            try:
                startTs = datetime.now(timezone.utc)
                if "batch" in statement:
                    res = tx.run(statement["cypher"],
                                 statement["parameters"],
                                 batch=statement["batch"])
                else:
                    res = tx.run(statement["cypher"],
                                 statement["parameters"])
                results.append(res)

                endTs = datetime.now(timezone.utc)
                duration.append(um.dateDiff('sec', startTs, endTs))
                statementList.append({"cypher": statement["cypher"], "parameters": statement["parameters"], "duration": um.dateDiff('sec', startTs, endTs),
                                      "status": "OK", "error": None})
            except Exception as e:
                endTs = datetime.now(timezone.utc)
                duration.append(um.dateDiff('sec', startTs, endTs))
                logging.error(repr(e))
                statementList.append({"cypher": statement["cypher"], "parameters": statement["parameters"], "duration": um.dateDiff('sec', startTs, endTs),
                                      "status": "ERROR", "error": repr(e)})

                return False

        return results, statementList