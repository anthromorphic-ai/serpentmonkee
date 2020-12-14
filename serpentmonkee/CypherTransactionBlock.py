import requests
from dateutil import parser
import json
from datetime import datetime, timezone
import time
import sys
import random
import uuid
import copy
from serpentmonkee import UtilsMonkee as um
from neo4j.exceptions import CypherSyntaxError, ServiceUnavailable, ClientError

# --------------------------------------------------------------------


class CypherTransactionBlock:
    def __init__(self,
                 priority=None,
                 statements=None,
                 transactionUid=None,
                 origin=None):
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

        self.setJson()

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
            "errors": self.errors
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
        self.setJson()


class CypherTransactionBlockWorker:
    def __init__(self, neoMonkee, cypherQueues):
        self.createdAt = datetime.now(timezone.utc)
        self.neoMonkee = neoMonkee
        self.cypherQueues = cypherQueues

    def popBlockFromWaitingQueues(self):
        """
        Fetches (LPOP) the next ctb from the queues
        """
        popped = self.cypherQueues.redisClient.blpop(
            self.cypherQueues.cQNames, 0.01)
        # popped = self.redisClient.lpop(self.queueName)

        if not popped:
            print("QUEUES ARE EMPTY_________________________________________")
        else:
            dataFromRedis = json.loads(popped[1], cls=um.RoundTripDecoder)

            # print(f"Data read from Q:{dataFromRedis}")
            ctb = CypherTransactionBlock(None, None, None, None)
            ctb.makeFromSerial(dataFromRedis)
            if ctb.statements:
                ctb.lastUpdatedAt = datetime.now(timezone.utc)
                ctb.setJson()
                self.pushBlockToWorkingQueue(ctb.instanceToSerial())
            return ctb

    def pushBlockToWorkingQueue(self, ctbSerial, isLeft=False):
        self.cypherQueues.pushCtbToWorkingQ(ctbSerial, isLeft)

    def removeBlockFromWorkingQueue(self, ctbSerial):
        rem = self.cypherQueues.removeCtbFromWorkingQ(ctbSerial)
        print(f' {rem} instances removed from workingQ')

    def popBlockFromWorkingQueue(self):
        ser = self.cypherQueues.popCtbSerialFromWorkingQ()
        if ser:
            ctb = CypherTransactionBlock(None, None, None, None)
            ctb.makeFromSerial(ser)
            return ctb
        return None

    def lookForExpiredWorkingBlocks(self, expiryInSeconds=90):
        wb = self.popBlockFromWorkingQueue()
        if wb:

            if wb.statements:

                timeDiff = um.dateDiff('s', wb.lastUpdatedAt,
                                       datetime.now())
                print(f' Age of workingQ item={timeDiff}')
                if timeDiff >= expiryInSeconds:
                    # self.removeBlockFromWorkingQueue(wb.instanceToSerial())
                    wb.lastUpdatedAt = datetime.now()
                    wb.numRetries += 1
                    self.cypherQueues.pushCtbToWaitingQ(wb)
                else:
                    wb.setJson()
                    wb_serial = wb.instanceToSerial()
                    self.pushBlockToWorkingQueue(wb_serial, isLeft=True)

    def executeBlock(self, ctBlock: CypherTransactionBlock):
        """
        Executes all statments in the ctb as one transaction.
        Returns success boolean
        """
        if ctBlock.statements:
            try:
                with self.neoMonkee.neoDriver.session() as session:
                    startTs = datetime.now(timezone.utc)
                    results = session.write_transaction(
                        self._statementsAsTransaction, ctBlock.statements)
                    print(results)
                    endTs = datetime.now(timezone.utc)
                    elapsedSec = um.dateDiff('sec', startTs, endTs)
                    ctBlock.runTime = elapsedSec
                    ctBlock.status = 'done'

                    self.removeBlockFromWorkingQueue(
                        ctBlock.instanceToSerial())
                    self.cypherQueues.pushCtbToCompletedQ(ctBlock)
                    return True
            except CypherSyntaxError as e:
                print('CypherSyntaxError')
                print(repr(e))
                ctBlock.numRetries += 1
                ctBlock.status = 'CypherSyntaxError'
                ctBlock.errors = repr(e)
                self.removeBlockFromWorkingQueue(ctBlock.instanceToSerial())
                self.cypherQueues.pushCtbToCompletedQ(ctBlock)

                return False
            except ServiceUnavailable as e:
                print('ServiceUnavailable')
                print(repr(e))
                ctBlock.numRetries += 1
                ctBlock.status = 'ServiceUnavailable'
                ctBlock.errors = repr(e)
                self.removeBlockFromWorkingQueue(ctBlock.instanceToSerial())
                self.cypherQueues.pushCtbToWaitingQ(ctBlock)
                return False
            except ClientError as e:
                print('ClientError')
                print(repr(e))
                ctBlock.numRetries += 1
                ctBlock.status = 'ClientError'
                ctBlock.errors = repr(e)
                self.removeBlockFromWorkingQueue(ctBlock.instanceToSerial())
                self.cypherQueues.pushCtbToCompletedQ(ctBlock)
                return False
            """except Exception as e:
                print(repr(e))
                self.removeBlockFromWorkingQueue(ctBlock.instanceToSerial())
                self.cypherQueues.pushCtbToWaitingQ(ctBlock)
                return False"""

    def _statementsAsTransaction(self, tx, statements):
        results = []
        for statement in statements:
            results.append(tx.run(statement["cypher"],
                                  statement["parameters"]))
        return results
