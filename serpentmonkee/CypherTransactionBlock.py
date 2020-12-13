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
from neo4j.exceptions import CypherSyntaxError

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

        self.setJson()

    def setJson(self):
        self.json = {
            "priority": self.priority,
            "numRetries": self.numRetries,
            "createdAt": self.createdAt,
            "lastUpdatedAt": self.lastUpdatedAt,
            "uid": self.transactionUid,
            "origin": self.origin,
            "statements": self.statements
        }

    def instanceToSerial(self):
        return json.dumps(self.json, cls=um.RoundTripEncoder)

    def makeFromSerial(self, serial):
        dict_ = json.loads(serial, cls=um.RoundTripDecoder)
        self.priority = um.getval(dict_, "priority")
        self.numRetries = um.getval(dict_, "numRetries")
        self.createdAt = um.getval(dict_, "createdAt")
        self.lastUpdatedAt = um.getval(dict_, "lastUpdatedAt")
        self.transactionUid = um.getval(dict_, "uid")
        self.origin = um.getval(dict_, "origin")
        self.statements = um.getval(dict_, "statements")


class CypherTransactionBlockWorker:
    def __init__(self, redisClient, neoMonkee):
        self.createdAt = datetime.now(timezone.utc)
        self.redisClient = redisClient
        self.neoMonkee = neoMonkee

    def executeBlock(self, ctBlock):
        """
        Executes all statments in the ctb as one transaction.
        Returns success boolean
        """
        try:
            with self.neoMonkee.neoDriver.session() as session:
                results = session.write_transaction(
                    self._statementsAsTransaction, ctBlock.statements)
                print(results)
                return True
        except CypherSyntaxError as e:
            print('CypherSyntaxError')
            print(repr(e))
            return False
        except Exception as e:
            print(repr(e))
            return False

    def _statementsAsTransaction(self, tx, statements):
        results = []
        for statement in statements:
            results.append(tx.run(statement["cypher"],
                                  statement["parameters"]))

        return results
