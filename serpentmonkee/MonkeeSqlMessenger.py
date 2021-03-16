# _METADATA_:Version: 11
# _METADATA_:Timestamp: 2021-03-12 01:41:55.901362+00:00
# _METADATA_:MD5: 581db14430c1ab0093535bd6fe9e5c9d
# _METADATA_:Publish:                                                                 None
# _METADATA_:
from datetime import datetime, timedelta, timezone
import random
# import monkee_utilities as mu
from serpentmonkee import UtilsMonkee as mu
import sqlalchemy
from sqlalchemy.sql.expression import bindparam
import json
import logging
import time


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
        self.topic_path = self.pubsub.topic_path(
            self.environmentName, self.topic_id)

    def sendFlare(self, messageData='awaken'):
        data = messageData.encode("utf-8")
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
        # self.sendFlare()

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
            query,
            insertList=[],
            queryTypeId=None,
            numRetries=0,
            maxRetries=30):

        self.query = query
        self.insertList = insertList
        self.createdAt = datetime.now(timezone.utc)
        self.queryTypeId = queryTypeId
        self.numRetries = numRetries
        self.maxRetries = maxRetries

    def instanceToSerial(self):
        return {"query": self.query, "insertList": self.insertList, "queryTypeId": self.queryTypeId, "numRetries": self.numRetries, "maxRetries": self.maxRetries}

    def retryAgain(self):
        return self.numRetries <= self.maxRetries
