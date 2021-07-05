# _METADATA_:Version: 20
# _METADATA_:Timestamp: 2021-01-17 21:26:27.469550+00:00
# _METADATA_:MD5: 1d26a56d8a45619d63b5715fb215c706
# _METADATA_:Publish:                                                                      None


# _METADATA_:
import logging
from datetime import datetime, timedelta, timezone
from neo4j import GraphDatabase, basic_auth, __version__ as neoVersion
from neo4j.exceptions import ServiceUnavailable
import uuid
import redis

from serpentmonkee.PubSubMonkee import PubSubMonkee
from serpentmonkee.CypherQueue import CypherQueue, CypherQueues
from serpentmonkee.CypherTransaction import CypherTransactionBlock, CypherTransactionBlockWorker
from serpentmonkee.MonkeeSQLblocks import MonkeeSQLblock, MonkeeSQLblockHandler


class NeoMonkee:  # --------------------------------------------------------------------

    def __init__(self, neoDriver, redisClient, publisher, projectId, topicId, sqlTable, callingCF=None):
        self.neoDriver = neoDriver
        self.driverUuid = None
        self.driverStartedAt = None
        self.sqlTable = sqlTable
        self.callingCF = callingCF
        self.redisClient = redisClient
        self.db_fb = None
        self.cypherQueues = self.makeCypherQueues()
        self.asyncStatements = []
        self.pubsub = PubSubMonkee(publisher, projectId, topicId)
        self.cypherWorker = CypherTransactionBlockWorker(
            self.neoDriver, self.cypherQueues, pubsub=self.pubsub, redisClient=redisClient, environmentName=projectId)

        self.sqlBlockHandler = MonkeeSQLblockHandler(environmentName=projectId,
                                                     redis_client=redisClient,
                                                     pubsub=publisher)
        # self.sqlBlockHandler.killQueue()

        # self.sqlWorker = MonkeeSQLblockWorker(environmentName=projectId,
        #                                      sqlClient=sqlClient,
        #                                      sqlBHandler=self.sqlBlockHandler)

        """
        sqlb = MonkeeSQLblock(query=sqlInsertQuery,
                                  insertList=[],
                                  queryTypeId='humans')
        self.sqlBlockHandler.toQ(sqlb)
        """

    def makeCypherQueues(self):
        cQH = CypherQueue("cypherQ_High")
        cQM = CypherQueue("cypherQ_Medium")
        cQL = CypherQueue("cypherQ_Low")
        wQ = CypherQueue("cypherWorking")
        compQ = CypherQueue("cypherDone")
        queues = [cQH, cQM, cQL]
        return CypherQueues(redisClient=self.redisClient,
                            cQueues=queues, workingQ=wQ, completedQ=compQ, fb_db=self.db_fb)

    def get_uuid(self):
        return str(uuid.uuid4())

    def makeNeoDriver(self, neo_uri, neo_user, neo_pass):
        if neo_uri is not None:
            self.driverUuid = self.get_uuid()
            self.driverStartedAt = datetime.now(timezone.utc)
            if neoVersion[0] == '4':
                self.neoDriver = GraphDatabase.driver(
                    uri=neo_uri,
                    auth=basic_auth(
                        user=neo_user,
                        password=neo_pass,
                    ),
                    max_transaction_retry_time=2
                    # max_connection_lifetime=200,
                    # encrypted=True,
                )
            if neoVersion[0] == '1':
                self.neoDriver = GraphDatabase.driver(
                    uri=neo_uri,
                    auth=basic_auth(
                        user=neo_user,
                        password=neo_pass,
                    ),
                    # max_connection_lifetime=200,
                    encrypted=True,
                    max_retry_time=2)

    def syncRead(self, query, cfInstanceUid='', **params):
        """
        Reads the results of a cypher query.

            USAGE:

            query = '''
                    MATCH (h:nodeType {_project: $projectname })
                    return h.uid as uid
                    '''

            params = {'projectname': 'Sandbox'}
            res = neomnkee.syncRead(query=query, params=params)
            for r in res:
                print(r['uid'])
        """
        start_ts = datetime.now(timezone.utc)
        with self.neoDriver.session() as session:
            result = session.read_transaction(
                self._inner,
                query,
                cfInstanceUid,
                **params,
            )

        end_ts = datetime.now(timezone.utc)
        self.saveToSql(proc_name='syncRead',
                       start_ts=start_ts,
                       end_ts=end_ts,
                       cypher=query,
                       params=str(params),
                       batch=str([]),
                       cfInstanceUid=cfInstanceUid)
        return result

    def asyncWriteStatement(self, query, batch=[], cfInstanceUid='', **params):

        if batch == []:
            self.asyncStatements.append(
                {"cypher": query, "parameters": params['params'], "ts": datetime.now(timezone.utc)})
        else:
            self.asyncStatements.append(
                {"cypher": query, "parameters": params['params'], "batch": batch, "ts": datetime.now(timezone.utc)})

    def asyncWrite(self, priority="M", appUid=None, docUid=None):
        """
        batches all the async statements posted so far and sends it off to the cypherQueue identified by priority
        """
        if self.asyncStatements == []:
            print('XXX no statements to action')
        else:
            guid = self.get_uuid()

            ctb = CypherTransactionBlock(priority=priority, statements=self.asyncStatements,
                                         transactionUid=guid, callingCF=self.callingCF, originDocUid=docUid, appUid=appUid, sqlBlockHandler=self.sqlBlockHandler)
            self.cypherQueues.pushCtbToWaitingQ(ctb)
            # Resets the asyncStatements: this "asyncWrite" is seen as a push the statements to queue and start with a clean list
            self.asyncStatements = []
            self.cypherQueues.getQLens()

            # Sends a pubsub message to start the cypher worker CF
            self.pubsub.publish_message('awaken')

    def syncWrite(self, query, cfInstanceUid='', **params):
        start_ts = datetime.now(timezone.utc)
        result = None
        with self.neoDriver.session() as session:
            result = session.write_transaction(self._inner, query,
                                               cfInstanceUid, **params)

        end_ts = datetime.now(timezone.utc)
        self.saveToSql(proc_name='syncWrite',
                       start_ts=start_ts,
                       end_ts=end_ts,
                       cypher=query,
                       params=str(params),
                       batch=str([]),
                       cfInstanceUid=cfInstanceUid)
        return result

    def _inner(self, tx, query, cfInstanceUid, params):
        result = tx.run(query, params)

        try:
            return [row for row in result]
        except ServiceUnavailable as e:
            self.saveToSql('_inner', None, None, repr(e),
                           'ERROR: ServiceUnavailable', None, cfInstanceUid)
            logging.error(repr(e))
            raise
        except Exception as e:
            self.saveToSql('_inner', None, None, repr(e), 'ERROR: Other', None,
                           cfInstanceUid)
            logging.error(repr(e))
            raise

    def syncWriteBatch(self, query, batch, cfInstanceUid='', **params):
        """
        Runs a batch update.

            USAGE:
            batch = ['01602cb23de544e8b33c4612810e96a5',
                '016a8a2f414c43b49b656b655da07fbe',
                '01f62dc44f6d4e85b0c2a7a973f97750']
            query = '''
                    UNWIND $batch AS hB
                    MATCH (h:humans { uid: hB, _project: $projectname })
                    set h.val = 314159
                    return h.uid as uid
                    '''

            params = {'projectname': 'Sandbox'}
            res = neomnkee.syncWriteBatch(
                query=query, params=params, batch=batch)
            for r in res:
                print(r['uid'])
        """
        write_results = None
        start_ts = datetime.now(timezone.utc)
        with self.neoDriver.session() as session:
            write_results = session.write_transaction(
                self._innerBatch,
                query,
                batch,
                cfInstanceUid,
                **params,
            )

        end_ts = datetime.now(timezone.utc)
        self.saveToSql(proc_name='syncWriteBatch',
                       start_ts=start_ts,
                       end_ts=end_ts,
                       cypher=query,
                       params=str(params),
                       batch=str(batch),
                       cfInstanceUid=cfInstanceUid)
        return write_results

    def _innerBatch(self, tx, query, batch, cfInstanceUid, params):
        result = tx.run(
            query,
            params,
            batch=batch,
        )

        try:
            return [row for row in result]
        except ServiceUnavailable as e:
            logging.error(repr(e))
            self.saveToSql('_innerBatch', None, None, repr(e),
                           'ERROR: ServiceUnavailable', None, cfInstanceUid)
            raise
        except Exception as e:
            self.saveToSql('_inner', None, None, repr(e), 'ERROR: Other', None,
                           cfInstanceUid)
            logging.error(repr(e))
            raise

    def saveToSql(self, proc_name, start_ts, end_ts, cypher, params, batch,
                  cfInstanceUid):
        """inserts the payloads to the necessary SQL tables"""
        try:

            timeFormat = "%Y-%m-%d, %H:%M:%S"
            duration = None

            if start_ts is not None and end_ts is not None:
                timeDiff = end_ts - start_ts
                duration = timeDiff.total_seconds()
            else:
                start_ts = datetime.now(timezone.utc)
                end_ts = datetime.now(timezone.utc)

            sqlInsertQuery = """ INSERT INTO """ + self.sqlTable + """(procedure_name,query_start,query_end,query_duration_in_s,cypher,params,batch,connector_uid,connector_start_time,calling_cf,cf_instance_uid)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            sqlInsertList = [
                proc_name,
                start_ts.strftime(timeFormat),
                end_ts.strftime(timeFormat), duration, cypher,
                params,
                str(batch), self.driverUuid, self.driverStartedAt,
                self.callingCF, cfInstanceUid
            ]

            sqlb = MonkeeSQLblock(query=sqlInsertQuery,
                                  insertList=sqlInsertList,
                                  queryTypeId='')
            self.sqlBlockHandler.toQ(sqlb, priority='H')

        except Exception as e:
            logging.error("saveToSql: {}".format(repr(e)))
            raise
