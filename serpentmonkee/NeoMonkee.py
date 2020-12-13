import logging
from datetime import datetime, timedelta, timezone
from neo4j import GraphDatabase, basic_auth, __version__ as neoVersion
from neo4j.exceptions import ServiceUnavailable
import uuid


class NeoMonkee:  # --------------------------------------------------------------------
    def __init__(self, neoDriver, sqlClient=None, callingCF=None):
        self.neoDriver = neoDriver
        self.driverUuid = None
        self.driverStartedAt = None
        self.sqlClient = sqlClient
        self.callingCF = callingCF

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
                    #max_connection_lifetime=200,
                    # encrypted=True,
                )
            if neoVersion[0] == '1':
                self.neoDriver = GraphDatabase.driver(
                    uri=neo_uri,
                    auth=basic_auth(
                        user=neo_user,
                        password=neo_pass,
                    ),
                    #max_connection_lifetime=200,
                    encrypted=True,
                    max_retry_time=2)

    def readResults(self, query, cfInstanceUid='', **params):
        """
        Reads the results of a cypher query.

            USAGE:

            query = '''
                    MATCH (h:humans {_project: $projectname })
                    return h.uid as uid
                    '''

            params = {'projectname': 'Sandbox'}
            res = neomnkee.readResults(query=query, params=params)
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
        self.saveToSql(proc_name='readResults',
                       start_ts=start_ts,
                       end_ts=end_ts,
                       cypher=query,
                       params=str(params),
                       batch=str([]),
                       cfInstanceUid=cfInstanceUid)
        return result

    def writeResults(self, query, cfInstanceUid='', **params):
        start_ts = datetime.now(timezone.utc)
        result = None
        with self.neoDriver.session() as session:
            result = session.write_transaction(self._inner, query,
                                               cfInstanceUid, **params)

        end_ts = datetime.now(timezone.utc)
        self.saveToSql(proc_name='writeResults',
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

    def writeResultsBatch(self, query, batch, cfInstanceUid='', **params):
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
            res = neomnkee.writeResultsBatch(
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
        self.saveToSql(proc_name='writeResultsBatch',
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
            if self.sqlClient is not None:
                timeFormat = "%Y-%m-%d, %H:%M:%S"
                with self.sqlClient.connect() as conn:

                    duration = None
                    if start_ts is not None and end_ts is not None:
                        timeDiff = end_ts - start_ts
                        duration = timeDiff.total_seconds()
                    else:
                        start_ts = datetime.now(timezone.utc)
                        end_ts = datetime.now(timezone.utc)
                    sqlInsertQuery = """ INSERT INTO monkee.neo4j_queries(procedure_name,query_start,query_end,query_duration_in_s,cypher,params,batch,connector_uid,connector_start_time,calling_cf,cf_instance_uid)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """

                    conn.execute(
                        sqlInsertQuery,
                        [
                            proc_name,
                            start_ts.strftime(timeFormat),
                            end_ts.strftime(timeFormat), duration, cypher,
                            params,
                            str(batch), self.driverUuid, self.driverStartedAt,
                            self.callingCF, cfInstanceUid
                        ],
                    )
        except Exception as e:
            logging.error(repr(e))
            raise


if __name__ == '__main__':
    print(neoVersion[0])
