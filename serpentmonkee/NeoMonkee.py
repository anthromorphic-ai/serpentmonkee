

import logging

from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable


class NeoMonkee:  # --------------------------------------------------------------------
    def __init__(self, neoDriver):
        self.neoDriver = neoDriver

    def makeNeoDriver(self, neo_uri, neo_user, neo_pass):
        if neo_uri is not None:
            self.neoDriver = GraphDatabase.driver(
                uri=neo_uri,
                auth=basic_auth(
                    user=neo_user,
                    password=neo_pass,
                ),
                max_connection_lifetime=200,
                # encrypted=True,
            )

    def readResults(self, query, **params):
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
        with self.neoDriver.session() as session:
            result = session.read_transaction(self._inner, query, **params,)
            return result

    def writeResults(self, query, **params):
        with self.neoDriver.session() as session:
            result = session.write_transaction(self._inner, query, **params,)
            return result

    def _inner(self, tx, query, params):
        result = tx.run(
            query, params)

        try:
            return [row for row in result]
        except ServiceUnavailable as e:
            logging.error(repr(e))
            raise

    def writeResultsBatch(self, query, batch, **params):
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
        with self.neoDriver.session() as session:
            write_results = session.write_transaction(
                self._innerBatch, query, batch, **params
            )
        return write_results

    def _innerBatch(self, tx, query, batch, params):
        result = tx.run(
            query,  params, batch=batch,)

        try:
            return [row for row in result]
        except ServiceUnavailable as e:
            logging.error(repr(e))
            raise
