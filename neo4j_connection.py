from neo4j import GraphDatabase
import time


class Neo4jConnection:
    def __init__(self, uri, user, pwd):
        # self.logger = utils.init_logger("Neo4jConnection")
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(
                self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db=None):
        """ Executes a query on the Neo4j graph.

        Args:
            query (str): Cypher Query to be executed.
            parameters (dict, optional): Dictionary with the parameters to be used in the query. Defaults to None.
            db (str, optional): Database to be used. Defaults to None.

        Returns:
            _type_: _description_
        """
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(
                database=db) if db is not None else self.__driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response

    def create_constraints(self, node, attribute):
        """Creates a constraint on a Neo4j node.

        Args:
            node (str): Node to be constrained.
            attribute (str): Attribute to be constrained.
        """
        query = f"""
                CREATE CONSTRAINT ON (n:{node}) ASSERT n.{attribute} IS UNIQUE
                """
        return self.query(query)

    def insert_data(self, query, rows, batch_size=10000):
        """ Inserts data into the Neo4j graph as a batch job.

        Args:
            query (str): Query to be executed.
            rows (pandas.DataFrame): Data to be inserted.
            batch_size (int, optional): Batch size. Defaults to 10000.
        Returns:
            response: Dictionary with the total number of selfections added and the time it took to add them.
        """

        total = 0
        batch = 0
        start = time.time()
        result = None

        while batch * batch_size < len(rows):

            res = self.query(query,
                             parameters={'rows': rows[batch*batch_size:(batch+1)*batch_size].to_dict('records')})
            total += res[0]['total']
            batch += 1
            result = {"total": total,
                      "batches": batch,
                      "time": time.time()-start}
            print(result)

        return result
