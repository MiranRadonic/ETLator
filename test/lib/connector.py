class Connector(object):
    """ Abstract class for different connectors. """

    def copy(self):
        pass

    def getConnection(self):
        return self.copy()

    def connect(self, connection_params=None):
        pass

    def cursor(self):
        if self.conn is None:
            raise NoConnectionError
        try:
            return self.conn.cursor()
        except:
            raise ConnectorError

    def commit(self):
        if self.conn is None:
            raise NoConnectionError
        try:
            self.conn.commit()
        except:
            raise ConnectorError("Can't commit")

    def rollback(self):
        if self.conn is None:
            raise NoConnectionError
        try:
            self.conn.rollback()
        except:
            raise ConnectorError("Can't rollback")

    def close(self):
        if self.conn is None:
            raise NoConnectionError
        try:
            self.conn.close()
        except:
            raise ConnectorError("Can't close connection")


class ConnectorError(Exception):
    """ Generic error of the Connector. """

    def __init__(self, arg = None):
      self.args = [arg]


class NoConnectionError(ConnectorError):
    """ The connection doesn't exist. Use connect() method. """

    def __init__(self, arg = None):
      self.args = [arg]


class PosgresqlConnector(Connector):
    """ Connector for posgresql database. Uses Psycopg2. """

    def __init__(self, connection_string = None):
        self.psycopg2 = __import__('psycopg2')
        if connection_string is not None:
            self.connection_string = connection_string
            self.connect(connection_string)

    def copy(self):
        return PosgresqlConnector(self.connection_string)

    def connect(self, connection_string = None):
        if connection_string is not None:
            self.connection_string = connection_string
        if self.connection_string is None:
            raise ConnectorError("Connection string is None")
        try:
            self.conn = self.psycopg2.connect(self.connection_string)
        except:
            raise ConnectorError("Can't connect")


class SQLiteConnector(Connector):
    """ Connector for SQLite database. Uses built-in module sqlite3. """

    def __init__(self, connection_string = None):
        self.sqlite3 = __import__('sqlite3')
        if connection_string is not None:
            self.connection_string = connection_string
            self.connect(connection_string)

    def copy(self):
        return SQLiteConnector(self.connection_string)

    def connect(self, connection_string = None):
        if connection_string is not None:
            self.connection_string = connection_string
        if self.connection_string is None:
            raise ConnectorError("Connection string is None")
        try:
            self.conn = self.sqlite3.connect(self.connection_string)
        except:
            raise ConnectorError("Can't connect")


class MySQLConnector(Connector):
    """ Connector for MySQL database. Uses mysql. """

    def __init__(self, connection_params = None):
        self.mysql_connector = __import__('mysql.connector')
        if connection_params is not None:
            self.connection_params = connection_params
            self.connect(connection_params)

    def copy(self):
        return MySQLConnector(self.connection_params)

    def connect(self, connection_params = None):
        if connection_params is not None:
            self.connection_params = connection_params
        if self.connection_params is None:
            raise ConnectorError("Connection string is None")
        try:
            self.conn = self.mysql_connector.connector.connect(host=self.connection_params['host'], user=self.connection_params['user'], passwd=self.connection_params['passwd'], db=self.connection_params['db'])
            self.conn.cursor().execute('SET foreign_key_checks = 0')
        except Exception as e:
            raise ConnectorError("Can't connect " + e.message)


class SQLServerConnector(Connector):
    """ Connector for Microsoft SQL Server database. Uses pymssql. """

    def __init__(self, connection_params = None):
        self.mysql_connector = __import__('pymssql')
        if connection_params is not None:
            self.connection_params = connection_params
            self.connect(connection_params)

    def copy(self):
        return SQLServerConnector(self.connection_params)

    def connect(self, connection_params = None):
        if connection_params is not None:
            self.connection_params = connection_params
        if self.connection_params is None:
            raise ConnectorError("Connection string is None")
        try:
            self.conn = self.mysql_connector.connector.connect(host=self.connection_params['host'], user=self.connection_params['user'], password=self.connection_params['password'], database=self.connection_params['database'])
        except Exception as e:
            raise ConnectorError("Can't connect " + e.message)
