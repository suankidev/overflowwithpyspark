import sqlite3

"""
creating context manager for slq lit so, that we don't need to 
each time write connection and execute statment

"""


class DatabaseConnection:

    def __init__(self,host):
        self.connection = None
        self.host = host

    def __enter__(self) -> sqlite3.Connection:
        self.connection = sqlite3.connect(self.host)
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        """exc_type  --> exception type
        exc_val ---> exception value
        exc_tb   --> exception traceback
        by defaul all of thema is null"""
        if exc_val or exc_type or exc_tb:
            self.connection.close()
        else:
            self.connection.commit()
            self.connection.close()


