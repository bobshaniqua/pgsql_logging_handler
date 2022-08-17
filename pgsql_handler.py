import psycopg2
import logging
import time
from platform import uname
from datetime import datetime
from urllib.parse import urlparse
from logging import Handler, StreamHandler

TEST_URL = "postgres://postgres:password@192.168.50.1:5432/database"

"""
postgresql::/[user:pass@][hostspec][/dbname][?paramspec]
psql -U 'postgres' -h '192.168.50.1' -d 'hmb' -c 
"""


class PostgresHandler(Handler):
    def __init__(self, url, schema: str, table: str) -> None:
        """
        Custom logging handler for submitting log messages to a PostgreSQL database.
        The handler requires a URL for the database, as well as the schema and table name.
        If the specified table does not exist, one will be created with the required columns.
        The table requires a specific format of columns: (time, source, alert_level, message).

        Postgres table format:
         - time = Time log was created in the format "%Y-%m-%d %T" e.g. 2022-08-17 10:33:20
         - source = Hostname of device
         - alert_level = Alert level of the log e.g. 'INFO', 'WARNING'
         - message = Message submitted with the log
        :param url: URL to the postgres database e.g. postgres://user:pass@host:port/database
        :param schema: Schema name
        :param table: Table name
        """
        Handler.__init__(self)

        self.schema = schema
        self.table = table

        try:
            parsed = urlparse(url)
        except Exception as parse_err:
            raise parse_err

        if parsed.scheme != 'postgresql' and parsed.scheme != 'postgres':
            raise ValueError(f"Database URL must have the scheme 'postgres' or 'postgresql', not '{parsed.scheme}'.")

        try:
            self.conn = psycopg2.connect(url)
        except Exception as conn_err:
            raise conn_err
        else:
            self.cur = self.conn.cursor()

    def emit(self, record):
        """
        We don't format records here
        :param record:
        :return:
        """
        timestamp = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %T")
        source = uname()[1]
        message = record.msg
        alert_level = record.levelname

        default_query = f"INSERT INTO {self.schema}.{self.table}(time, source, alert_level, message)" \
                        f"VALUES ('{timestamp}', '{source}', '{alert_level}', '{message}')"

        try:
            self.cur.execute(default_query)
            self.conn.commit()
        except Exception as err:
            raise err

    def close(self):
        """
        Close the handler & connection to the postgres database
        :return:
        """
        try:
            self.cur.close()
            self.conn.close()
        except AttributeError:
            pass
        except Exception as err:
            print(err)
        finally:
            logging.Handler.close(self)


if __name__ == "__main__":
    # Set up the logger
    logger = logging.getLogger("hmb2_log")
    logger.addHandler(PostgresHandler(schema="hmb_data", table="system_logs", url=TEST_URL))
    f_format = logging.FileHandler("local.log")
    f_format.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(f_format)
    logger.setLevel("DEBUG")

    # Do some logging
    logger.warning("This is a python logging test")
    logger.critical("This is a critical")
    logger.error("This is an alert")
    logger.debug("This is a debug")
    logger.info("This is an info")
