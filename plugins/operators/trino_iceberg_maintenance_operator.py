from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import trino
import logging

LOGGER = logging.getLogger(__name__)


class TrinoIcebergMaintenanceOperator(BaseOperator):
    """
    An operator that executes Apache Iceberg Maintenance prodecures
    such as: Optimize, Analyze, Drop Extended Stats, Expire Snapshots, Remove Orphan Files
    using Trino as engine.

    You only need a Trino connection configured into your Airflow environment and pick a table.
    """
    def __init__(self, table_name: str, conn_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.conn_id = conn_id
        self.hook = BaseHook.get_connection(conn_id=self.conn_id)
        self.session_properties = {"use_preferred_write_partitioning": "false",
                                   "enable_stats_calculator": "false"}

    def get_trino_connection(self):
        trino_connection = trino.dbapi.connect(
            host=self.hook.host,
            http_scheme='https',
            port=self.hook.port,
            user=self.hook.login,
            auth=trino.auth.BasicAuthentication(self.hook.login,
                                                self.hook.password),
            verify=True)
        return trino_connection

    def optimize_table(self, cursor):
        optimize_command = f"alter table {self.table_name} execute optimize"
        try:
            LOGGER.info(f"OPTIMIZING TABLE {self.table_name}")
            LOGGER.info(optimize_command)
            cursor.execute(optimize_command)
        except Exception as error:
            raise error

    def expire_snapshots(self, cursor):
        expire_snapshots_command = f"""ALTER TABLE {self.table_name} 
        EXECUTE expire_snapshots(retention_threshold => '7d')"""
        try:
            LOGGER.info(f"EXPIRING SNAPSHOTS FROM TABLE {self.table_name}")
            LOGGER.info(expire_snapshots_command)
            cursor.execute(expire_snapshots_command)
        except Exception as error:
            raise error

    def remove_orphan_files(self, cursor):
        remove_orphan_files_command = f"""ALTER TABLE {self.table_name} 
        EXECUTE remove_orphan_files(retention_threshold => '7d')"""
        try:
            LOGGER.info(f"REMOVING ORPHAN FILES FROM TABLE {self.table_name}")
            LOGGER.info(remove_orphan_files_command)
            cursor.execute(remove_orphan_files_command)
        except Exception as error:
            raise error

    def drop_extended_stats(self, cursor):
        drop_extended_stats_command = f"""ALTER TABLE {self.table_name} EXECUTE drop_extended_stats"""
        try:
            LOGGER.info(f"""DROPPING EXTENDED STATS FROM TABLE {self.table_name}""")
            LOGGER.info(drop_extended_stats_command)
            cursor.execute(drop_extended_stats_command)
        except Exception as error:
            raise error

    def analyze_table(self, cursor):
        analyze_command = f"""ANALYZE {self.table_name}"""
        try:
            LOGGER.info(f"""ANALYZING TABLE {self.table_name}""")
            LOGGER.info(analyze_command)
            cursor.execute(analyze_command)
        except Exception as error:
            raise error

    def execute(self, context):
        trino_connection = self.get_trino_connection()
        cursor = trino_connection.cursor()
        try:
            LOGGER.info(f"STARTING TABLE {self.table_name} MAINTENANCE PROCCESS")
            self.optimize_table(cursor)
            self.expire_snapshots(cursor)
            self.remove_orphan_files(cursor)
            self.drop_extended_stats(cursor)
            self.analyze_table(cursor)
        except Exception as error:
            raise error
        finally:
            trino_connection.close()

