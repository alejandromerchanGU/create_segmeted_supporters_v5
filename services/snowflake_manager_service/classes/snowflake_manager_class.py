import io
import snowflake.connector
import logging
import traceback

class SnowflakeManager:
    def __init__(self, user, private_key, account, warehouse, database, schema, role, region):
        self.user = user
        self.private_key = private_key
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.region = region
        self.conn = None
    
    def connect(self):
        try:
            #logging.info(f"--- connection: {self.user} {self.private_key} {self.account} {self.warehouse} {self.database} {self.schema } {self.role} {self.region}")
            self.conn = snowflake.connector.connect(
                user=self.user,
                private_key=self.private_key,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                authenticator='SNOWFLAKE_JWT',
                role=self.role,
                region=self.region
            )
            logging.info("Connected to Snowflake successfully!")
        except Exception as e:
            logging.error("Error connecting to Snowflake:" + str(e))
    def begin_transaction(self):
        try:
            if not self.conn:
                self.connect()
            self.conn.cursor().execute("BEGIN")
        except Exception:
            logging.exception("Failed to begin transaction")
            raise
    def commit(self):
        try:
            if self.conn:
                self.conn.cursor().execute("COMMIT")
        except Exception:
            logging.exception("Failed to commit transaction")
            raise
    def rollback(self):
        try:
            if self.conn:
                self.conn.cursor().execute("ROLLBACK")
        except Exception:
            logging.exception("Failed to rollback transaction")
            raise
    def execute_query(self, query, fetch_results=False):
        try:
            if not self.conn:
                self.connect()
            cursor = self.conn.cursor()
            cursor.execute("USE SCHEMA core_tables;")
            cursor.execute(query)
            if fetch_results:
                rows = cursor.fetchall()
            else:
                rows = None
            cursor.close()
            return rows
        except Exception:
            logging.exception("Failed execution of query: " + query)
            traceback_buffer = io.StringIO()
            traceback.print_exc(file=traceback_buffer)
            logging.error("An error occurred:\n%s", traceback_buffer.getvalue())
            raise
    def close_connection(self):
        try:
            if self.conn:
                self.conn.close()
                logging.info("Connection closed successfully!")
        except Exception as e:
            logging.info("Error closing connection:" + str(e))

    def get_nps(self):
        query = f"""
                   SELECT NONPROFIT_ID 
                   FROM NONPROFITS
                   WHERE IS_ACTIVE = TRUE and nonprofit_id in (30)
                """
        query_output = self.execute_query(query, True)
        return query_output

    def fetch_segments(self, nonprofit_id):
        query = f"""
                SELECT SEGMENTS_ID, SEGMENT_NAME, FILTER_GENERATED_SQL, LIST, STATUS
                FROM SUPPORTER_SEGMENTS
                WHERE IFF(DELETED, TRUE, FALSE) <> TRUE
                    AND (SEGMENT_ADDED_BY IS NULL OR SEGMENT_ADDED_BY <> 'superAdmin')
                    AND (FILTER_GENERATED_SQL IS NOT NULL
                            OR (FILTER_GENERATED_SQL IS NULL AND (LIST IS NULL OR LIST = [])) 
                        )
                    AND NONPROFIT_ID = {nonprofit_id}
                    AND STATUS IN ('pending', 'error', 'pending_error', 'updated')
                ORDER BY UPDATED_AT desc nulls last 
               """
        query_output = self.execute_query(query, True)
        return query_output
    
    def fetch_build_query_select(self, query):
        query_output = self.execute_query(query, True)
        return query_output
    
    def create_trasient_table(self, nonprofit_id, snowflake_database):
        logging.info(f"--- creating tmp able: TEMP.SEGMENTED_SUPPORTER_JOURNEY_TRIGGERED_{nonprofit_id}")
        create_journey_triggered_query = f"""
                create or replace TRANSIENT TABLE {snowflake_database}.TEMP.SEGMENTED_SUPPORTER_JOURNEY_TRIGGERED_{nonprofit_id} (
                    NONPROFIT_ID NUMBER(38,0) NOT NULL,
                    SEGMENT_ID NUMBER(38,0) NOT NULL,
                    SUPPORTER_ID NUMBER(38,0) NOT NULL,
                    IS_NEW_SUPPORTER BOOLEAN NOT NULL,
                    JOURNEY_TRIGGERED BOOLEAN NOT NULL
                );
                """
        create_removed_query = f"""
                create or replace TRANSIENT TABLE {snowflake_database}.TEMP.SEGMENTED_SUPPORTERS_REMOVED_{nonprofit_id} (
                    NONPROFIT_ID NUMBER(38,0) NOT NULL,
                    SEGMENT_ID NUMBER(38,0) NOT NULL,
                    SUPPORTER_ID NUMBER(38,0) NOT NULL
                );
                """
        self.begin_transaction()
        self.execute_query(create_journey_triggered_query)
        self.execute_query(create_removed_query)
        self.commit()

    def update_segment_status(self, nonprofit_id, segment_id, status):
        update_query = f"""
                UPDATE SUPPORTER_SEGMENTS
                SET 
                    STATUS = '{status}',
                    STATUS_AT = CURRENT_TIMESTAMP(),
                    ERROR_DETAILS = NULL
                WHERE SEGMENTS_ID = {segment_id}
                    AND NONPROFIT_ID = {nonprofit_id}
                """
        query_output = self.execute_query(update_query, True)
        return query_output
    def update_segment_error_status(self, nonprofit_id, segment_id, current_status, error_details):
        if current_status == 'pending':
            error_status = 'pending_error'
        else:
            error_status = 'error'
        update_query = f"""
                UPDATE SUPPORTER_SEGMENTS
                SET 
                    STATUS = '{error_status}',
                    STATUS_AT = CURRENT_TIMESTAMP(),
                    ERROR_DETAILS = $${error_details}$$
                WHERE SEGMENTS_ID = {segment_id}
                    AND NONPROFIT_ID = {nonprofit_id}
                """
        query_output = self.execute_query(update_query, True)
        return query_output



    def fetch_triggered_supporters_grouped(self, nonprofit_id, snowflake_database: str):

        table_fqn = f"{snowflake_database}.TEMP.SEGMENTED_SUPPORTER_JOURNEY_TRIGGERED_{nonprofit_id}"
        where_clause = "WHERE IS_NEW_SUPPORTER = TRUE"

        query = f"""
            SELECT
              NONPROFIT_ID,
              SEGMENT_ID,
              ARRAY_AGG(DISTINCT SUPPORTER_ID) AS SUPPORTER_IDS
            FROM {table_fqn}
            {where_clause}
            GROUP BY NONPROFIT_ID, SEGMENT_ID
            ORDER BY NONPROFIT_ID, SEGMENT_ID
        """

        query_output = self.execute_query(query, True)
        return  query_output


    def fetch_triggered_supporters_lambda_grouped(self, nonprofit_id, snowflake_database: str):

        table_fqn = f"{snowflake_database}.TEMP.SEGMENTED_SUPPORTER_JOURNEY_TRIGGERED_{nonprofit_id}"
        where_clause = "WHERE JOURNEY_TRIGGERED = TRUE"

        query = f"""
            SELECT
              NONPROFIT_ID,
              SEGMENT_ID,
              ARRAY_AGG(DISTINCT SUPPORTER_ID) AS SUPPORTER_IDS
            FROM {table_fqn}
            {where_clause}
            GROUP BY NONPROFIT_ID, SEGMENT_ID
            ORDER BY NONPROFIT_ID, SEGMENT_ID
        """

        query_output = self.execute_query(query, True)
        return  query_output


    def fetch_removed_supporters_grouped(self, nonprofit_id, snowflake_database: str):

        table_fqn = f"{snowflake_database}.TEMP.SEGMENTED_SUPPORTERS_REMOVED_{nonprofit_id}"

        query = f"""
            SELECT
              NONPROFIT_ID,
              SEGMENT_ID,
              ARRAY_AGG(DISTINCT SUPPORTER_ID) AS SUPPORTER_IDS
            FROM {table_fqn}
            GROUP BY NONPROFIT_ID, SEGMENT_ID
            ORDER BY NONPROFIT_ID, SEGMENT_ID
        """

        query_output = self.execute_query(query, True)
        return  query_output