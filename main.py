from logging.handlers import RotatingFileHandler
import traceback
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from services.snowflake_manager_service.classes.snowflake_manager_class import SnowflakeManager
import logging
import os
import boto3
from typing import Iterable, List, Dict, Any, Generator
import json
from botocore.exceptions import ClientError
import sys
import io

load_dotenv()

# Configure the logging
log_filename = 'css_logs.log'
log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)

# Create a rotating file handler to manage log files
handler = RotatingFileHandler(log_filename, mode='a', maxBytes=1024 * 1024, backupCount=2, encoding='utf-8',
                              delay=False)

# Set the formatter for the handler
handler.setFormatter(logging.Formatter(log_format))

# Add the handler to the root logging
logging.getLogger().addHandler(handler)
logger = logging.getLogger(__name__)

#Configue database env
snowflake_database = os.environ.get("SNOWFLAKE_DATABASE")

def get_manager(user, account, warehouse, database, schema, region, role):

    #logging.info(f"--- connection: {user}  {account} {warehouse} {database} {schema } {role} {region}")

    ssm = boto3.client('ssm')  # NOSONAR
    passphrase_bytes = ssm.get_parameter(Name="/app/phase_runner/SNOWFLAKE_PASSPHRASE", WithDecryption=True)["Parameter"]["Value"].encode('utf-8')
    pem_private_key_bytes = ssm.get_parameter(Name="/app/phase_runner/SNOWFLAKE_AUTH_KEY", WithDecryption=True)["Parameter"]["Value"].encode('utf-8')

    #pem_private_key_bytes = os.environ.get("SNOWFLAKE_AUTH_KEY").encode('utf-8')
    #passphrase_bytes = os.environ.get("SNOWFLAKE_PASSPHRASE").encode('utf-8')

    private_key_obj = serialization.load_pem_private_key(
        pem_private_key_bytes,
        password=passphrase_bytes,
        backend=default_backend()
    )

    private_key_bytes = private_key_obj.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    snowflake_manager = SnowflakeManager(
        user=user,
        private_key=private_key_bytes,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        region=region,
        role=role
    )
    return snowflake_manager

def process_segment(snowflake_manager: SnowflakeManager, nonprofit_id: int, segment_id: int, filter_generated_sql: str,
                    current_status: str, delete_only: bool) -> None:
    try:
        snowflake_manager.begin_transaction()

        delete_sql = f"""
            DELETE FROM {snowflake_database}.CORE_TABLES.SEGMENTED_SUPPORTER
            WHERE SEGMENT_ID = {segment_id}
            AND NONPROFIT_ID = {nonprofit_id}
            """
        delete_read_sql = f"""
            DELETE FROM {snowflake_database}.CORE_TABLES.SEGMENTED_SUPPORTER_READ
            WHERE SEGMENT_ID = {segment_id}
            AND NONPROFIT_ID = {nonprofit_id}
            """

        update_status_sql = f"""
            UPDATE {snowflake_database}.CORE_TABLES.SUPPORTER_SEGMENTS
            SET
                STATUS = 'updated',
                STATUS_AT = CURRENT_TIMESTAMP()
            WHERE SEGMENTS_ID = {segment_id}
                AND NONPROFIT_ID = {nonprofit_id}
            """
        if delete_only:
            insert_remove_trasient_sql = f"""
            INSERT INTO {snowflake_database}.TEMP.SEGMENTED_SUPPORTERS_REMOVED_{nonprofit_id} 
            (NONPROFIT_ID, SEGMENT_ID, SUPPORTER_ID)
            SELECT
                {nonprofit_id} AS NONPROFIT_ID,
                {segment_id} AS SEGMENT_ID,
                SUPPORTER_ID
            FROM {snowflake_database}.CORE_TABLES.SEGMENTED_SUPPORTER_READ
            WHERE SEGMENT_ID = {segment_id}
            AND NONPROFIT_ID = {nonprofit_id}
            """
            snowflake_manager.execute_query(insert_remove_trasient_sql)
            snowflake_manager.execute_query(delete_sql)
            snowflake_manager.execute_query(delete_read_sql)
            snowflake_manager.execute_query(update_status_sql)
        else:
            insert_journey_trigger_transient_sql = f"""
                INSERT INTO {snowflake_database}.TEMP.SEGMENTED_SUPPORTER_JOURNEY_TRIGGERED_{nonprofit_id} 
                (NONPROFIT_ID, SEGMENT_ID, SUPPORTER_ID, IS_NEW_SUPPORTER, JOURNEY_TRIGGERED)
                SELECT
                    {nonprofit_id} AS NONPROFIT_ID,
                    {segment_id} AS SEGMENT_ID,
                    t.SUPPORTER_ID,
                    CASE 
                        WHEN NOT EXISTS (
                            SELECT 1 
                            FROM {snowflake_database}.CORE_TABLES.SEGMENTED_SUPPORTER_READ ss
                            WHERE ss.SUPPORTER_ID = t.SUPPORTER_ID
                            AND ss.SEGMENT_ID = {segment_id}
                            AND ss.NONPROFIT_ID = {nonprofit_id}
                            AND IFF(ss.DELETED, TRUE, FALSE) <> TRUE
                        ) THEN TRUE
                        ELSE FALSE
                    END AS IS_NEW_SUPPORTER,
                    CASE 
                        WHEN '{current_status}' IN ('pending', 'pending_error') THEN FALSE
                        WHEN NOT EXISTS (
                            SELECT 1 
                            FROM {snowflake_database}.CORE_TABLES.SEGMENTED_SUPPORTER_READ ss
                            WHERE ss.SUPPORTER_ID = t.SUPPORTER_ID
                            AND ss.SEGMENT_ID = {segment_id}
                            AND ss.NONPROFIT_ID = {nonprofit_id}
                            AND IFF(ss.DELETED, TRUE, FALSE) <> TRUE
                        ) THEN TRUE
                        ELSE FALSE
                    END AS JOURNEY_TRIGGERED
                FROM (
                    {filter_generated_sql}
                ) t
                """
            insert_remove_trasient_sql = f"""
                INSERT INTO {snowflake_database}.TEMP.SEGMENTED_SUPPORTERS_REMOVED_{nonprofit_id} 
                (NONPROFIT_ID, SEGMENT_ID, SUPPORTER_ID)
                SELECT
                    {nonprofit_id} AS NONPROFIT_ID,
                    {segment_id} AS SEGMENT_ID,
                    ssr.SUPPORTER_ID
                FROM {snowflake_database}.CORE_TABLES.SEGMENTED_SUPPORTER_READ ssr
                WHERE ssr.SEGMENT_ID = {segment_id}
                    AND ssr.NONPROFIT_ID = {nonprofit_id}
                    AND IFF(ssr.DELETED, TRUE, FALSE) <> TRUE
                    AND NOT EXISTS (
                        SELECT 1 
                        FROM (
                            {filter_generated_sql}
                        ) fg
                        WHERE fg.SUPPORTER_ID = ssr.SUPPORTER_ID
                    )
                """
            created_by_desc = "'created_seg_supporter_pyprocess_ao'"
            insert_sql = f"""
                INSERT INTO {snowflake_database}.CORE_TABLES.SEGMENTED_SUPPORTER (
                    ID, SEGMENT_ID, NONPROFIT_ID, SUPPORTER_ID, CREATED_AT, CREATED_BY, DELETED
                )
                SELECT
                    {snowflake_database}.CORE_TABLES.SEQ_SEGMENTED_SUPPORTER_HYBRID.NEXTVAL,
                    SEGMENT_ID,
                    NONPROFIT_ID,
                    SUPPORTER_ID,
                    CURRENT_TIMESTAMP(),         -- CREATED_AT
                    {created_by_desc},           -- CREATED_BY
                    FALSE                        -- DELETED
                FROM {snowflake_database}.TEMP.SEGMENTED_SUPPORTER_JOURNEY_TRIGGERED_{nonprofit_id}
                WHERE SEGMENT_ID = {segment_id}
                """
            insert_read_sql = f"""
                INSERT INTO {snowflake_database}.CORE_TABLES.SEGMENTED_SUPPORTER_READ (
                    SEGMENT_ID, NONPROFIT_ID, SUPPORTER_ID, CREATED_AT, CREATED_BY, DELETED
                )
                SELECT
                    SEGMENT_ID,
                    NONPROFIT_ID,
                    SUPPORTER_ID,
                    CURRENT_TIMESTAMP(),         -- CREATED_AT
                    {created_by_desc},           -- CREATED_BY
                    FALSE                        -- DELETED
                FROM {snowflake_database}.TEMP.SEGMENTED_SUPPORTER_JOURNEY_TRIGGERED_{nonprofit_id}
                WHERE SEGMENT_ID = {segment_id}
                """
            snowflake_manager.execute_query(insert_journey_trigger_transient_sql)
            snowflake_manager.execute_query(insert_remove_trasient_sql)
            snowflake_manager.execute_query(delete_sql)
            snowflake_manager.execute_query(delete_read_sql)
            snowflake_manager.execute_query(insert_sql)
            snowflake_manager.execute_query(insert_read_sql)
            snowflake_manager.execute_query(update_status_sql)

        snowflake_manager.commit()
    except Exception:
        snowflake_manager.rollback()
        raise

def process_segments(nonprofit_id: int, snowflake_manager) -> None:
    ##snowflake_manager = get_manager()
    ##snowflake_manager.connect()
    snowflake_manager.create_trasient_table(nonprofit_id, snowflake_database)
    segments = snowflake_manager.fetch_segments(nonprofit_id)
    for segment in segments:
        segment_id = segment[0]
        segment_name = segment[1]
        filter_generated_sql = segment[2]
        segment_list = segment[3]
        status = segment[4]
        try:
            update_output = snowflake_manager.update_segment_status(nonprofit_id, segment_id, 'processing')
            logger.info(f"Updated segment status {segment_id} - {segment_name} and updated rows in supporter_segments table: " + str(update_output))
            process_segment(snowflake_manager, nonprofit_id, segment_id, filter_generated_sql, status, segment_list is None or segment_list == [] or segment_list == '[]')
        except Exception as e:
            error_details = f"{str(e)}\n{traceback.format_exc()}"
            logger.error(f"Error processing segment {segment_id} - {segment_name} (nonprofit {nonprofit_id}): {error_details}", exc_info=True)
            snowflake_manager.update_segment_error_status(
                nonprofit_id,
                segment_id,
                status,
                error_details
            )

def _chunked(lst: List[int], n:int) -> Generator[List[int], None, None]:
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def send_to_lambda(nonprofit_id: int,
                   snowflake_manager,
                   lambda_function_name,
                   region_name: str = "us-east-1",
                   batch_size: int = 8000) -> None:
    supporters = snowflake_manager.fetch_triggered_supporters_lambda_grouped(nonprofit_id,snowflake_database)

    if not supporters:
        logging.info(f"-- not supporters to send, nonprofit: {nonprofit_id}")

    lambda_client = boto3.client("lambda", region_name = region_name)

    total_batches =0
    total_supporters = 0

    for support in supporters:
        np_id = int(support[0])
        seg_id = int(support[1])
        supporter_ids_raw = support[2] or []

        supporter_ids = list(dict.fromkeys(json.loads(supporter_ids_raw)))

        if not supporter_ids:
            logging.info(f"segments without supporters, nonprofit = {np_id}, segment_id = {seg_id}")
            continue

        for batch_idx, batch in enumerate(_chunked(supporter_ids, batch_size), start=1):
            payload = {
                "nonprofitId": np_id,
                "segmentId": seg_id,
                "supporterIds": batch
            }

            try:
                resp = lambda_client.invoke(
                    FunctionName=lambda_function_name,
                    InvocationType="RequestResponse",
                    Payload=json.dumps(payload).encode("utf-8")
                )

                total_batches += 1
                total_supporters += len(batch)

                status_code = resp.get("StatusCode")
                logging.info(
                    f"Lambda called: {lambda_function_name} "
                    f"np={np_id}, seg={seg_id}, batch={batch_idx}, size={len(batch)} "
                    f"StatusCode={status_code}"
                )

            except ClientError as e:
                logging.exception(
                    f"fails calling the lambda for np: {np_id}"
                )
                traceback_buffer = io.StringIO()
                traceback.print_exc(file=traceback_buffer)
                logging.error("An error occurred:\n%s", traceback_buffer.getvalue())
                raise


def test_call_segments_service(token):
    base_url = "https://segment.dev.goodunited.io/nonprofit/30/segments/cache-sync/remove-segment"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-private-access-token": "1"
    }

    payload = {
        "segmentId": "1760709005044737",
        "supporterIds": ["988394845"]
    }


def call_segments_service(
        nonprofit_id: int,
        operation: str,
        snowflake_manager,
        env: str,
        token: str,
        batch_size: int = 8000
) -> None:
    import requests
    import json

    op = operation.strip().lower()
    if op not in ("insert-segment", "remove-segment"):
        logger.error(f"[segments-service] Invalid operation: {operation}")
        return

    path = "insert-segment" if op == "insert-segment" else "remove-segment"
    base_url = f"https://{env}/nonprofit/{nonprofit_id}/segments/cache-sync/{path}"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-private-access-token": "1"
    }

    logger.info(f"[segments-service] Starting HTTP call: op={op}, url={base_url}, np={nonprofit_id}")

    # Select data source based on operation (DRY)
    if op == "insert-segment":
        rows = snowflake_manager.fetch_triggered_supporters_grouped(nonprofit_id, snowflake_database)
        empty_msg = f"[segments-service] -- No supporters to insert, nonprofit: {nonprofit_id}"
    else:  # remove_segments
        rows = snowflake_manager.fetch_removed_supporters_grouped(nonprofit_id, snowflake_database)
        empty_msg = f"[segments-service] -- No supporters to remove, nonprofit: {nonprofit_id}"

    if not rows:
        logger.info(empty_msg)
        return

    timeout_secs = 30
    total_batches = 0
    total_supporters = 0

    def _send_batches(np_id: int, seg_id: int, supporter_ids_json: str) -> None:
        nonlocal total_batches, total_supporters
        raw = supporter_ids_json or "[]"

        try:
            # Deduplicate while preserving order
            supporter_ids_list = list(dict.fromkeys(json.loads(raw)))
        except Exception:
            logger.exception(f"[segments-service] Error parsing supporter_ids for np={np_id}, seg={seg_id}")
            return

        if not supporter_ids_list:
            logger.info(f"[segments-service] Segment has no supporters, nonprofit={np_id}, segment_id={seg_id}")
            return

        for batch_idx, chunk in enumerate(_chunked(supporter_ids_list, batch_size), start=1):
            payload = {
                "segmentId": str(seg_id),
                "supporterIds": [str(x) for x in chunk]
            }
            try:
                print(f'--- headers: {headers}')
                resp = requests.post(
                    base_url,
                    headers=headers,
                    json=payload,
                    timeout=timeout_secs,
                )
                total_batches += 1
                total_supporters += len(chunk)

                logger.info(
                    f"[segments-service] POST {base_url} "
                    f"np={np_id}, seg={seg_id}, batch={batch_idx}, size={len(chunk)}, "
                    f"status={resp.status_code}"
                )

                if resp.status_code >= 400:
                    logger.error(
                        f"[segments-service] HTTP error ({resp.status_code}) np={np_id}, seg={seg_id}, "
                        f"batch={batch_idx}, body={resp.text[:500]}"
                    )
            except Exception:
                logger.exception(
                    f"[segments-service] Exception while calling endpoint for np={np_id}, seg={seg_id}, batch={batch_idx}"
                )
                traceback_buffer = io.StringIO()
                traceback.print_exc(file=traceback_buffer)
                logging.error("An error occurred:\n%s", traceback_buffer.getvalue())
                raise

    # rows: iterable of (nonprofit_id, segment_id, supporter_ids_json)
    for row in rows:
        np_id = int(row[0])
        seg_id = int(row[1])
        supporter_ids_raw = row[2]
        _send_batches(np_id, seg_id, supporter_ids_raw)

    logger.info(
        f"[segments-service] Completed: op={op}, np={nonprofit_id}, "
        f"batches={total_batches}, supporters_sent={total_supporters}"
    )









def main():
    # Validar variables de entorno requeridas
    required_envs = ["SERVICE_TOKEN", "SERVICE_ENV", "LAMBDA_ENV", "NP_ID"]
    missing = [var for var in required_envs if not os.environ.get(var)]

    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}"
        )

    token = os.environ.get("SERVICE_TOKEN")
    service_env = os.environ.get("SERVICE_ENV")
    lambda_env = os.environ.get("LAMBDA_ENV")
    np_id = os.environ.get("NP_ID")

    user = os.environ.get("USER_DB")
    account=os.environ.get("ACCOUNT_DB")
    warehouse=os.environ.get("WAREHOUSE_DB")
    database=os.environ.get("DATABASE")
    schema=os.environ.get("SCHEMA_DB")
    region=os.environ.get("REGION_DB")
    role=os.environ.get("ROLE_DB")

    snowflake_manager = get_manager(user, account, warehouse, database, schema, region, role)
    snowflake_manager.connect()
    #nps = snowflake_manager.get_nps()

    logger.info(f"--- processing segments for np: {np_id}")
    process_segments(np_id, snowflake_manager)

    call_segments_service(np_id,'insert-segment',snowflake_manager,service_env,token,8000)
    call_segments_service(np_id,'remove-segment',snowflake_manager,service_env,token,8000)
    send_to_lambda(np_id,snowflake_manager,lambda_env)


if __name__ == "__main__":
    main()



