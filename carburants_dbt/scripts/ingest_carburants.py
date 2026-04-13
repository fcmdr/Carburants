#!/usr/bin/env python3
"""
Ingestion script for French fuel prices data.
Downloads data from data.gouv.fr and loads it into Snowflake.

Data source: https://donnees.roulez-eco.fr/opendata/instantane
"""

import os
import sys
import logging
import zipfile
import io
from datetime import datetime, timezone
from typing import Optional
import xml.etree.ElementTree as ET

import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Data source URL
DATA_URL = "https://donnees.roulez-eco.fr/opendata/instantane"

# Snowflake configuration from environment variables
SNOWFLAKE_CONFIG = {
    'account': os.environ.get('SNOWFLAKE_ACCOUNT'),
    'user': os.environ.get('SNOWFLAKE_USER'),
    'password': os.environ.get('SNOWFLAKE_PASSWORD'),
    'database': os.environ.get('SNOWFLAKE_DATABASE', 'CARBURANTS_DEV'),
    'schema': os.environ.get('SNOWFLAKE_SCHEMA', 'RAW'),
    'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
    'role': os.environ.get('SNOWFLAKE_ROLE', 'TRANSFORMER'),
}


def download_data() -> bytes:
    """Download the fuel prices ZIP file from the API."""
    logger.info(f"Downloading data from {DATA_URL}")

    response = requests.get(DATA_URL, timeout=120)
    response.raise_for_status()

    logger.info(f"Downloaded {len(response.content)} bytes")
    return response.content


def extract_xml_from_zip(zip_content: bytes) -> str:
    """Extract XML content from the downloaded ZIP file."""
    logger.info("Extracting XML from ZIP archive")

    with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
        # Find the XML file in the archive
        xml_files = [f for f in zf.namelist() if f.endswith('.xml')]
        if not xml_files:
            raise ValueError("No XML file found in the ZIP archive")

        xml_filename = xml_files[0]
        logger.info(f"Found XML file: {xml_filename}")

        with zf.open(xml_filename) as xml_file:
            return xml_file.read().decode('latin-1')


def parse_stations(root: ET.Element) -> pd.DataFrame:
    """Parse station information from XML."""
    stations = []

    for pdv in root.findall('.//pdv'):
        station = {
            'station_id': pdv.get('id'),
            'latitude': pdv.get('latitude'),
            'longitude': pdv.get('longitude'),
            'cp': pdv.get('cp'),
            'pop': pdv.get('pop'),
            'adresse': pdv.findtext('adresse', ''),
            'ville': pdv.findtext('ville', ''),
        }

        # Extract services
        services_elem = pdv.find('services')
        if services_elem is not None:
            services = [s.text for s in services_elem.findall('service') if s.text]
            station['services'] = ','.join(services)
        else:
            station['services'] = ''

        # Extract horaires
        horaires_elem = pdv.find('horaires')
        if horaires_elem is not None:
            station['automate_24_24'] = horaires_elem.get('automate-24-24', '')

            jours = []
            for jour in horaires_elem.findall('jour'):
                jour_info = {
                    'nom': jour.get('nom', ''),
                    'ferme': jour.get('ferme', ''),
                }
                horaires_list = []
                for h in jour.findall('horaire'):
                    horaires_list.append(f"{h.get('ouverture', '')}-{h.get('fermeture', '')}")
                jour_info['horaires'] = ';'.join(horaires_list)
                jours.append(f"{jour_info['nom']}:{jour_info['ferme']}:{jour_info['horaires']}")
            station['horaires_json'] = '|'.join(jours)
        else:
            station['automate_24_24'] = ''
            station['horaires_json'] = ''

        stations.append(station)

    df = pd.DataFrame(stations)
    df['_loaded_at'] = datetime.now(timezone.utc)

    logger.info(f"Parsed {len(df)} stations")
    return df


def parse_prix(root: ET.Element) -> pd.DataFrame:
    """Parse fuel prices from XML."""
    prix_list = []

    for pdv in root.findall('.//pdv'):
        station_id = pdv.get('id')

        for prix in pdv.findall('prix'):
            prix_data = {
                'station_id': station_id,
                'carburant_nom': prix.get('nom'),
                'carburant_id': prix.get('id'),
                'valeur': prix.get('valeur'),
                'maj': prix.get('maj'),
            }
            prix_list.append(prix_data)

    df = pd.DataFrame(prix_list)
    df['_loaded_at'] = datetime.now(timezone.utc)

    logger.info(f"Parsed {len(df)} price records")
    return df


def parse_ruptures(root: ET.Element) -> pd.DataFrame:
    """Parse fuel shortages/ruptures from XML."""
    ruptures_list = []

    for pdv in root.findall('.//pdv'):
        station_id = pdv.get('id')

        for rupture in pdv.findall('rupture'):
            rupture_data = {
                'station_id': station_id,
                'carburant_nom': rupture.get('nom'),
                'carburant_id': rupture.get('id'),
                'debut': rupture.get('debut'),
                'fin': rupture.get('fin'),
                'type': rupture.get('type'),
            }
            ruptures_list.append(rupture_data)

    df = pd.DataFrame(ruptures_list)
    if not df.empty:
        df['_loaded_at'] = datetime.now(timezone.utc)

    logger.info(f"Parsed {len(df)} rupture records")
    return df


def get_snowflake_connection():
    """Create a Snowflake connection."""
    logger.info("Connecting to Snowflake")

    return snowflake.connector.connect(
        account=SNOWFLAKE_CONFIG['account'],
        user=SNOWFLAKE_CONFIG['user'],
        password=SNOWFLAKE_CONFIG['password'],
        database=SNOWFLAKE_CONFIG['database'],
        schema=SNOWFLAKE_CONFIG['schema'],
        warehouse=SNOWFLAKE_CONFIG['warehouse'],
        role=SNOWFLAKE_CONFIG['role'],
    )


def ensure_schema_exists(conn) -> None:
    """Create the RAW schema if it doesn't exist."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_CONFIG['schema']}")
        logger.info(f"Schema {SNOWFLAKE_CONFIG['schema']} ready")
    finally:
        cursor.close()


def create_tables(conn) -> None:
    """Create the raw tables if they don't exist."""
    cursor = conn.cursor()

    tables = {
        'RAW_STATIONS': """
            CREATE TABLE IF NOT EXISTS RAW_STATIONS (
                STATION_ID VARCHAR(20),
                LATITUDE VARCHAR(20),
                LONGITUDE VARCHAR(20),
                CP VARCHAR(10),
                POP VARCHAR(5),
                ADRESSE VARCHAR(500),
                VILLE VARCHAR(200),
                SERVICES VARCHAR(2000),
                AUTOMATE_24_24 VARCHAR(10),
                HORAIRES_JSON VARCHAR(5000),
                _LOADED_AT TIMESTAMP_TZ
            )
        """,
        'RAW_PRIX': """
            CREATE TABLE IF NOT EXISTS RAW_PRIX (
                STATION_ID VARCHAR(20),
                CARBURANT_NOM VARCHAR(50),
                CARBURANT_ID VARCHAR(10),
                VALEUR VARCHAR(20),
                MAJ VARCHAR(50),
                _LOADED_AT TIMESTAMP_TZ
            )
        """,
        'RAW_RUPTURES': """
            CREATE TABLE IF NOT EXISTS RAW_RUPTURES (
                STATION_ID VARCHAR(20),
                CARBURANT_NOM VARCHAR(50),
                CARBURANT_ID VARCHAR(10),
                DEBUT VARCHAR(50),
                FIN VARCHAR(50),
                TYPE VARCHAR(50),
                _LOADED_AT TIMESTAMP_TZ
            )
        """,
        'RAW_INGESTION_LOG': """
            CREATE TABLE IF NOT EXISTS RAW_INGESTION_LOG (
                INGESTION_ID VARCHAR(50),
                STARTED_AT TIMESTAMP_TZ,
                COMPLETED_AT TIMESTAMP_TZ,
                STATIONS_COUNT INTEGER,
                PRIX_COUNT INTEGER,
                RUPTURES_COUNT INTEGER,
                STATUS VARCHAR(20),
                ERROR_MESSAGE VARCHAR(5000)
            )
        """
    }

    try:
        for table_name, ddl in tables.items():
            cursor.execute(ddl)
            logger.info(f"Table {table_name} ready")
    finally:
        cursor.close()


def truncate_and_load(conn, df: pd.DataFrame, table_name: str) -> None:
    """Truncate table and load new data."""
    if df.empty:
        logger.warning(f"No data to load into {table_name}")
        return

    cursor = conn.cursor()
    try:
        # Truncate existing data
        cursor.execute(f"TRUNCATE TABLE IF EXISTS {table_name}")
        logger.info(f"Truncated {table_name}")
    finally:
        cursor.close()

    # Convert column names to uppercase for Snowflake
    df.columns = [col.upper() for col in df.columns]

    # Write data
    success, nchunks, nrows, _ = write_pandas(
        conn, df, table_name, quote_identifiers=False
    )

    if success:
        logger.info(f"Loaded {nrows} rows into {table_name}")
    else:
        raise Exception(f"Failed to load data into {table_name}")


def log_ingestion(
    conn,
    ingestion_id: str,
    started_at: datetime,
    stations_count: int,
    prix_count: int,
    ruptures_count: int,
    status: str,
    error_message: Optional[str] = None
) -> None:
    """Log the ingestion run."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO RAW_INGESTION_LOG
            (INGESTION_ID, STARTED_AT, COMPLETED_AT, STATIONS_COUNT, PRIX_COUNT, RUPTURES_COUNT, STATUS, ERROR_MESSAGE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                ingestion_id,
                started_at,
                datetime.now(timezone.utc),
                stations_count,
                prix_count,
                ruptures_count,
                status,
                error_message
            )
        )
        logger.info(f"Logged ingestion: {status}")
    finally:
        cursor.close()


def main():
    """Main ingestion function."""
    ingestion_id = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    started_at = datetime.now(timezone.utc)

    logger.info(f"Starting ingestion run: {ingestion_id}")

    # Validate configuration
    missing_config = [k for k, v in SNOWFLAKE_CONFIG.items() if not v and k not in ['database', 'schema', 'warehouse', 'role']]
    if missing_config:
        logger.error(f"Missing required configuration: {missing_config}")
        sys.exit(1)

    try:
        # Download and parse data
        zip_content = download_data()
        xml_content = extract_xml_from_zip(zip_content)

        root = ET.fromstring(xml_content)

        df_stations = parse_stations(root)
        df_prix = parse_prix(root)
        df_ruptures = parse_ruptures(root)

        # Connect to Snowflake and load data
        conn = get_snowflake_connection()
        try:
            ensure_schema_exists(conn)
            create_tables(conn)

            truncate_and_load(conn, df_stations, 'RAW_STATIONS')
            truncate_and_load(conn, df_prix, 'RAW_PRIX')
            truncate_and_load(conn, df_ruptures, 'RAW_RUPTURES')

            log_ingestion(
                conn, ingestion_id, started_at,
                len(df_stations), len(df_prix), len(df_ruptures),
                'SUCCESS'
            )

            logger.info("Ingestion completed successfully!")

        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")

        # Try to log the failure
        try:
            conn = get_snowflake_connection()
            ensure_schema_exists(conn)
            create_tables(conn)
            log_ingestion(conn, ingestion_id, started_at, 0, 0, 0, 'FAILED', str(e))
            conn.close()
        except Exception:
            pass

        sys.exit(1)


if __name__ == '__main__':
    main()
