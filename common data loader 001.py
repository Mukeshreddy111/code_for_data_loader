import argparse
import json
import logging
import oracledb
import teradatasql
from typing import List, Dict
from collections.abc import Mapping


class DatabaseLoader:
    def __init__(self, config: Dict, db_type: str):
        """
        Initialize the DatabaseLoader.
        
        Args:
        - config: Configuration dictionary.
        - db_type: Type of the database ("oracle" or "teradata").
        """
        self.config = config
        self.db_type = db_type.lower()
        self.connection = None
        self.logger = logging.getLogger(f'{db_type}-data-loader')

        if self.db_type not in ['oracle', 'teradata']:
            raise ValueError("Unsupported database type. Please use 'oracle' or 'teradata'.")

    def setup_logging(self, log_level=logging.INFO):
        """
        Setup logging based on the provided log level.
        """
        logging.basicConfig(level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    def connect(self):
        """
        Establish a connection to the database based on the db_type.
        """
        if self.db_type == "teradata":
            host = self.config.get('host', 'teradata.example.com')
            logmech = self.config.get('logmech', 'KERBEROS')
            tmode = self.config.get('tmode', 'ANSI')
            encrypt_data = self.config.get('encrypt_data', True)

            self.logger.info(f"Connecting to Teradata at {host} with {logmech}, transaction mode {tmode}, encrypt_data={encrypt_data}")
            self.connection = teradatasql.connect(host=host, logmech=logmech, tmode=tmode, encryptdata=encrypt_data)

        elif self.db_type == "oracle":
            oracledb.init_oracle_client(lib_dir=self.config.get('lib_dir'))  # Optional, if Oracle client libraries are needed
            dsn = oracledb.makedsn(
                self.config.get('host', 'oracle.example.com'),
                self.config.get('port', 1521),
                service_name=self.config.get('service_name', 'orcl')
            )
            self.logger.info(f"Connecting to Oracle at {dsn}")
            self.connection = oracledb.connect(
                user=self.config.get('username'),
                password=self.config.get('password'),
                dsn=dsn
            )

    def disconnect(self):
        """
        Close the database connection.
        """
        if self.connection:
            self.connection.close()
            self.logger.info(f"Connection to {self.db_type} closed.")

    def execute_query(self, query: str, output_file: str = None):
        """
        Execute a SQL query and optionally write the results to a file.
        
        Args:
        - query: SQL query to execute.
        - output_file: Path to output the query results (if provided).
        """
        cursor = self.connection.cursor()
        self.logger.info(f"Executing query: {query}")
        cursor.execute(query)

        results = cursor.fetchall()
        columns = [col[0] for col in cursor.description]  # Get column names

        if output_file:
            with open(output_file, 'w') as f:
                f.write(','.join(columns) + '\n')
                for row in results:
                    f.write(','.join(map(str, row)) + '\n')
            self.logger.info(f"Results written to {output_file}")

        cursor.close()
        return results

    def get_metadata(self, query: str):
        """
        Fetch metadata for the given query (like column names and types).
        
        Args:
        - query: SQL query to fetch metadata for.
        
        Returns:
        - metadata: A list of column metadata (names and types).
        """
        cursor = self.connection.cursor()
        self.logger.info(f"Fetching metadata for query: {query}")
        cursor.execute(query)
        
        metadata = []
        for col in cursor.description:
            metadata.append({
                "name": col[0],
                "type": col[1].__name__ if self.db_type == "oracle" else col[1]
            })

        cursor.close()
        return metadata

    def sanitize_names(self, names: List[str]) -> List[str]:
        """
        Sanitize a list of column names by replacing special characters with '_'.
        
        Args:
        - names: List of column names.
        
        Returns:
        - sanitized_column_names: List of sanitized column names.
        """
        sanitized_column_names = [''.join(c if c.isalnum() else '_' for c in name).lower() for name in names]
        return sanitized_column_names


# Utility function to merge dictionaries recursively
def merge_dicts(d1: Dict, d2: Dict) -> Dict:
    """
    Recursively merge two dictionaries.
    Values from d2 override those in d1.
    
    Args:
    - d1: The base dictionary.
    - d2: The dictionary with overriding values.
    
    Returns:
    - result: Merged dictionary.
    """
    for k, v in d2.items():
        if isinstance(v, Mapping) and v:
            d1[k] = merge_dicts(d1.get(k, {}), v)
        else:
            d1[k] = v
    return d1


# Argument parsing and configuration reading
def parse_args():
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Database Loader for Teradata and Oracle")
    parser.add_argument("--config-files", nargs='+', type=str, help="Paths to JSON configuration files (supports multiple files)", required=True)
    parser.add_argument("--query", type=str, help="SQL query to execute", required=True)
    parser.add_argument("--db-type", type=str, choices=["oracle", "teradata"], help="Database type", required=True)
    parser.add_argument("--output-file", type=str, help="File to output the query results", required=False)
    parser.add_argument("--log-level", type=str, default="INFO", help="Set the log level (default: INFO)")
    
    return parser.parse_args()


def load_and_merge_configs(config_files: List[str]) -> Dict:
    """
    Load and merge multiple configuration files. Later files override earlier ones.
    
    Args:
    - config_files: List of paths to JSON config files.
    
    Returns:
    - merged_config: A dictionary containing merged configuration values.
    """
    merged_config = {}
    
    for config_file in config_files:
        with open(config_file, 'r') as f:
            config = json.load(f)
            merged_config = merge_dicts(merged_config, config)
    
    return merged_config


if __name__ == "__main__":
    args = parse_args()
    
    # Set log level
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    
    # Load and merge configuration files
    config = load_and_merge_configs(args.config_files)
    
    # Initialize DatabaseLoader
    db_loader = DatabaseLoader(config=config, db_type=args.db_type)
    
    try:
        db_loader.setup_logging(log_level)
        db_loader.connect()
        
        # Execute query and optionally output to a file
        db_loader.execute_query(args.query, output_file=args.output_file)
    
    finally:
        db_loader.disconnect()
