import json
import os
import logging
import psycopg2
import requests
from datetime import datetime

class BaseMigration:
    def __init__(self, token, host="http://localhost:3000"):
        self.token = token
        self.host = host
        self.headers = {"Authorization": "Bearer " + token}
        self.base_url = "https://is3.cloudhost.id/portalhumas/portal_humas_asset/media/"
        self.logger = self._setup_logger()
        self.connection = psycopg2.connect('dbname=portal-humas-backup user=abialqhafari')
        self.migration_errors = []
        
    def _setup_logger(self):
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def load_json(self, filename):
        if os.path.exists(filename):
            with open(filename, "r") as file:
                return json.load(file)
        return []
    
    def save_json(self, filename, data):
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        class DateTimeEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return super().default(obj)
        
        with open(filename, "w") as file:
            json.dump(data, file, indent=4, cls=DateTimeEncoder)
            
    def query_data(self, query, limit=None):
        list_rows = []
        cursor = self.connection.cursor()
        offset = 0

        try:
            while True:
                 # If limit is None, use pagination with 1000 records per fetch
                if limit is None:
                    paginated_query = f"{query} LIMIT 1000 OFFSET {offset}"
                    offset += 1000
                else:
                    # If limit is specified, fetch only that amount without pagination
                    paginated_query = f"{query} LIMIT {limit}"

                self.logger.info(f"Executing query: {paginated_query}")
                cursor.execute(paginated_query)
                rows = cursor.fetchall()

                if not rows:
                    break

                list_rows.extend(rows)
                
                # If limit is specified, break after first fetch
                if limit is not None:
                    break

        finally:
            cursor.close()

        return list_rows
    
    def save_error_log(self, module_name):
        if self.migration_errors:
            error_file = f"data/error_{module_name}.json"
            self.save_json(error_file, self.migration_errors)
            self.logger.info(f"Saved {len(self.migration_errors)} errors to {error_file}")
            
    def log_migration_error(self, data, error):
        error_entry = {
            "data": data,
            "error": str(error),
            "timestamp": datetime.now().isoformat()
        }
        self.migration_errors.append(error_entry)
        self.logger.error(f"Migration error: {error}")