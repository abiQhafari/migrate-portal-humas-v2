from base.base import BaseMigration
import requests
import time
from services.keycloak_service import KeycloakService

class WebsiteRegionMigration(BaseMigration):
    def __init__(self, token, region_migration):
        super().__init__(token)
        self.list_website_regions = []
        self.region_migration = region_migration

    def make_website_region(self, beforeId, name, url, region_id, parser, external_id):
        try:
            index_map_regions = {
                item["beforeId"]: index
                for index, item in enumerate(self.region_migration.list_regions)
                if item is not None
            }
            index_region = index_map_regions.get(region_id, None)
            
            if index_region is None:
                raise ValueError(f"Region with ID {region_id} not found")
            
            regionId = None
            
            if index_region is not None:
                regionId = self.region_migration.list_regions[index_region]["afterId"]
            
            payload = {
                "name": name or "",
                "url": url,
                "region_id": regionId
            }
                
            while True:
                
                self.token = KeycloakService.get_valid_token()
                
                if not self.token:
                    print("Token not available, waiting...")
                    time.sleep(5)
                    continue
                
                response = requests.post(
                    self.host + "/api/v1/web-regions",
                    json=payload,
                    headers=self.headers,
                )
                
                if response.status_code == 201:
                    return {
                        "beforeId": beforeId,
                        "name": name,
                        "url": url,
                        "region_id": region_id,
                        "afterId": response.json()["data"]["id"],
                    }
                elif response.status_code == 401:
                    print("Token expired, waiting for token...")
                    KeycloakService().refresh_token()
                    time.sleep(5)
                else:
                    raise ValueError(f"API Error: {response.text}")
            
        except Exception as err:
            raise Exception(f"Error creating website region '{beforeId}': {err}")

    def migrate(self):
        file_name = "data/website_regions.json"
        self.list_website_regions = self.load_json(file_name) or []
        
        # Buat set untuk menyimpan ID yang sudah ada di JSON
        existing_website_region_ids = {item["beforeId"] for item in self.list_website_regions}

        query = """
            SELECT id, name, url, region_id, parser, external_id 
            FROM regions_regionwebsite 
            WHERE is_deleted = false
        """
        
        results = self.query_data(query, 1000)
        list_migration_website_regions = self.list_website_regions.copy()

        for row in results:
            if row[0] not in existing_website_region_ids:  # Hanya migrasi jika belum ada
                try:
                    new_website_region = self.make_website_region(*row)
                    self.logger.info(f"Migrated website region: {new_website_region['name']}")
                    list_migration_website_regions.append(new_website_region)
                except Exception as err:
                    self.log_migration_error({
                        "id": row[0],
                        "name": row[1],
                        "url": row[2],
                        "region_id": row[3],
                        "parser": row[4],
                        "external_id": row[5]
                    }, err)

        if list_migration_website_regions != self.list_website_regions:
            self.save_json(file_name, list_migration_website_regions)
            self.list_website_regions = list_migration_website_regions

        self.save_error_log("website_region")
        
        # if not self.list_website_regions:
        #     list_migration_website_regions = []
        #     query = """
        #         SELECT id, name, url, region_id, parser, external_id FROM regions_regionwebsite WHERE is_deleted = false
        #     """
            
        #     results = self.query_data(query, 1000)
        #     for row in results:
        #         try:
        #             new_website_region = self.make_website_region(row[0], row[1], row[2], row[3], row[4], row[5])
        #             self.logger.info(f"Migrated website region: {new_website_region}")
        #             list_migration_website_regions.append(new_website_region)
        #         except Exception as err:
        #             self.log_migration_error({
        #                 "id": row[0],
        #                 "name": row[1],
        #                 "url": row[2],
        #                 "region_id": row[3],
        #                 "parser": row[4],
        #                 "external_id": row[5]
        #             }, err)
            
        #     if list_migration_website_regions:
        #         self.save_json(file_name, list_migration_website_regions)
        #         self.list_website_regions = list_migration_website_regions
                
        #     self.save_error_log("website_region")