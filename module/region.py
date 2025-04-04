from base.base import BaseMigration
from services.keycloak_service import KeycloakService

import requests
import time

class RegionMigration(BaseMigration):
    def __init__(self, token):
        super().__init__(token)
        self.list_regions = []

    def make_regions(self, beforeId, territory, level, parent_id, category, description, slug, icon, address, sequence):
        try:
            index_map = {
                item["beforeId"]: index
                for index, item in enumerate(self.list_regions)
                if item is not None
            }
            index = index_map.get(parent_id)
            parent_region_id = None
            if index is not None:
                parent_region_id = self.list_regions[index]["afterId"]
                
            payload = {
                "name": territory,
                "icon": icon,
                "address": address,
                "sortIndex": sequence if sequence > -1 else -1,
                "description": description,
                "slug": slug,
                "level": category.split(" ")[0].upper(),
                "parentId": parent_region_id,
            }
                
            result = KeycloakService().execute_with_retry(
                lambda token: requests.post(
                    self.host + "/api/v1/regions",
                    json=payload,
                    headers={"Authorization": f"Bearer {token}"},
                )
            )
            if result.status_code == 201:
                return {
                    "beforeId": beforeId,
                    "name": territory,
                    "icon": icon,
                    "address": address,
                    "sortIndex": sequence if sequence > -1 else -1,
                    "description": description,
                    "slug": slug,
                    "level": category.split(" ")[0].upper(),
                    "parentId": parent_id,
                    "afterId": result.json()["data"]["id"],
                }
            
            else:
                raise ValueError(f"API Error: {result.text}")
            
        except Exception as err:
            raise ValueError(f"Failed to create region: {err}")

    def migrate(self):
        file_name = "data/regions.json"
        self.list_regions = self.load_json(file_name)
        
        existing_region_ids = {item["beforeId"] for item in self.list_regions}
        
        for level in range(4):
            query = f"SELECT id, territory, level, parent_id, category, description, slug, icon, address, sequence FROM regions_regions WHERE level = {level} AND is_deleted = false"
            
            while True:
                results = self.query_data(query, 1000)

                if results:
                    break
                
                print(f"Query level {level} kosong, mencoba ulang dalam 5 detik...")
                time.sleep(5)

            list_migration_regions = self.list_regions.copy()
            
            for row in results:
                if row[0] not in existing_region_ids:
                    try:
                        new_regions = self.make_regions(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
                        self.logger.info(f"Migrated region: {new_regions}")
                        list_migration_regions.append(new_regions)
                    except Exception as err:
                        self.log_migration_error({
                            "id": row[0],
                            "territory": row[1],
                            "level": row[2],
                            "parent_id": row[3],
                            "category": row[4],
                            "description": row[5],
                            "slug": row[6],
                            "icon": row[7],
                            "address": row[8],
                            "sequence": row[9]
                        }, err)
                        
            if list_migration_regions != self.list_regions:
                self.save_json(file_name, list_migration_regions)
                self.list_regions = list_migration_regions
            
        self.save_error_log("region")
        