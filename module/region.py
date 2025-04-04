from base.base import BaseMigration
from services.keycloak_service import KeycloakService

import requests
import time
import signal
import sys

class RegionMigration(BaseMigration):
    def __init__(self, token):
        super().__init__(token)
        self.list_regions = []
        self.is_interrupted = False
        
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
    def handle_interrupt(self, signum, frame):
        self.logger.warning("Received interrupt signal. Saving current progress...")
        self.is_interrupted = True
        self.save_checkpoint()
        sys.exit(1)
        
    def save_checkpoint(self):
        if self.list_regions:
            try:
                self.save_json("data/regions.json", self.list_regions)
                self.logger.info("Progress saved successfully")
            except Exception as e:
                self.logger.error(f"Failed to save progress: {str(e)}")
        

    def make_regions(self, beforeId, territory, level, parent_id, category, description, slug, icon, address, sequence):
        try:
            if parent_id is not None:
                parent_slug = None
                
                old_parent = self.query_one(f"SELECT id, slug FROM regions_regions WHERE id = {parent_id} AND is_deleted = false")
                
                if old_parent is None:
                    raise ValueError(f"Parent region with ID {parent_id} not found")
                
                parent_slug = old_parent[1]
                
                new_parent = KeycloakService().execute_with_retry(
                    lambda token: requests.get(
                        self.host + f"/api/v1/regions/{parent_slug}",
                        headers={"Authorization": f"Bearer {token}"},
                    )
                )
                
                if new_parent.status_code != 200:
                    raise ValueError(f"Failed to fetch parent region: {new_parent.text}")
                
                parent_id = new_parent.json()["data"]["id"]
                
            payload = {
                "name": territory if territory else category,
                "icon": icon if icon else "-",
                "address": address if address else "-",
                "sortIndex": sequence if sequence > -1 else -1,
                "description": description if description else "-",
                "slug": slug,
                "level": category.split(" ")[0].upper(),
                "parentId": parent_id,
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
            if self.is_interrupted:
                break
            
            query = f"SELECT id, territory, level, parent_id, category, description, slug, icon, address, sequence FROM regions_regions WHERE level = {level} AND is_deleted = false"
            
            while True:
                results = self.query_data(query, 1000)

                if results:
                    break
                
                print(f"Query level {level} kosong, mencoba ulang dalam 5 detik...")
                time.sleep(5)

            list_migration_regions = self.list_regions.copy()
            
            for row in results:
                if self.is_interrupted:
                    break
            
                if row[0] not in existing_region_ids:
                    try:
                        new_regions = self.make_regions(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
                        self.logger.info(f"Migrated region: {new_regions}")
                        list_migration_regions.append(new_regions)
                        
                        if len(list_migration_regions) % 10 == 0:
                            self.save_json(file_name, list_migration_regions)
                            self.list_regions = list_migration_regions
                            
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
        
    def delete_region(self):
        try:
            # Get all regions
            existing_region = KeycloakService.execute_with_retry(
                lambda token: requests.get(
                    self.host + "/api/v1/regions?limit=1000&orderBy=id&orderDirection=DESC",
                    headers={"Authorization": f"Bearer {token}"},
                )
            )
            
            if existing_region.status_code != 200:
                raise ValueError(f"Failed to fetch regions: {existing_region.text}")
                
            regions = existing_region.json()["data"]
            
            # Delete regions in reverse order (children first)
            for region in regions:
                try:
                    result = KeycloakService.execute_with_retry(
                        lambda token: requests.delete(
                            self.host + f"/api/v1/regions/{region['slug']}",
                            headers={"Authorization": f"Bearer {token}"},
                        )
                    )
                    
                    if result.status_code != 204:
                        self.logger.error(f"Failed to delete region {region['slug']}: {result.text}")
                    else:
                        self.logger.info(f"Region {region['slug']} deleted successfully")
                        
                except Exception as err:
                    self.logger.error(f"Error deleting region {region['slug']}: {str(err)}")
                    
                # Add small delay between deletions
                time.sleep(0.5)
                
        except Exception as err:
            self.logger.error(f"Delete regions operation failed: {str(err)}")
            raise
            