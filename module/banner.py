from base.base import BaseMigration
import requests
import time
from services.keycloak_service import KeycloakService

class BannerMigration(BaseMigration):
    def __init__(self, token):
        super().__init__(token)
        self.list_banners = []
        
    def make_banner(self, beforeId, name, url, image, is_show, description):
        self.logger.info(f"Creating banner: {name}")
        
        try:
            payload = {
                "name": name,
                "redirectUrl": "https://p0rt4lhum45.0x1.space/id" if url=="#" else url,
                "imageUrl": self.base_url + image,
                "isActive": is_show,
                "description": "-" if description == None else description
            }
            
            result = KeycloakService().execute_with_retry(
                lambda token: requests.post(
                    self.host + "/api/v1/banners",
                    json=payload,
                    headers={"Authorization": f"Bearer {token}"},
                )
            )
            if result.status_code == 201:
                return {
                    "beforeId": beforeId,
                    "name": name,
                    "redirectUrl": url,
                    "imageUrl": image,
                    "isActive": is_show,
                    "description": description,
                    "afterId": result.json()["data"]["id"],
                }
                
            else:
                raise Exception(f"Failed to create banner: {result.json()}")
        except Exception as e:
            raise Exception(f"Failed to create banner. Error: {str(e)}")
        
        
    def migrate(self):
        file_name = "data/banners.json"
        self.list_banners = self.load_json(file_name)
        
        if not self.list_banners:
            list_migration_banners = []
            
            delete_query = """
                TRUNCATE TABLE IF EXISTS internal_apps_internalapps CASCADE RESTART IDENTITY
            """
            
            delete_results = self.delete_data(delete_query)
            
            query = """
                SELECT id, name, url, image, is_show, description FROM internal_apps_internalapps WHERE is_deleted = false
            """
            results = self.query_data(query, 1000)
            
            for row in results:
                try:
                    
                    new_banner = self.make_banner(row[0], row[1], row[2], row[3], row[4], row[5])                    
                    self.logger.info(f"Migrated banner: {new_banner}")
                    list_migration_banners.append(new_banner)
                    
                except Exception as err:
                    self.log_migration_error({
                        "id": row[0],
                        "name": row[1],
                        "url": row[2],
                        "image": row[3],
                        "is_show": row[4],
                        "description": row[5]
                    }, err)

            if list_migration_banners:
                self.save_json(file_name, list_migration_banners)
                self.list_banners = list_migration_banners
        
            self.save_error_log("banner")