from base.base import BaseMigration
import requests
import time
from services.keycloak_service import KeycloakService

class SocialMediaMigration(BaseMigration):
    def __init__(self, token):
        super().__init__(token)
        self.list_social_media = []

    def make_social_media(self, beforeId, name, base_profile_url, base_post_url):
            
        try:
            payload = {
                "name": name,
                "baseProfileUrl": base_profile_url if base_profile_url else "-",
                "basePostUrl": base_post_url,
                "baseScrapeUrl": base_profile_url
            }
            
            result = KeycloakService().execute_with_retry(
                lambda token: requests.post(
                    self.host + "/api/v1/socmeds",
                    json=payload,
                    headers={"Authorization": f"Bearer {token}"},
                )
            )
            
            if result.status_code == 201:
                return {
                    "beforeId": beforeId,
                    "name": name,
                    "baseProfileUrl": base_profile_url,
                    "basePostUrl": base_post_url,
                    "baseScrapeUrl": base_profile_url,
                    "afterId": result.json()["data"]["id"],
                }
                
            else:
                raise ValueError(f"API Error: {result.text}")
        except Exception as err:
            raise ValueError(f"Failed to create social media: {err}")

    def migrate(self):
        file_name = "data/social_media.json"
        self.list_social_media = self.load_json(file_name)
        
        existing_social_media_ids = {item["beforeId"] for item in self.list_social_media}
        
        if not self.list_social_media:
            
            query = """
                SELECT id, name, base_profile_url, base_post_url FROM social_medias_socialmedia WHERE is_deleted = false
            """
            
            while True:
                results = self.query_data(query, 1000)
                if results:
                    break
                
                print(f"Query Ksosong, mencoba ulang dalam 5 detik...")
                time.sleep(5)
                
            list_migration_social_media = []
            
            for row in results:
                
                if row[0] not in existing_social_media_ids:
                    try:    
                        new_socmeds = self.make_social_media(row[0], row[1], row[2], row[3])
                        self.logger.info(f"Migrated social media: {new_socmeds}")
                        list_migration_social_media.append(new_socmeds)
                        
                        if len(list_migration_social_media) % 10 == 0:
                            self.save_json(file_name, list_migration_social_media)
                            self.list_social_media = list_migration_social_media
                    
                    except Exception as err:
                        self.log_migration_error({
                            "id": row[0],
                            "name": row[1],
                            "base_profile_url": row[2],
                            "base_post_url": row[3]
                        }, err)
                    
            if list_migration_social_media:
                self.save_json(file_name, list_migration_social_media)
                self.list_social_media = list_migration_social_media
            
            self.save_error_log("social_media")