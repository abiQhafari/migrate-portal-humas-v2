from base.base import BaseMigration
import requests
import time
from services.keycloak_service import KeycloakService

class SocialMediaMigration(BaseMigration):
    def __init__(self, token):
        super().__init__(token)
        self.list_social_media = []

    def make_social_media(self, beforeId, name, base_profile_url, base_post_url):
            
        while True:
            self.token = KeycloakService.get_valid_token()
            
            if not self.token:
                print("Token not available, waiting...")
                time.sleep(5)
                continue
            
            response = requests.post(
                self.host + "/api/v1/socmeds",
                json={
                    "name": name,
                    "baseProfileUrl": "" if base_profile_url=="" else base_profile_url,
                    "basePostUrl": base_post_url,
                    "baseScrapeUrl": base_profile_url
                },
                headers=self.headers,
            )
            
            if response.status_code == 201:
                return {
                    "beforeId": beforeId,
                    "name": name,
                    "baseProfileUrl": base_profile_url,
                    "basePostUrl": base_post_url,
                    "baseScrapeUrl": base_profile_url,
                    "afterId": response.json()["data"]["id"],
                }
            elif response.status_code == 401:
                print("Token expired, waiting for token...")
                KeycloakService().refresh_token()
                time.sleep(5)
            else:
                raise ValueError(f"API Error: {response.text}")

    def migrate(self):
        file_name = "data/social_media.json"
        self.list_social_media = self.load_json(file_name)
        
        if not self.list_social_media:
            try:
                list_migration_social_media = []
                query = "SELECT id, name, base_profile_url, base_post_url FROM social_medias_socialmedia"
                results = self.query_data(query)
                
                for row in results:
                    
                    new_socmeds = self.make_social_media(row[0], row[1], row[2], row[3])
                    self.logger.info(f"Migrated social media: {new_socmeds}")
                    list_migration_social_media.append(new_socmeds)

                self.save_json(file_name, list_migration_social_media)
                self.list_social_media = list_migration_social_media

            except Exception as err:
                self.logger.error(f"Error in social media migration: {err}")