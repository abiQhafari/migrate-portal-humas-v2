from base.base import BaseMigration
import requests
import time
from services.keycloak_service import KeycloakService

class SocmedRegionMigration(BaseMigration):
    def __init__(self, token, region_migration, social_media_migration):
        super().__init__(token)
        self.list_socmed_regions = []
        self.region_migration = region_migration
        self.social_media_migration = social_media_migration

    def make_socmed_region(self, beforeId, account_username, engagement_rate, followers_count, followings_count, posts_count, external_id, is_scrape, social_media_id, region_id):
        try:
            index_map_regions = {
                item["beforeId"]: index
                for index, item in enumerate(self.region_migration.list_regions)
                if item is not None
            }
            index_region = index_map_regions.get(region_id, None)
            
            if index_region is None:
                raise ValueError(f"Region with ID {region_id} not found")
            
            index_map_socmed = {
                item["beforeId"]: index
                for index, item in enumerate(self.social_media_migration.list_social_media)
                if item is not None
            }
            index_socmed = index_map_socmed.get(social_media_id, None)
            
            if index_socmed is None:
                raise ValueError(f"Social Media with ID {social_media_id} not found")
            
            regionId = None
            if index_region is not None:
                regionId = self.region_migration.list_regions[index_region]["afterId"]
                
            socmedId = None
            if index_socmed is not None:
                socmedId = self.social_media_migration.list_social_media[index_socmed]["afterId"]
            
                
            while True:

                self.token = KeycloakService.get_valid_token()
                
                if not self.token:
                    print("Token not available, waiting...")
                    time.sleep(5)
                    continue
                
                payload = {
                    "accountUsername": account_username or "",
                    "engagementRate": engagement_rate,
                    "totalFollowers": followers_count,
                    "totalFollowings": followings_count,
                    "totalPost": posts_count,
                    "externalId": external_id,
                    "isScrape": is_scrape,
                    "socmed_id": socmedId,
                    "region_id": regionId
                }
                
                response = requests.post(
                    self.host + "/api/v1/socmed-regions",
                    json=payload,
                    headers=self.headers,
                )
                
                if response.status_code == 201:
                    return {
                        "beforeId": beforeId,
                        "accountUsername": account_username,
                        "engagementRate": engagement_rate,
                        "totalFollowers": followers_count,
                        "totalFollowings": followings_count,
                        "totalPost": posts_count,
                        "externalId": external_id,
                        "isScrape": is_scrape,
                        "socmed_id": social_media_id,
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
            raise Exception(f"Error creating socmed region '{beforeId}: {err}")

    def migrate(self):
        file_name = "data/socmed_regions.json"
        self.list_socmed_regions = self.load_json(file_name)
        
        if not self.list_socmed_regions:
            list_migration_socmed_regions = []
            query = """
                SELECT id, account_username, engagement_rate, followers_count, followings_count, posts_count, external_id, is_scrape, social_media_id, region_id FROM region_social_media_regionsocialmedia WHERE is_deleted = false
            """
            
            results = self.query_data(query)
            for row in results:
                try:
                    
                    new_socmed_regions = self.make_socmed_region(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
                    self.logger.info(f"Migrated socmed region: {new_socmed_regions}")
                    list_migration_socmed_regions.append(new_socmed_regions)
                except Exception as err:
                    self.log_migration_error({
                        "id": row[0],
                        "account_username": row[1],
                        "engagement_rate": row[2],
                        "followers_count": row[3],
                        "followings_count": row[4],
                        "posts_count": row[5],
                        "external_id": row[6],
                        "is_scrape": row[7],
                        "social_media_id": row[8],
                        "region_id": row[9]
                    }, err)
                    
            if list_migration_socmed_regions:
                self.save_json(file_name, list_migration_socmed_regions)
                self.list_socmed_regions = list_migration_socmed_regions
                
            self.save_error_log("socmed_region")