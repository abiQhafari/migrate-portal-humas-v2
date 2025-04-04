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
            
            if region_id is not None:
                old_region = self.query_one(f"SELECT id FROM regions_regions WHERE id = {region_id} AND is_deleted = false")
                
                if old_region is None:
                    raise ValueError(f"Region with ID {region_id} not found")
                
                new_regions = KeycloakService().execute_with_retry(
                    lambda token: requests.get(
                        self.host + f"/api/v1/regions/{old_region[0]}",
                        headers={"Authorization": f"Bearer {token}"},
                    )
                )
                
                if new_regions.status_code != 200:
                    raise ValueError(f"Failed to fetch region: {new_regions.text}")
                
                region_id = new_regions.json()["data"]["id"]

            else:
                raise ValueError(f"Social Media Region dengan ID {beforeId} tidak memiliki region_id")
            
            if social_media_id is not None:
                old_social_media = self.query_one(f"SELECT id FROM social_medias_socialmedia WHERE id = {social_media_id} AND is_deleted = false")
                
                if old_social_media is None:
                    raise ValueError(f"Social Media with ID {social_media_id} not found")
                
                new_social_media = KeycloakService().execute_with_retry(
                    lambda token: requests.get(
                        self.host + f"/api/v1/socmeds/{old_social_media[0]}",
                        headers={"Authorization": f"Bearer {token}"},
                    )
                )
                
                if new_social_media.status_code != 200:
                    raise ValueError(f"Failed to fetch social media: {new_social_media.text}")
                
                social_media_id = new_social_media.json()["data"]["id"]
            else:
                raise ValueError(f"Social Media Region dengan ID {beforeId} tidak memiliki social_media_id")
                
            
            payload = {
                "accountUsername": account_username or "",
                "engagementRate": engagement_rate,
                "totalFollowers": followers_count,
                "totalFollowings": followings_count,
                "totalPost": posts_count,
                "externalId": external_id,
                "isScrape": is_scrape,
                "socmed_id": social_media_id,
                "region_id": region_id,
            }
            
            result = KeycloakService().execute_with_retry(
                lambda token: requests.post(
                    self.host + "/api/v1/socmed-regions",
                    json=payload,
                    headers={"Authorization": f"Bearer {token}"},
                )
            )
            
            if result.status_code == 201:
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
                    "afterId": result.json()["data"]["id"],
                }
                
            else:
                raise ValueError(f"API Error: {result.text}")
            
        except Exception as err:
            raise ValueError(f"Failed to create socmed region '{beforeId}: {err}")
        

    def migrate(self):
        file_name = "data/socmed_regions.json"
        self.list_socmed_regions = self.load_json(file_name)
        
        existing_socmed_region_ids = {item["beforeId"] for item in self.list_socmed_regions}
        
        if not self.list_socmed_regions:
            
            list_migration_socmed_regions = []
            
            query = """
                SELECT id, account_username, engagement_rate, followers_count, followings_count, posts_count, external_id, is_scrape, social_media_id, region_id FROM region_social_media_regionsocialmedia WHERE is_deleted = false
            """
            
            results = self.query_data(query)
            
            for row in results:
                
                if row[0] in existing_socmed_region_ids:
                    try:
                        new_socmed_regions = self.make_socmed_region(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
                        self.logger.info(f"Migrated socmed region: {new_socmed_regions}")
                        list_migration_socmed_regions.append(new_socmed_regions)
                        
                        if len(list_migration_socmed_regions) % 100 == 0:
                            self.save_json(file_name, list_migration_socmed_regions)
                            self.list_socmed_regions = list_migration_socmed_regions
                            
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