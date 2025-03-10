from base.base import BaseMigration
import requests

class SocmedRegionMigration(BaseMigration):
    def __init__(self, token, region_migration, social_media_migration):
        super().__init__(token)
        self.list_socmed_regions = []
        self.region_migration = region_migration
        self.social_media_migration = social_media_migration

    def make_socmed_region(self, beforeId, account_username, engagement_rate, followers_count, followings_count, posts_count, external_id, is_scrape, social_media_id, region_id):
        index_map_regions = {
            item["beforeId"]: index
            for index, item in enumerate(self.region_migration.list_regions)
            if item is not None
        }
        index_region = index_map_regions.get(region_id, None)
        
        index_map_socmed = {
            item["beforeId"]: index
            for index, item in enumerate(self.social_media_migration.list_social_media)
            if item is not None
        }
        index_socmed = index_map_socmed.get(social_media_id, None)
        
        payload = {
            "accountUsername": account_username or "",
            "engagementRate": engagement_rate,
            "totalFollowers": followers_count,
            "totalFollowings": followings_count,
            "totalPost": posts_count,
            "externalId": external_id,
            "isScrape": is_scrape,
            "socmed_id": self.social_media_migration.list_social_media[index_socmed]["afterId"],
            "region_id": self.region_migration.list_regions[index_region]["afterId"],
        }
        
        response = requests.post(
            self.host + "/api/v1/socmed-regions",
            json=payload,
            headers=self.headers,
        )

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

    def migrate(self):
        file_name = "data/socmed_regions.json"
        self.list_socmed_regions = self.load_json(file_name)
        
        if not self.list_socmed_regions:
            try:
                list_migration_socmed_regions = []
                query = "SELECT id, account_username, engagement_rate, followers_count, followings_count, posts_count, external_id, is_scrape, social_media_id, region_id FROM region_social_media_regionsocialmedia WHERE is_deleted = false"
                
                results = self.query_data(query)
                for row in results:
                    try:
                        new_socmed_regions = self.make_socmed_region(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
                        self.logger.info(f"Migrated socmed region: {new_socmed_regions}")
                        list_migration_socmed_regions.append(new_socmed_regions)
                    except Exception as err:
                        self.logger.error(f"Error migrating socmed region: {err}")
                        
                self.list_socmed_regions.extend(list_migration_socmed_regions)
                self.save_json(file_name, self.list_socmed_regions)

            except Exception as err:
                self.logger.error(f"Error in socmed region migration: {err}")