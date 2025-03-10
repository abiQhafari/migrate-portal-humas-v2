from base.base import BaseMigration
import requests

class WebsiteRegionMigration(BaseMigration):
    def __init__(self, token, region_migration):
        super().__init__(token)
        self.list_website_regions = []
        self.region_migration = region_migration

    def make_website_region(self, beforeId, name, url, region_id, parser, external_id):
        index_map_regions = {
            item["beforeId"]: index
            for index, item in enumerate(self.region_migration.list_regions)
            if item is not None
        }
        index_region = index_map_regions.get(region_id, None)
        
        payload = {
            "name": name or "",
            "url": url,
            "region_id": self.region_migration.list_regions[index_region]["afterId"] if index_region is not None else None,
        }
        
        response = requests.post(
            self.host + "/api/v1/web-regions",
            json=payload,
            headers=self.headers,
        )
        
        payload_scrape = {
            "name": name or "",
            "parser": parser or "",
            "isScrape": True,
            "clientScrapper": "http",
            "tag": "",
            "scheduleTime": 1,
            "url": url or "",
            "regionWebsite_id": response.json()["data"]["id"],
        }   
        
        # Mengirim request
        response_scrape = requests.post(
            self.host + "/api/v1/scrape",
            json=payload_scrape,
            headers=self.headers,
        )

        if response_scrape.status_code != 201 or "data" not in response_scrape.json():
            raise RuntimeError("Failed to create socmed region: " + response_scrape.text)

        return {
            "beforeId": beforeId,
            "name": name,
            "url": url,
            "region_id": region_id,
            "afterId": response.json()["data"]["id"],
        }      

    def migrate(self):
        file_name = "data/website_regions.json"
        self.list_website_regions = self.load_json(file_name)
        
        if not self.list_website_regions:
            try:
                list_migration_website_regions = []
                query = """
                    SELECT id, name, url, region_id, parser, external_id FROM regions_regionwebsite WHERE is_deleted = false
                """
                
                results = self.query_data(query)
                for row in results:
                    try:
                        new_website_region = self.make_website_region(row[0], row[1], row[2], row[3], row[4], row[5])
                        self.logger.info(f"Migrated website region: {new_website_region}")
                        list_migration_website_regions.append(new_website_region)
                    except Exception as err:
                        self.logger.error(f"Error migrating website region: {err}")
                
                self.save_json(file_name, list_migration_website_regions)
                self.list_website_regions = list_migration_website_regions

            except Exception as err:
                self.logger.error(f"Error in website region migration: {err}")