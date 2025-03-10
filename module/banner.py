from base.base import BaseMigration
import requests

class BannerMigration(BaseMigration):
    def __init__(self, token):
        super().__init__(token)
        self.list_banners = []
        
    def make_banner(self, beforeId, name, url, image, is_show, description):
        self.logger.info(f"Creating banner: {name}")
        
        response = requests.post(
            self.host + "/api/v1/banners",
            json={
                "name": name,
                "redirectUrl": "https://p0rt4lhum45.0x1.space/id" if url=="#" else url,
                "imageUrl": self.base_url + image,
                "isActive": is_show,
                "description": "-" if description == None else description
            },
            headers=self.headers,
        )

        return {
            "beforeId": beforeId,
            "name": name,
            "redirectUrl": url,
            "imageUrl": image,
            "isActive": is_show,
            "description": description,
            "afterId": response.json()["data"]["id"],
        }
        
    def migrate(self):
        file_name = "data/banners.json"
        self.list_banners = self.load_json(file_name)
        
        if not self.list_banners:
            try:
                list_migration_banners = []
                query = "SELECT id, name, url, image, is_show, description FROM internal_apps_internalapps"
                results = self.query_data(query,)
                
                for row in results:
                    new_banner = self.make_banner(row[0], row[1], row[2], row[3], row[4], row[5])
                    self.logger.info(f"Migrated banner: {new_banner}")
                    list_migration_banners.append(new_banner)

                self.save_json(file_name, list_migration_banners)
                self.list_banners = list_migration_banners

            except Exception as err:
                self.logger.error(f"Error in banner migration: {err}")