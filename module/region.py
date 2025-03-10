from base.base import BaseMigration

import requests

class RegionMigration(BaseMigration):
    def __init__(self, token):
        super().__init__(token)
        self.list_regions = []

    def make_regions(self, beforeId, territory, level, parent_id, category, description, slug, icon, address, sequence):
        index_map = {
            item["beforeId"]: index
            for index, item in enumerate(self.list_regions)
            if item is not None
        }
        index = index_map.get(parent_id, None)
        
        response = requests.post(
            self.host + "/api/v1/regions",
            json={
                "name": "" if territory==None else territory,
                "icon": self.base_url + icon,
                "address": "" if address==None else address,
                "sortIndex": -1 if sequence > -1 else sequence,
                "description": "" if description==None else description,
                "slug": slug,
                "level": category.split(" ")[0].upper(),
                "parentRegion_id": None if parent_id == None else self.list_regions[index]["afterId"]
            },
            headers=self.headers,
        )

        return {
            "beforeId": beforeId,
            "name": territory,
            "icon": icon,
            "address": address,
            "sortIndex": -1 if sequence > -1 else sequence,
            "description": description,
            "slug": slug,
            "level": category[0],
            "parentId": parent_id,
            "afterId": response.json()["data"]["id"],
        }

    def migrate(self):
        file_name = "data/regions.json"
        self.list_regions = self.load_json(file_name)
        
        if not self.list_regions:
            try:
                for level in range(4):  # 0: Root, 1: Polda, 2: Polres, 3: Polsek
                    list_migration_regions = []
                    query = f"SELECT id, territory, level, parent_id, category, description, slug, icon, address, sequence FROM regions_regions WHERE level = {level} AND is_deleted = false"
                    
                    results = self.query_data(query)
                    for row in results:
                        try:
                            new_regions = self.make_regions(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
                            self.logger.info(f"Migrated region: {new_regions}")
                            list_migration_regions.append(new_regions)
                        except Exception as err:
                            self.logger.error(f"Error migrating region: {err}")
                            
                    self.list_regions.extend(list_migration_regions)

                self.save_json(file_name, self.list_regions)

            except Exception as err:
                self.logger.error(f"Error in region migration: {err}")