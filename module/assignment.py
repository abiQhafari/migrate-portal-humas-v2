from base.base import BaseMigration

import requests

class AssignmentMigration(BaseMigration):
    def __init__(self, token, region_migration, user_migration):
        super().__init__(token)
        self.list_assignments = []
        self.region_migration = region_migration
        self.user_migration = user_migration
        
    def make_assignment(self, beforeId, title, description, assignment_url, assignment_number, created_by_id, attachment, region_ids):
        try:
            if isinstance(region_ids, str):
                region_ids = [int(region_id.strip()) for region_id in region_ids.split(",") if region_id.strip().isdigit()]
            
            index_map_regions = {
                item["beforeId"]: index
                for index, item in enumerate(self.region_migration.list_regions)
                if item is not None
            }
            
            # Konversi region_ids dari sistem lama ke ID baru
            index_regions = [index_map_regions.get(region_id, None) for region_id in region_ids]
            
            index_map_user = {
                item["beforeId"]: index
                for index, item in enumerate(self.user_migration.list_users)
                if item is not None
            }
            
            index_user = index_map_user.get(created_by_id, None)
            
            payload = {
                "title": title,
                "description": description,
                "url": assignment_url,
                "assignmentCode": assignment_number,
                "createdBy": self.user_migration.list_users[index_user]["afterId"] if index_user is not None else None,
                "attachment": attachment,
                "region_id": [
                    self.region_migration.list_regions[index]["afterId"] 
                    for index in index_regions if index is not None
                ],
            }
            
            response = requests.post(
                self.host + "/api/v1/assignments",
                json=payload,
                headers=self.headers,
            )
            
            return {
                "beforeId": beforeId,
                "title": title,
                "description": description,
                "url": assignment_url,
                "assignmentCode": assignment_number,
                "attachment": attachment,
                "createdBy": created_by_id,
                "region_id": region_ids,
                "afterId": response.json()["data"]["id"],
            }
        except Exception as e:
            raise Exception(f"Failed to create assignment: {title}. Error: {str(e)}")
    
    def migrate(self):
        file_name = "data/assignments.json"
        self.list_assignments = self.load_json(file_name)
        
        if not self.list_assignments:
            list_migration_assignments = []
            
            query = """
                SELECT id, title, description, assignment_url, assignment_number, created_by_id, attachment, region_ids from assignments_assignment where assignments_assignment.is_deleted = false
            """
            
            results = self.query_data(query, 1000)
            for row in results:
                try:
                    new_assignment = self.make_assignment(*row)
                    self.logger.info(f"Migrated assignment: {new_assignment['title']}")
                    list_migration_assignments.append(new_assignment)
                except Exception as err:
                    self.log_migration_error({
                        "id": row[0],
                        "title": row[1],
                        "description": row[2],
                        "assignment_url": row[3],
                        "assignment_number": row[4],
                        "created_by_id": row[5],
                        "attachment": row[6],
                        "region_ids": row[7]
                    }, err)
                    
            if list_migration_assignments:
                self.save_json(file_name, list_migration_assignments)
                self.list_assignments = list_migration_assignments
                
            self.save_error_log("assignment")
        