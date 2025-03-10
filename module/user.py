import re
from datetime import datetime
import requests
from base.base import BaseMigration

class UserMigration(BaseMigration):
    def __init__(self, token, region_migration):
        super().__init__(token)
        self.list_users = []
        self.region_migration = region_migration

    def make_user(self, beforeId, username, first_name, last_name, email, nrp, full_name, general_role, phonenumber, region_id, tgl_lahir, is_content_verificator):
        try:
            index_map_regions = {
                item["beforeId"]: index
                for index, item in enumerate(self.region_migration.list_regions)
                if item is not None
            }
            index_region = index_map_regions.get(region_id, None)
            
            if region_id and index_region is None:
                raise ValueError(f"Region with ID {region_id} not found")
            
            groupName = ""
    
            if general_role == "super_admin":
                groupName = "superadmin"
            elif general_role == "ppid":
                groupName  = "ppid"
            elif general_role == "anggota":
                groupName = "internal"
            elif general_role == "masyarakat":
                groupName = "citizen"
            else: 
                groupName = "pers"
            
            payload = {
                "fullName": full_name or f"{first_name} {last_name}".strip(),
                "email": re.sub(r'_deleted_.*$', '', email) or full_name+"@gmail.com",
                "password": "Ph" + username.capitalize() + "@123",
                "phoneNumber": phonenumber or "",
                "birthPlace": "",
                "birthDate": tgl_lahir.strftime("%Y-%m-%d") if tgl_lahir else datetime.now().strftime("%Y-%m-%d"),
                "groupName": groupName,
                "address": "",
                "hobby": "",
                "nrp": nrp or "",
                "isVerificator": is_content_verificator or False,
                "region_id": self.region_migration.list_regions[index_region]["afterId"] if index_region is not None else None,
            }
            
            response = requests.post(
                self.host + "/api/v1/users",
                json=payload,
                headers=self.headers,
            )
            
            if response.status_code != 201:
                raise ValueError(f"API Error: {response.text}")

            return {
                "beforeId": beforeId,
                "email": payload["email"],
                "fullName": payload["fullName"],
                "region_id": region_id,
                "afterId": response.json()["data"],
            }
            
        except Exception as e:
            raise Exception(f"Error creating user '{email}': {str(e)}")

    def migrate(self):
        file_name = "data/users.json"
        self.list_users = self.load_json(file_name)
        
        if not self.list_users:
            existing_user_ids = {user["beforeId"] for user in self.list_users}
            
            list_migration_users = self.list_users.copy()
                
            query = """
                SELECT au.id, au.username, au.first_name, au.last_name, au.email, 
                    ue.nrp, ue.full_name, ue.general_role, ue.phonenumber, 
                    ue.region_id, ue.tgl_lahir, ue.is_content_verificator 
                FROM auth_user au 
                LEFT JOIN users_userextend ue ON ue.user_id = au.id
                WHERE ue.is_deleted = false
            """
            
            results = self.query_data(query, 10000)
            for row in results:
                if row[0] not in existing_user_ids:  # Check if user hasn't been migrated
                    try:
                        new_user = self.make_user(*row)
                        self.logger.info(f"Migrated user: {new_user['email']}")
                        list_migration_users.append(new_user)
                    except Exception as err:
                        self.log_migration_error({
                            "id": row[0],
                            "email": row[4],
                            "username": row[1],
                            "full_name": row[6],
                            "region_id": row[9]
                        }, err)
            
            if list_migration_users:
                self.save_json(file_name, list_migration_users)
                self.list_users = list_migration_users
            
            self.save_error_log("users")