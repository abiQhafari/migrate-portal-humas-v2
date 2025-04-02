import re 
from datetime import datetime
import requests
from base.base import BaseMigration
import time
from services.keycloak_service import KeycloakService
from config import Config

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
                
            regionId = None
            if index_region is not None:
                regionId = self.region_migration.list_regions[index_region]["afterId"]
            
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
                "region_id": regionId
            }
            
            result = KeycloakService().execute_with_retry(
                lambda token: requests.post(
                    self.host + "/api/v1/users",
                    json=payload,
                    headers={"Authorization": f"Bearer {token}"}
                )
            )
            
            if result and result.status_code == 201:
                
                return {
                    "beforeId": beforeId,
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
                    "afterId": result.json()["data"]["id"],
                }
            else:
                raise ValueError(f"API Error: {result}")
            
            
                
            # while True: 
                
            #     self.token = KeycloakService(config_instance).get_valid_token()
                
            #     if not self.token:
            #         print("Token not available, waiting...")
            #         time.sleep(5)
            #         continue
                
            #     response = requests.post(
            #         self.host + "/api/v1/users",
            #         json=payload,
            #         headers=self.headers,
            #     )
                
            #     if response.status_code == 201:
            #         return {
            #             "beforeId": beforeId,
            #             "fullName": full_name or f"{first_name} {last_name}".strip(),
            #             "email": re.sub(r'_deleted_.*$', '', email) or full_name+"@gmail.com",
            #             "password": "Ph" + username.capitalize() + "@123",
            #             "phoneNumber": phonenumber or "",
            #             "birthPlace": "",
            #             "birthDate": tgl_lahir.strftime("%Y-%m-%d") if tgl_lahir else datetime.now().strftime("%Y-%m-%d"),
            #             "groupName": groupName,
            #             "address": "",
            #             "hobby": "",
            #             "nrp": nrp or "",
            #             "isVerificator": is_content_verificator or False,
            #             "afterId": response.json()["data"]["id"],
            #         }
                
            #     elif response.status_code == 401:
            #         print("Token expired, waiting for token...")
            #         new_token = KeycloakService(config_instance).refresh_token()
                    
            #         if new_token:
            #             self.token = new_token
            #         time.sleep(5)
            #     else:
            #         raise ValueError(f"API Error: {response.text}")

            
        except Exception as e:
            raise Exception(f"Error creating user '{email}': {str(e)}")

    def migrate(self):
        file_name = "data/users.json"
        self.list_users = self.load_json(file_name) or []
        
        # Buat set untuk cek user yang sudah ada
        existing_user_ids = {user["beforeId"] for user in self.list_users}

        query = """
            SELECT au.id, au.username, au.first_name, au.last_name, au.email, 
                ue.nrp, ue.full_name, ue.general_role, ue.phonenumber, 
                ue.region_id, ue.tgl_lahir, ue.is_content_verificator 
            FROM auth_user au 
            LEFT JOIN users_userextend ue ON ue.user_id = au.id
            WHERE ue.is_deleted = false
            ORDER BY ue.general_role
        """
        
        results = self.query_data(query, 10000)
        list_migration_users = self.list_users.copy()
        
        for row in results:
            if row[0] not in existing_user_ids:
                try:
                    
                    new_user = self.make_user(*row)
                    self.logger.info(f"Migrated user: {new_user['email']}")
                    list_migration_users.append(new_user)
                    
                    if list_migration_users != self.list_users:
                        self.save_json(file_name, list_migration_users)
                        self.list_users = list_migration_users
                    
                except Exception as err:
                    self.log_migration_error({
                        "id": row[0],
                        "username": row[1],
                        "first_name": row[2],
                        "last_name": row[3],
                        "email": row[4],
                        "nrp": row[5],
                        "full_name": row[6],
                        "general_role": row[7],
                        "phonenumber": row[8],
                        "region_id": row[9],
                        "tgl_lahir": row[10],
                        "is_content_verificator": row[11]
                    }, err)
        

        self.save_error_log("users")