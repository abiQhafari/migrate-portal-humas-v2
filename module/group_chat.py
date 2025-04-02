from base.base import BaseMigration
import requests
import time

from services.keycloak_service import KeycloakService

class GroupChatMigration(BaseMigration):
    def __init__(self, token, user_migration):
        super().__init__(token)
        self.list_group_chat = []
        self.user_migration = user_migration
        
    def make_group_chat(self, beforeId, channel_type, name, user_id):
        try:
            index_map_users = {
                item["beforeId"]: index
                for index, item in enumerate(self.user_migration.list_users)
                if item is not None
            }
            index_user = index_map_users.get(user_id, None)
            
            if user_id and index_user is None:
                raise ValueError(f"User with ID {user_id} not found")
            
            payload = {
                "name": name,
                "type": channel_type.upper(),
                "user_keycloak": self.user_migration.list_users[index_user]["afterId"] if index_user is not None else None,
            }
                
            while True:
                
                self.token = KeycloakService.get_valid_token()
                
                if not self.token:
                    print("Token not available, waiting...")
                    time.sleep(5)
                    continue
                
                response = requests.post(
                    self.host + "/api/v1/group-chats",
                    json=payload,
                    headers=self.headers
                )
                
                if response.status_code == 201:
                    return {
                        "beforeId": beforeId,
                        "name": name,
                        "type": channel_type,
                        "user_id": user_id,
                        "afterId": response.json()["data"]["id"],
                    }
                elif response.status_code == 401:
                    print("Token expired, waiting for token...")
                    KeycloakService().refresh_token()
                    time.sleep(5)
                else:
                    raise ValueError(f"API Error: {response.text}")
        
        except Exception as e:
            raise Exception(f"Error creating groupchat '{name}': {str(e)}")
            
    def migrate(self):
        file_name = "data/group-chats.json"
        self.list_group_chat = self.load_json(file_name) or []
        
        existing_group_chat_ids = {item["beforeId"] for item in self.list_group_chat}
        
        query = """
            SELECT id, channel_type, name, user_id 
            FROM share_channels_sharechannel 
            WHERE share_channels_sharechannel.is_deleted = false
        """
        
        results = self.query_data(query, 1000)
        list_migration_group_chat = self.list_group_chat.copy()
        
        for row in results:
            if row[0] not in existing_group_chat_ids:  # Hanya migrasi jika belum ada
                try:
                    new_groupchat = self.make_group_chat(*row)
                    self.logger.info(f"Migrated Group Chat: {new_groupchat['name']}")
                    list_migration_group_chat.append(new_groupchat)
                except Exception as err:
                    self.log_migration_error({
                        "id": row[0],
                        "name": row[2],
                        "type": row[1],
                        "user_id": row[3]
                    }, err)
        
        if list_migration_group_chat != self.list_group_chat:
            self.save_json(file_name, list_migration_group_chat)
            self.list_group_chat = list_migration_group_chat
            
        self.save_error_log("groupchat")