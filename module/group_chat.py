from base.base import BaseMigration
import requests

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
            
            response = requests.post(
                self.host + "/api/v1/group-chats",
                json=payload,
                headers=self.headers
            )
            
            if response.status_code != 201:
                raise ValueError(f"API Error: {response.text}")

            return {
                "beforeId": beforeId,
                "name": payload["name"],
                "type": payload["type"],
                "user_id": user_id,
                "afterId": response.json()["data"]["id"],
            }
        except Exception as e:
            raise Exception(f"Error creating groupchat '{name}': {str(e)}")
            
    def migrate(self):
        file_name = "data/group-chats.json"
        self.list_group_chat = self.load_json(file_name)
        
        if not self.list_group_chat:
            list_migration_group_chat = []
            
            query = """
                SELECT id, channel_type, name, user_id FROM share_channels_sharechannel WHERE share_channels_sharechannel.is_deleted = false
            """
            
            results = self.query_data(query, 1000)
            for row in results:
                try:
                    new_groupchat = self.make_group_chat(*row)
                    self.logger.info(f"Migrated Groupchat: {new_groupchat['name']}")
                    list_migration_group_chat.append(new_groupchat)
                except Exception as err:
                    self.log_migration_error({
                        "id": row[0],
                        "name": row[2],
                        "type": row[1],
                        "user_id": row[3]
                    }, err)
            
            if list_migration_group_chat:
                self.save_json(file_name, list_migration_group_chat)
                self.list_group_chat = list_migration_group_chat
            
            self.save_error_log("groupchat")