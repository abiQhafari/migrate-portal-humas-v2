from base.base import BaseMigration
from collections import defaultdict
from typing import List, Dict, Any

import requests

class AssignmentSubmitMigration(BaseMigration):
    def __init__(self, token, user_migration, assignment_migration, group_chat_migration):
        super().__init__(token)
        self.list_assignment_submits = []
        self.user_migration = user_migration
        self.assignment_migration = assignment_migration
        self.group_chat_migration = group_chat_migration
        
    def group_evidence_by_user(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:  # Added self parameter
        grouped_data = defaultdict(lambda: {
            "status": "",
            "note": "",
            "approved_date": None,
            "approved_by_id": None,
            "assignment_id": None,
            "user_id": None,
            "work_date": None,
            "evidence": []
        })

        for row in data:
            user_id = row["id"]
            if not grouped_data[user_id]["status"]:  # Set once
                grouped_data[user_id].update({
                    "status": row["status"],
                    "note": row["note"],
                    "approved_date": row["approved_date"],
                    "approved_by_id": row["approved_by_id"],
                    "assignment_id": row["assignment_id"],
                    "user_id": row["user_id"],
                    "work_date": row["work_date"]
                })
            
            if row["evidence"]:  # Avoid None
                evidence_data = {
                    "evidence": row["evidence"],
                    "share_channel_id": row["share_channel_id"]
                }
                grouped_data[user_id]["evidence"].append(evidence_data)

        return [{"id": k, **v} for k, v in grouped_data.items()]
        
    def make_assignment_submit(self, beforeId, status, note, approved_date, approved_by_id, assignment_id, user_id, work_date, evidence_list):
        try:
            index_map_assignments = {
                item["beforeId"]: index
                for index,item in enumerate(self.assignment_migration.list_assignments)
                if item is not None
            }
            
            index_assignment = index_map_assignments.get(assignment_id, None)
            
            if assignment_id and index_map_assignments is None:
                raise ValueError(f"Assignment with ID {assignment_id} not found")
            
            
            index_map_users = {
                item["beforeId"]: index
                for index, item in enumerate(self.user_migration.list_users)
                if item is not None
            }
            
            index_user = index_map_users.get(user_id, None)
            index_approved_by = index_map_users.get(approved_by_id, None)
            
            
            if user_id is None or index_user is None:
                raise ValueError(f"User with ID {user_id} not found")
            
            parsed_status = ""
            
            if status == "approved":
                parsed_status = "APPROVED"
            elif status == "rejected":
                parsed_status = "REJECTED"
            elif status == "pending":
                parsed_status = "PENDING"
            
            # Add group chat mapping
            index_map_groupchats = {
                item["beforeId"]: item["afterId"]
                for item in self.group_chat_migration.list_group_chat
                if item is not None
            }
            
            # Process evidence list
            processed_evidence = []
            for evidence_item in evidence_list:
                evidence_data = {
                    "attachment": evidence_item["evidence"],
                    "groupChat_id": index_map_groupchats.get(evidence_item["share_channel_id"]) if evidence_item.get("share_channel_id") else None
                }
                processed_evidence.append(evidence_data)
                
            formatted_work_date = work_date.isoformat() if work_date else None
            formatted_approved_date = approved_date.isoformat() if approved_date else None
            
            payload = {
                "assignment_id": self.assignment_migration.list_assignments[index_assignment]["afterId"] if index_assignment is not None else None,
                "user_keycloak": self.user_migration.list_users[index_user]["afterId"] if index_user is not None else None,
                "submittedAt": formatted_work_date,
                "approvedAt": formatted_approved_date,
                "status": parsed_status,
                "note": note,
                "approvedBy": self.user_migration.list_users[index_approved_by]["afterId"] if index_approved_by is not None else None,
                "evidence": processed_evidence
            }
            
            response = requests.post(
                self.host + "/api/v1/assignment-submits",
                json=payload,
                headers=self.headers
            )
            
            if response.status_code != 201:
                raise ValueError(f"API Error: {response.text}")

            return {
                "beforeId": beforeId,
                "status": parsed_status,
                "note": note,
                "evidence": processed_evidence,
                "afterId": response.json()["data"]["id"],
            }
            
        except Exception as e:
            raise Exception(f"Failed to create assignment submit. Error: {str(e)}")
            
    def migrate(self):
        file_name = "data/assignment_submits.json"
        self.list_assignment_submits = self.load_json(file_name)
        
        if not self.list_assignment_submits:
            list_migration_assignment_submits = []
            
            query = """
                SELECT
                    au.id AS id,
                    au.status AS status,
                    au.note AS note,
                    au.approved_date AS approved_date,
                    au.approved_by_id AS approved_by_id,
                    au.assignment_id AS assignment_id,
                    au.user_id AS user_id,
                    au.work_date AS work_date,
                    ae.evidence AS evidence,
                    ae.share_channel_id AS share_channel_id
                FROM assignments_assignmentuser AS au
                LEFT JOIN assignments_assignmentevidence AS ae
                ON au.id = ae.assignment_user_id
                WHERE au.status != 'assigned' AND ae.share_channel_id IS NOT NULL
                AND au.is_deleted = false
            """
            
            results = self.query_data(query, 1000)
            
            # Convert results to list of dictionaries
            result_dicts = [
                {
                    "id": row[0],
                    "status": row[1],
                    "note": row[2],
                    "approved_date": row[3],
                    "approved_by_id": row[4],
                    "assignment_id": row[5],
                    "user_id": row[6],
                    "work_date": row[7],
                    "evidence": row[8],
                    "share_channel_id": row[9]
                }
                for row in results
            ]
            
            grouped_results = self.group_evidence_by_user(result_dicts)
            
            for group in grouped_results:
                try:
                    
                    new_assignment_submit = self.make_assignment_submit(
                        group["id"],
                        group["status"],
                        group["note"],
                        group.get("approved_date"),
                        group.get("approved_by_id"),
                        group.get("assignment_id"),
                        group.get("user_id"),
                        group.get("work_date"),
                        group["evidence"],
                    )
                    self.logger.info(f"Migrated assignment submit: {new_assignment_submit['beforeId']}")
                    list_migration_assignment_submits.append(new_assignment_submit)
                except Exception as err:
                    print("Error: ", err)
                    self.log_migration_error({
                        "id": group["id"],
                        "status": group["status"],
                        "note": group["note"],
                        "approved_date": group.get("approved_date"),
                        "approved_by_id": group.get("approved_by_id"),
                        "assignment_id": group.get("assignment_id"),
                        "user_id": group.get("user_id"),
                        "work_date": group.get("work_date"),
                        "evidence": group["evidence"]
                    }, err)
                    
            if list_migration_assignment_submits:
                self.save_json(file_name, list_migration_assignment_submits)
                self.list_assignment_submits = list_migration_assignment_submits
                
            self.save_error_log("assignment_submit")

