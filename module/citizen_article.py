from base.base import BaseMigration
import requests
from datetime import datetime

class CitizenArticleMigration(BaseMigration):
    def __init__(self, token, user_migration, region_migration):
        super().__init__(token)
        self.list_citizen_articles = []
        self.user_migration = user_migration
        self.region_migration = region_migration

    def make_citizen_article(self, beforeId, created_at, updated_at,  title , image, video, source, is_public, publication_date, author_id, approved_by_id, caption, audio, source_audio, source_image, source_video, description, source_url, region_id, slug, status, rejected_note, image_metadata, video_metadata, init_viewer, init_like, init_share, kategori):
        try:
            index_map_regions = {
                item["beforeId"]: index
                for index, item in enumerate(self.region_migration.list_regions)
                if item is not None
            }
            index_region = index_map_regions.get(region_id, None)
            
            if index_region is None:
                raise ValueError(f"Region with ID {region_id} not found")
            
            index_map_user = {
                item["beforeId"]: index
                for index, item in enumerate(self.user_migration.list_users)
                if item is not None
            }
            
            print(title)
            
            print(author_id)
            print(approved_by_id)
            
            index_user = index_map_user.get(author_id, None)
            
            if author_id and index_user is None:
                raise ValueError(f"User with ID {author_id} not found")
            
            index_approved_by = index_map_user.get(approved_by_id, None)
            
            print(index_user)
            print(index_approved_by)
            
            formatted_created_at = created_at.strftime("%Y-%m-%dT%H:%M:%S.%f") if created_at else None
            formatted_updated_at = updated_at.strftime("%Y-%m-%dT%H:%M:%S.%f") if updated_at else None
            formatted_publication_date = publication_date.strftime("%Y-%m-%dT%H:%M:%S.%f") if publication_date else None
            
            author_keycloak = None
            approved_by_keycloak = None
            
            if index_user is not None:
                author_keycloak = self.user_migration.list_users[index_user]["afterId"]
            
            if index_approved_by is not None:
                approved_by_keycloak = self.user_migration.list_users[index_approved_by]["afterId"]
            
            
            payload = {
                "authorKeycloak" : author_keycloak,
                "approvedBy" : approved_by_keycloak,
                "title": title if title else "",
                "slug": slug if slug else "",
                "createdAt": formatted_created_at,
                "updatedAt": formatted_updated_at,
                "image": self.base_url+image if image else source_image,
                "video": self.base_url+video if video else source_video,
                "audio": self.base_url+audio if audio else source_audio,
                "publicationDate": formatted_publication_date,
                "description": description if description else title,
                "caption": caption if caption else title,
                "status": "PUBLISHED",
                "initialView": init_viewer,
                "initialLike": init_like,
                "initialShare": init_share,
                "isPublic": is_public if is_public else False,
                "region_id": self.region_migration.list_regions[index_region]["afterId"],
                "categories": [
                    kategori if kategori else "Terbaru"
                ]
            }
            
            response = requests.post(
                self.host + "/api/v1/articles",
                json=payload,
                headers=self.headers,
            )
            
            if response.status_code != 201:
                raise ValueError(f"API Error: {response.text}")

            return {
                "beforeId": beforeId,
                "authorKeycloak" : author_keycloak,
                "approvedBy" : approved_by_keycloak,
                "title": title or "",
                "slug": slug or "",
                "createdAt": formatted_created_at,
                "updatedAt": formatted_updated_at,
                "image": self.base_url+image if image else source_image,
                "video": self.base_url+video if video else source_video,
                "audio": self.base_url+audio if audio else source_audio,
                "publicationDate": formatted_publication_date,
                "description": description or "",
                "caption": caption or "",
                "status": "PUBLISHED",
                "initialView": init_viewer,
                "initialLike": init_like,
                "initialShare": init_share,
                "isPublic": is_public or False,
                "region_id": self.region_migration.list_regions[index_region]["afterId"],
                "categories": [
                    kategori if kategori else "Terbaru"
                ],
                "afterId": response.json()["data"]["id"],
            }           
        except Exception as e:
            raise Exception(f"Error creating article '{title}': {str(e)}")

    def migrate(self):
        file_name = "data/citizen_articles.json"
        self.list_citizen_articles = self.load_json(file_name)
        
        if not self.list_citizen_articles:
            list_migration_citizen_articles = []
            
            query = """
                SELECT contents_articles.id, contents_articles.created_at, contents_articles.updated_at, title, image, video, source, contents_articles.is_public, publication_date, author_id, approved_by_id, caption, audio, source_audio, source_image, source_video, description, source_url,region_id, contents_articles.slug, status, rejected_note, image_metadata, video_metadata, init_viewer, init_like, init_share, c.name as kategory FROM contents_articles LEFT JOIN contents_articles_categories cac ON cac.articles_id = contents_articles.id LEFT JOIN contents_articlecategory c ON c.id = cac.articlecategory_id WHERE author_id IS NOT NULL AND contents_articles.is_deleted = false
            """
            
            results = self.query_data(query, 1000)
            for row in results:
                try:
                    new_article = self.make_citizen_article(*row)
                    self.logger.info(f"Migrated article: {new_article['title']}")
                    list_migration_citizen_articles.append(new_article)
                except Exception as err:
                    self.log_migration_error({
                        "id": row[0],
                        "title": row[3],
                    }, err)
            
            if list_migration_citizen_articles:
                self.save_json(file_name, list_migration_citizen_articles)
                self.list_articles = list_migration_citizen_articles
            
            self.save_error_log("citizenarticles")