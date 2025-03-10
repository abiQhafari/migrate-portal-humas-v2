from base.base import BaseMigration
import requests
from datetime import datetime

class ArticleMigration(BaseMigration):
    def __init__(self, token, region_migration):
        super().__init__(token)
        self.list_articles = []
        self.region_migration = region_migration

    def make_article(self, beforeId, created_at, updated_at,  title , image, video, source, is_public, publication_date, author_id, approved_by_id, caption, audio, source_audio, source_image, source_video, description, source_url, region_id, slug, status, rejected_note, image_metadata, video_metadata, init_viewer, init_like, init_share, kategori):
        try:
            index_map_regions = {
                item["beforeId"]: index
                for index, item in enumerate(self.region_migration.list_regions)
                if item is not None
            }
            index_region = index_map_regions.get(region_id, None)
            
            if index_region is None:
                raise ValueError(f"Region with ID {region_id} not found")
            
            
            payload = {
                "title": title or "",
                "slug": slug or "",
                "createdAt": created_at.strftime("%Y-%m-%d %H:%M:%S") if created_at else None,
                "updatedAt": updated_at.strftime("%Y-%m-%d %H:%M:%S") if updated_at else None,
                "image": self.base_url+image if image else source_image,
                "video": self.base_url+video if video else source_video,
                "audio": self.base_url+audio if audio else source_audio,
                "publicationDate": publication_date.strftime("%Y-%m-%d %H:%M:%S") if publication_date else None,
                "description": description or "",
                "caption": caption or "",
                "status": "PUBLISHED",
                "initialView": init_viewer,
                "initialLike": init_like,
                "initialShare": init_share,
                "sourceUrl": source_url or "",
                "isPublic": is_public or False,
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
                "title": title or "",
                "slug": slug or "",
                "createdAt": created_at,
                "updatedAt": datetime.now().strftime("%Y-%m-%d"),
                "image": image if image else source_image,
                "video": video if video else source_video,
                "audio": audio if audio else source_audio,
                "publicationDate": publication_date or None,
                "description": description or "",
                "caption": caption or "",
                "status": "PUBLISHED",
                "initialView": init_viewer,
                "initialLike": init_like,
                "initialShare": init_share,
                "sourceUrl": source_url or "",
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
        file_name = "data/articles.json"
        self.list_articles = self.load_json(file_name)
        
        if not self.list_articles:
            list_migration_articles = []
            
            query = """
                SELECT contents_articles.id, contents_articles.created_at, contents_articles.updated_at, title, image, video, source, contents_articles.is_public, publication_date, author_id, approved_by_id, caption, audio, source_audio, source_image, source_video, description, source_url,region_id, contents_articles.slug, status, rejected_note, image_metadata, video_metadata, init_viewer, init_like, init_share, c.name as kategory FROM contents_articles LEFT JOIN contents_articles_categories cac ON cac.articles_id = contents_articles.id LEFT JOIN contents_articlecategory c ON c.id = cac.articlecategory_id WHERE author_id IS NULL
            """
            
            results = self.query_data(query, 1000)
            for row in results:
                try:
                    new_article = self.make_article(*row)
                    self.logger.info(f"Migrated article: {new_article['title']}")
                    list_migration_articles.append(new_article)
                except Exception as err:
                    self.log_migration_error({
                        "id": row[0],
                        "title": row[3],
                        "region_id": row[19],
                        "content_preview": row[17][:100] if row[2] else None
                    }, err)
            
            if list_migration_articles:
                self.save_json(file_name, list_migration_articles)
                self.list_articles = list_migration_articles
            
            self.save_error_log("articles")