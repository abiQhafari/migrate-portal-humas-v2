from module.banner import BannerMigration
from module.region import RegionMigration
from module.social_media import SocialMediaMigration
from module.socmed_region import SocmedRegionMigration
from module.user import UserMigration
from module.website_region import WebsiteRegionMigration
from module.article import ArticleMigration
from module.assignment import AssignmentMigration
from module.group_chat import GroupChatMigration
from module.assignment_submit import AssignmentSubmitMigration
from module.citizen_article import CitizenArticleMigration
from services.keycloak_service import KeycloakService
from config import Config

def main():
    config = Config()
    keycloak_service = KeycloakService(config)
    token = keycloak_service.execute_with_retry(lambda t: t)
    
    if not token:
        raise Exception("Failed to get initial token")
        
    print("Initial token obtained successfully")
    
    try:
        # banner_migration = BannerMigration(token)
        region_migration = RegionMigration(token)
        # social_media_migration = SocialMediaMigration(token)
        # socmed_region_migration = SocmedRegionMigration(token, region_migration, social_media_migration)
        # user_migration = UserMigration(token, region_migration)
        # website_region_migration = WebsiteRegionMigration(token, region_migration)
        article_migration = ArticleMigration(token, region_migration)
        # assignment_migration = AssignmentMigration(token, region_migration, user_migration)
        # groupchat_migration = GroupChatMigration(token, user_migration)
        # assignment_submit_migration = AssignmentSubmitMigration(token, user_migration, assignment_migration, groupchat_migration)
        # citizen_article_migration = CitizenArticleMigration(token, user_migration, region_migration)
        
        
        # Execute migrations in order
        # banner_migration.migrate()
        region_migration.migrate()
        # social_media_migration.migrate()
        # socmed_region_migration.migrate()
        # website_region_migration.migrate()
        article_migration.migrate()
        # user_migration.migrate()
        # assignment_migration.migrate()
        # groupchat_migration.migrate()
        # assignment_submit_migration.migrate()
        # citizen_article_migration.migrate()
        
    except Exception as e:
        print(f"Migration failed: {e}")

if __name__ == "__main__":
    main()