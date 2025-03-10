import requests
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from config import Config

class KeycloakService:
    def __init__(self, config: Config):
        self.access_token = ''
        self.url = config.get('KEYCLOAK_URL')
        self.realm = config.get('KEYCLOAK_REALM')
        self.client_id = config.get('KEYCLOAK_CLIENT_ID')
        self.client_secret = config.get('KEYCLOAK_CLIENT_SECRET')
        
        # Initialize scheduler
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(
            self.get_access_token,
            'cron',
            minute='*/5'  # Run every 5 minutes
        )
        self.scheduler.start()

    def init(self):
        """Initialize the service by getting the first access token"""
        self.access_token = self.get_access_token()
        
    def get_access_token(self):
        """Get access token from Keycloak"""
        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'client_credentials'
        }
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        response = requests.post(
            f"{self.url}/realms/{self.realm}/protocol/openid-connect/token",
            data=data,
            headers=headers
        )
        
        response.raise_for_status()  # Raise exception for bad status codes
        self.access_token = response.json()['access_token']
        return self.access_token

    def __del__(self):
        """Cleanup scheduler when object is destroyed"""
        if hasattr(self, 'scheduler'):
            self.scheduler.shutdown()