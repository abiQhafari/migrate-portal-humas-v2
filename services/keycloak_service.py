import requests
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from config import Config
import threading
import time

class KeycloakService:
    _instance = None
    _instance_lock = threading.Lock()
    _global_token = None
    _scheduler = None
    _initialized = False
    
    def __new__(cls, config: Config = None):
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance
    
    def __init__(self, config: Config = None):

        print("Initializing KeycloakService")
        
        with self.__class__._instance_lock:
            if self.__class__._initialized:
                print("KeycloakService already initialized")
                return
            
            self.config = config or Config()
            
            self.access_token = None
            self.url = self.config.get('KEYCLOAK_URL')
            self.realm = self.config.get('KEYCLOAK_REALM')
            self.client_id = self.config.get('KEYCLOAK_CLIENT_ID')
            self.client_secret = self.config.get('KEYCLOAK_CLIENT_SECRET')
            
            # Initialize scheduler only once
            if not self.__class__._scheduler:
                
                self.__class__._scheduler = BackgroundScheduler()
                    
                print("Starting scheduler")
                
                self.__class__._scheduler.add_job(
                    self.refresh_token,
                    'interval',
                    minutes=5
                )
                
                self.__class__._scheduler.start()
            
            self.__class__._initialized = True
            
        print("KeycloakService initialized")
            
    def get_access_token(self):
        """Get access token from Keycloak"""
        
        print("Getting access token")
        
        try:
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
            
            if response.status_code == 200:
                self.access_token = response.json()['access_token']
                return self.access_token
            else:
                print(f"Failed to get token: {response.status_code}")
                return None
        except Exception as e:
            print(f"Exception getting token: {str(e)}")
            return None
    
    @classmethod
    def get_valid_token(cls):
        """Get the current valid token"""
        if not cls._global_token:
            # If no token exists, try to get one
            instance = cls()
            instance.refresh_token()
        return cls._global_token
    
    def refresh_token(self):
        """Get new token from Keycloak and update global token"""
        with self.__class__._instance_lock:
            try:
                new_token = self.get_access_token()
                if new_token:
                    self.__class__._global_token = new_token
                    self.access_token = new_token
                    return new_token
            except Exception as e:
                print(f"Error refreshing token: {str(e)}")
        return None

    def __del__(self):
        """Cleanup scheduler when object is destroyed"""
        if hasattr(self.__class__, '_scheduler') and self.__class__._scheduler:
            self.__class__._scheduler.shutdown()
            
    @staticmethod
    def execute_with_retry(api_call_function):
        """
        Execute an API call with automatic token refresh on 401 errors
        
        Usage example:
        result = KeycloakService.execute_with_retry(lambda token: make_api_call(token))
        """
        attempt = 1
        
        print("Executing with retry")
        
        while True:
            token = KeycloakService.get_valid_token()
            if not token:
                print(f"Token not available, waiting... (Attempt {attempt})")
                time.sleep(5)
                attempt += 1
                continue
                
            try:
                result = api_call_function(token)
                
                if isinstance(result, requests.Response):
                    if result.status_code == 401:
                        print(f"Token expired, refreshing... (Attempt {attempt})")
                        KeycloakService()._instance.refresh_token()
                        time.sleep(2)
                        attempt += 1
                        continue
                    if result.status_code != 201:
                        raise Exception(f"API Error: {result.text}")
                return result
            except Exception as e:
                print(f"Unexpected error: {str(e)} (Attempt {attempt})")
                time.sleep(5)
                attempt += 1
                raise e
                # break