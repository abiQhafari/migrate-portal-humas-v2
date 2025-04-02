class Config:
    def __init__(self):
        
        print("Initializing Config")
        
        self.config = {
            'KEYCLOAK_URL': 'https://key.isilop.online/',
            'KEYCLOAK_REALM': 'humas-presisi',
            'KEYCLOAK_CLIENT_ID': 'portal-humas',
            'KEYCLOAK_CLIENT_SECRET': '0Xwko523qXNNoNHdxRwWLjw4k8ZNmiY4'
        }
    
    def get(self, key):
        return self.config.get(key)