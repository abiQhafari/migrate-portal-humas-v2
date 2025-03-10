class Config:
    def __init__(self):
        self.config = {
            'KEYCLOAK_URL': 'https://keycloak.0x1.space',
            'KEYCLOAK_REALM': 'migration',
            'KEYCLOAK_CLIENT_ID': 'portal-humas',
            'KEYCLOAK_CLIENT_SECRET': 'tsg1RG62S2SSA1x09HTCjSEwtQwSVsEv'
        }
    
    def get(self, key):
        return self.config.get(key)