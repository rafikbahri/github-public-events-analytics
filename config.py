import configparser


class Config:
        def __init__(self):
            config=configparser.ConfigParser()
            config.read('config/app.conf')
            self.database_url=config.get('Firebase','DatabaseURL')
            self.hdfs_storage_path=config.get('HDFS','StoragePath')
            self.github_url=config.get('GitHub','URL')
            self.github_oauth_token=config.get('GitHub','OAuthToken')

