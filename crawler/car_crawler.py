class BaseCrawler:
    
    def __init__(self, site_name=None, base_url=None, username=None, password=None):
        self.site_name = site_name
        self.base_url = base_url
        self.username = username
        self.password = password
        self.session = None

    def update_credentials(self, base_url, username, password):
        raise NotImplementedError

    def pre_login(self):
        raise NotImplementedError

    def login(self,  check_code_id=None, verify_code=None):
        raise NotImplementedError

    def get_accident_cars(self, max_count=1000):
        raise NotImplementedError

    def get_used_cars(self, max_count=100):
        raise NotImplementedError
