class BaseCrawler:
    def __init__(self, base_url=None, username=None, password=None):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.site_name = None
        self.session = None

    def login(self):
        raise NotImplementedError

    def get_accident_cars(self, max_count=100):
        raise NotImplementedError

    def get_used_cars(self, max_count=100):
        raise NotImplementedError
