class BaseCrawler:
    def __init__(self, username=None, password=None):
        self.username = username
        self.password = password
        self.session = None

    def login(self):
        raise NotImplementedError

    def get_accident_cars(self, max_count=100):
        raise NotImplementedError

    def get_used_cars(self, max_count=100):
        raise NotImplementedError
