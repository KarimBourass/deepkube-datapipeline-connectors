class APIConnector:
    
    def __init__(self, settings):
        self.url = settings["url"]
        self.method = settings["method"]    