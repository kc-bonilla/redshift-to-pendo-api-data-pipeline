""" * endpoint: API endpoint relative path, when added to the base URL, creates the full path
    * kind: type of record to be updated ('account' or 'visitor')
    * group: type of Pendo attribute to be updated ('agent' or 'custom')
    """


class Endpoint:
    def __init__(self, stream):
        self.stream = stream
        self.base_url = 'https://app.pendo.io'
        self.kinds = ['account', 'visitor']
        self.group = 'custom'
        self.kind = self.get_kind()
        self.endpoint = self.build_endpoint()
        self.url = self.build_url()
    
    # create and store {kind} for stream's url path
    def get_kind(self):
        for i in self.kinds:
            if i in self.stream:
                self.kind = i
            
        return self.kind
        
    # build stream's endpoint for requests to Pendo
    def build_endpoint(self):
        self.path = '/api/v1/metadata/{}/{}/value'
        self.endpoint = self.path.format(self.kind, self.group)
                
        return self.endpoint
        
    # build stream's url for requests to Pendo
    def build_url(self):
        self.url = self.base_url + self.endpoint
                
        return self.url
