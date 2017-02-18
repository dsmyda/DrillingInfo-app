from state_worker import State

class ProdhWorker(State):

    def __init__(self, destination_url, state, start_page, page_size, api_key, out_fp):
        self.file_name = state + self.type_abbrev() + ".json"
        super().__init__(destination_url, state, start_page, page_size, api_key, out_fp+self.path_delimiter()+self.file_name)

    def __repr__(self):
        return self.state+" "+self.type_abbrev()

    def progess(self):
        return self.completed()

    def type_abbrev(self):
        return "prodh"