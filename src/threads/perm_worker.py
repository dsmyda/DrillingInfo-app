from state_worker import State

class PermWorker(State):

    #TODO: implement args
    def __init__(self, destination_url, state, start_page, page_size, api_key, file_path):
        self.file_name = state + self.type_abbrev() + self.file_ext()
        self.save_location = file_path+self.path_delimiter()+self.file_name
        super().__init__(destination_url, state, start_page, page_size, api_key, self.save_location)

    def __repr__(self):
        return self.state+" "+self.type_abbrev()

    def progess(self):
        return self.completed()

    def type_abbrev(self):
        return "perm"