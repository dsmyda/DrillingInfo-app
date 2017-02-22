from entity_worker import Entity

class ProdmWorker(Entity):

    #TODO: implement args
    def __init__(self, destination_url, start_page, page_size, api_key, format, in_fp, out_fp):
        super().__init__(destination_url, start_page, page_size, api_key, format,
                         in_fp, out_fp)
        self.total = self.estimate_requests_num()

    def __repr__(self):
        return "Monthly production"

    def progess(self):
        return self.cnt / self.total

    def type_abbrev(self):
        return "prodm"