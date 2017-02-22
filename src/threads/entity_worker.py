"""

@author: Danny Smyda
@date: 11-17-16

"""
import requests
import worker as w

class Entity(w.Worker):

    """ An basic worker thread.

    Subclasses are responsible for maintaining progress information
    and naming features. """

    def __init__(self, destination_url, start_page, page_size, api_key, format, in_fp, out_fp):
        super().__init__(out_fp, format)

        self.destination_url = destination_url
        self.api_key = api_key

        self.start_page = start_page
        self.page_size = page_size

        self.in_file = in_fp

    @w.process
    def run(self):

        #TODO: MapReduce framework
        #partition
        #spawn threads for each partition
        #write results to file using same buffer

        for e_id in self.get_key_set():
            self.suspend_here()

            response = requests.get(self.destination_url.format(e_id, self.start_page, self.page_size),
                                    headers={'X-API-KEY': self.api_key})

            if (response.status_code != 200): raise LookupError(response.url)
            response_json = response.json()
            if not response_json: break
            self.commit_results(response_json)

            self.start_page += 1

    def estimate_requests_num(self):
        with open(self.in_file, 'rb') as f:
            requests = 0
            buffer_size = 1024 * 1024
            read_h = f.read

            buffer = read_h(buffer_size)
            while buffer:
                requests += buffer.count(b'{')
                buffer = read_h(buffer_size)

            return requests

    def map(self, key_set):
        pass

    def reduce(self):
        pass

    def get_key_set(self):
        #TODO get all entity id's from file
        return []

    def partition(self):
        pass

    def __repr__(self):
        return "Entity Worker"

    def progess(self):
        return "Unknown Progress"