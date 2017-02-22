"""

@author: Danny Smyda
@date: 11-17-16

"""
import requests
import time
import worker as w

class State(w.Worker):

    """ An basic worker thread.

    Subclasses are responsible for maintaining progress information
    and naming features. """

    def __init__(self, destination_url, state, start_page, page_size, api_key, format, out_fp):
        super().__init__(out_fp)

        self.destination_url = destination_url
        self.api_key = api_key

        self.format = format
        self.start_page = start_page
        self.page_size = page_size
        self.state = state

    @w.process
    def run(self):
        while True:
            self.suspend_here()         #Procedure will hang if suspended.

            response = requests.get(self.destination_url.format(self.state, self.start_page, self.page_size),
                                    headers={'X-API-KEY': self.api_key})

            if (response.status_code != 200): raise LookupError(response.url)
            response_json = response.json()
            if not response_json: break
            self.commit_results(response_json, self.format)

            self.start_page += 1

    def __repr__(self):
        return "State Worker"

    def progess(self):
        return "Unknown Progress"