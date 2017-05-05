import csv
from t_http import HTTP_T, run_decorator

class __Entity(HTTP_T):
    def __init__(self, destination_url, start_page, page_size, api_key, in_fp, out_fp, save_id, key):
        super().__init__(out_fp, key)
        self.destination_url = destination_url
        self.api_key = api_key
        self.id = save_id
        self.start_page = start_page
        self.page_size = page_size
        self.in_file = in_fp
        self.req_cnt = None
        self.bad_status = False
        self.update_d(self.id, self.start_page, self.page_size)
    @run_decorator
    def run(self):
        self.data_collect(self.get_key_set())
        if self.is_running():
            missing_ids = self.scan_to_verify()
            while len(missing_ids) > 0:
                self.data_collect(missing_ids)
                missing_ids = self.scan_to_verify()
            self.finished()
    def data_collect(self, key_set):
        for e_id in key_set:
            self.id = e_id
            while self.is_running():
                self.suspend_here()  # Procedure will hang if suspended.
                response = self.retreive(self.destination_url.format(self.id, self.start_page, self.page_size))
                if response.status_code != 200:
                    self.safe_kill()
                    break
                response_json = response.json()
                if not response_json: break
                self.write(response_json, self.id, self.start_page, self.page_size)
                self.start_page += 1
            if not self.is_running(): break
            self.start_page = 1
        if self.is_running(): self.finished()
    def scan_to_verify(self):
        missing = []
        with open(self.in_file, 'r') as head:
            with open(self.atom._t_fd) as res:
                pass
        return missing
    def get_key_set(self):
        with open(self.in_file, "r") as cf:
            found = False
            csv_reader = csv.DictReader(cf)
            for r in csv_reader:
                if self.id:
                    if not found and self.id == r["entity_id"]:
                        found = True
                else:
                    found = True
                if found:
                    yield r["entity_id"]
                self.completed()
    def __repr__(self):
        return "Entity Worker"
    def progress(self):
        if not self.req_cnt: self.req_cnt = self.master.return_fileCnt(self.in_file)
        self.req_cnt = 1 if self.req_cnt == 0 else self.req_cnt
        return "{0:.1f}%".format(self.get_cnt() * 100.0 / self.req_cnt)
    def get_id(self): return self.id
    def get_sp(self): return self.start_page
    def get_ps(self): return self.page_size

class Prodm(__Entity):
    def __init__(self, destination_url, state, start_page, page_size, api_key, in_fp, out_fp, save_id, key, num):
        self.state = state
        self.file_name = self.state + self.type_abbrev() + str(key) + ".csv"
        super().__init__(destination_url, start_page, page_size, api_key,
                         in_fp, out_fp + self.path_delimiter() + self.file_name, save_id, key)
        self.set_descriptor(self.file_name)
    def __repr__(self):
        return self.state+" "+self.type_abbrev() + str(self.get_key())
    def type_abbrev(self):
        return "prodm"

class __State(HTTP_T):
    def __init__(self, destination_url, state, start_page, page_size, api_key, out_fp, key):
        super().__init__(out_fp, key)
        self.destination_url = destination_url
        self.api_key = api_key
        self.start_page = start_page
        self.page_size = page_size
        self.set_cnt(start_page)
        self.id = "/"
        self.state = state
        self.req_cnt = 1
        self.update_d(self.id, self.start_page, self.page_size)
    @run_decorator
    def run(self):
        while self.is_running():
            self.suspend_here()  # Procedure will hang if suspended.
            response = self.retreive(self.destination_url.format(self.state, self.start_page, self.page_size))
            if response.status_code != 200: break
            response_json = response.json()
            if not response_json: break
            self.write(response_json, self.id, self.start_page, self.page_size)
            self.completed()
            self.start_page += 1
        if self.is_running(): self.finished()
    def progress(self):
        if not self.req_cnt:
            #Pull from pickled dictionary
            pass
        return "{0:.1f}%".format(self.get_cnt() * 100.0 / self.req_cnt)
    def get_id(self): return self.id
    def get_sp(self): return self.start_page
    def get_ps(self): return self.page_size

class Prodh(__State):
    def __init__(self, destination_url, state, start_page, page_size, api_key, out_fp, key, num):
        self.file_name = state + self.type_abbrev() + str(key) +".csv"
        super().__init__(destination_url, state, start_page, page_size, api_key,
                         out_fp+self.path_delimiter()+self.file_name, key)
        self.set_descriptor(self.file_name)
    def __repr__(self):
        return self.state+" "+self.type_abbrev() + str(self.get_key())
    def type_abbrev(self):
        return "prodh"

class Perm(__State):
    def __init__(self, destination_url, state, start_page, page_size, api_key, file_path, key, num):
        self.file_name = state + self.type_abbrev() + str(key) + ".csv"
        self.save_location = file_path+self.path_delimiter()+self.file_name
        super().__init__(destination_url, state, start_page, page_size, api_key, self.save_location, key)
        self.set_descriptor(self.file_name)
    def __repr__(self):
        return self.state+" "+self.type_abbrev() + str(self.get_key())
    def type_abbrev(self):
        return "perm"