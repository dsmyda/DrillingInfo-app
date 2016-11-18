"""

@author: Danny Smyda
@date: 11-17-16

"""

import requests
import json
import logging
import os
from csv import DictWriter

def retrieve(thread, request_url, api_key, type, state, progress, ps, pn):
    """ This function calls the DrillingInfo API and stores the result to disk. """
    
    handler = logging.FileHandler("{}{}.log".format(state, type))
    handler.setFormatter(logging.Formatter("%(asctime)s;%(levelname)s;%(message)s",
                             "%Y-%m-%d %H:%M:%S"))
    log = logging.getLogger("{}{}".format(state, type))
    log.addHandler(handler)
    log.setLevel(logging.CRITICAL)

    file_path = state + type + ".json"
    header_f = None
    
    if type == "prodm":
        try:
            header_f = open(state + "prodh.json", 'rb')
        except FileNotFoundError:
            log.critical("type: %s specified, but unable to locate production header file as \"%sprodh.json\"\n"
                        "Please move the file into the current directory." % (type, state))
            return

    with open(file_path, 'a') as f:  # 'a' flag to append to created file, or to create if it does not already exist
        if header_f:
            size = estimate_requests_num(header_f)   #estimate our file size before processing
            header_f.seek(0)     #reset file obj
            for obj in json_gen(header_f):
                id = obj["entity_id"]
                process(thread, f, request_url, api_key, id, progress, ps, pn, log, size)
                #Check to see if user wishes to kill thread
                if thread.getTerminationFlag:
                    break
            header_f.close()
        else:
            process(thread, f, request_url, api_key, state, progress, ps, pn, log)


def json_gen(f):
    """ Parses the json array file object f, and yields each constituent json object for processing."""

    cnt = 0
    json_byte_array = bytearray()

    byte = f.read(1)
    while byte:
        if byte == b'{':
            cnt += 1
        if cnt > 0:
            json_byte_array += byte
        if byte == b'}':
            cnt -= 1
        if cnt == 0 and json_byte_array:
            yield json.loads(json_byte_array.decode())
            json_byte_array = bytearray()

        byte = f.read(1)


def estimate_requests_num(h):
    """ Estimate the number of requests that will need to be done on prodm retrievals. Optimized as much as
    possible with buffer read strategy.

    :return: line count = request count
    """

    lines = 0
    buffer_size = 1024 * 1024
    read_h = h.read

    buffer = read_h(buffer_size)
    while buffer:
        lines += buffer.count(b'{')
        buffer = read_h(buffer_size)

    return lines


def process(thread, f, request_url, api_key, var, progress, ps, pn, log, size=None):
    """ Append to the file object f the results from the call"""

    cnt = 0.0
    last_obj = None

    f.write("[")
    for result in request_gen(thread, request_url, api_key, var, ps, pn, log):
        if last_obj:
            json.dump(last_obj,f)
            f.write(",")
            cnt = update(cnt, size, progress)

        for obj in result[:-1]:
            json.dump(obj, f)
            f.write(",")
            cnt = update(cnt, size, progress)

        if result: last_obj = result[len(result) - 1]

    if last_obj: json.dump(last_obj, f)
    f.write("]")


def update(cnt, size, progress):
    """ Simply update the cnt variable and have that reflect in the progress object of the calling thread. """

    cnt += 1

    if size:
        progress.update(cnt / size)
    else:
        progress.update(cnt)

    return cnt


def request_gen(thread, request_url, api_key, var, ps, pn, log):
    """ A generator used to iterate through results returned from the API. This generator will be used
    to dump json results to disk in the retrieve function. """

    while True:
        response = get_response(request_url, api_key, var, ps, pn)

        if not response:
            break
        elif response.status_code != 200 or thread.getTerminationFlag():
            log.critical('%s, failed to successfully complete.' % response.url)
            break

        yield response.json()
        pn += 1


def get_response(request_url, api_key, var, ps, pn):
    """ This generalized response function provides functionality to all of the target API request urls.
    For the permit and production headers call, all we need to specify is the state, for the production monthly calls,
    we will need to use the entity_id. The variable "var" takes on either one of these instances.
    """

    return requests.get(url=request_url.format(var, pn, ps), headers={'X-API-KEY': api_key})


def convert_to_csv(fp):
    """ Converts the file object (.json) at fp into a .csv file within the current directory. This function can be called
    in the REPL."""

    try:
        new_fp = fp[:fp.index(".")] + ".csv"
        first = True
        with open(fp, 'rb') as input, open(new_fp, 'w') as output:
            for obj in json_gen(input):
                writer = DictWriter(output, obj.keys())
                if first: writer.writeheader()
                first = False
                writer.writerow(obj)
    except (FileNotFoundError, ValueError):
        print("File at {} was not found. Make sure it has extension .json and is in the current directory: {}.".format(fp, os.getcwd()))
