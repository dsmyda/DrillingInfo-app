"""

"""

from prodm_worker import ProdmWorker
from prodh_worker import ProdhWorker
from perm_worker import PermWorker

from states import abbrev
#Factory class for worker initialization.

#Responsible for abstracting away particular initialization details from the user.

TYPE_MAPPING = {
    'Permits': 'https://di-api.drillinginfo.com/v1/direct-access/permits?state_province={}&format=json&page={}&pagesize={}',
    'Production Headers': 'https://di-api.drillinginfo.com/v1/direct-access/producing-entities?state_province={}&format=json&page={}&pagesize={}',
    'Production Monthly': 'https://di-api.drillinginfo.com/v1/direct-access/producing-entities-details?entity_id={}&format=json&page={}&pagesize={}'
}

def get_instance(type, state, api_key, start_page, page_size, save_fp, input_file):
    if type == 'Permits':
        return PermWorker(TYPE_MAPPING[type], state, start_page, page_size, api_key, save_fp)
    elif type == 'Production Headers':
        return ProdhWorker(TYPE_MAPPING[type], abbrev(state).name, start_page, page_size, api_key, save_fp)
    elif type == 'Production Monthly':
        return ProdmWorker(TYPE_MAPPING[type], start_page, page_size, api_key, input_file, save_fp)