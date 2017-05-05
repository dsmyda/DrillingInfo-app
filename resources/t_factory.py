"""

"""
import DrillingInfo, USGS
from Convert import Convert
from states import abbrev

TYPE_MAPPING = {
    'Permits': 'https://di-api.drillinginfo.com/v1/direct-access/permits?state_province={}&format=json&page={}&pagesize={}',
    'Production Headers': 'https://di-api.drillinginfo.com/v1/direct-access/producing-entities?state_province={}&format=json&page={}&pagesize={}',
    'Production Monthly': 'https://di-api.drillinginfo.com/v1/direct-access/producing-entities-details?entity_id={}&format=json&page={}&pagesize={}',
    'Earthquake Data': 'https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime=2014-01-01&endtime=2014-01-02'
}

def get_api_instance(type, state, api_key, start_page, page_size, save_fp, input_file, save_id, key, num =1):
    if type == 'Permits':
        return DrillingInfo.Perm(TYPE_MAPPING[type], state, start_page, page_size, api_key, save_fp, key, num)
    elif type == 'Production Headers':
        return DrillingInfo.Prodh(TYPE_MAPPING[type], abbrev(state).name, start_page, page_size, api_key, save_fp, key, num)
    elif type == 'Production Monthly':
        return DrillingInfo.Prodm(TYPE_MAPPING[type], state, start_page, page_size, api_key, input_file, save_fp, save_id, key, num)
    elif type == "Earthquake Data":
        return USGS.EQuake(None)

def get_convert_instance(format, src, dest):
    return Convert(format, src, dest)