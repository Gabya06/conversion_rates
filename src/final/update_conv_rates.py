"""
script to truncate conversion rates to 4 decimal places
in Conversion Rate object in SalesForce (not Windows machine, locally)
use beatbox to query and update data
"""

import os
import numpy as np
import beatbox

os.chdir("/Users/Gabi/Documents/dev/conv_rates/")
data_path = os.getcwd() + '/data/input_data/'

def login_beatbox():
    """
    Function to login to SF via Beatbox
    :return: svc beatbox connections
    """
    sf_user = 'john.angerami@collibra.com'
    sf_token = 'TFAB49jVswWVquu75y9nAklhl'
    sf_pass = 'Fiske!418'
    sf_pass_token = sf_pass + sf_token

    # instantiate object & login
    sf = beatbox._tPartnerNS
    svc = beatbox.PythonClient()
    svc.login(sf_user, sf_pass_token)
    dg = svc.describeGlobal()
    return svc




def get_object_fields(svc, sf_object):
    '''
    function to get SalesForce object fields

    :param svc: beatbox connection
    :param sf_object: object to get description
    :return: list of column names

    ex: obj_desc = svc.describeSObjects("Conversion_Rate__c")
    '''

    obj_desc = svc.describeSObjects(sf_object)[0]
    names = [name for name in obj_desc.fields]
    return names



def query_data(svc, query_string):
    '''
    Function to query data and return results

    :param svc: beatbox connection
    :param query_string: query string
    :return: list of dictionaries with data
    '''
    record_list = []
    query_res = svc.query(query_string)
    record_list += query_res['records']

    # if need to get more records
    if query_res.done == False:
        print " ******** FETCHING MORE DATAS ********"
        query_res = svc.queryMore(query_res.queryLocator)
        record_list += query_res

    return record_list

def update_data(svc, list_of_records):
    '''
    Function to update a list of records via beatbox
    Truncate conversion rates to 4 decimals

    :param svc: beatbox connection
    :param list_of_records: list of records to update, where each record is a dictionary
    '''
    for rec in list_of_records:
        conv_id = rec['Id']
        conv_rate = rec['ConversionRate__c']
        trunc_rate = np.float(str(conv_rate)[:6])
        rec_update = {'ConversionRate__c': trunc_rate, 'type': 'Conversion_Rate__c', 'Id': conv_id}

        # update eacb record
        results_updated = svc.update(rec_update)

        if results_updated[0]['success'] == True:
            print "Success truncating conversion rate"
        else:
            err_stat = results_updated[0]['errors'][0]['statusCode']
            print err_stat
            pass

svc = login_beatbox()
s = "SELECT Id, ConversionRate__c from Conversion_Rate__c"
results = query_data(svc, s)
update_data(svc, list_of_records=results)





