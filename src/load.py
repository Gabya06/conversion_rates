import os
import sys
import pandas as pd
import string
from datetime import datetime as dt
from datetime import timedelta
import numpy as np
import itertools
import operator
from dateutil.relativedelta import relativedelta

# filename = 'Opp_Created_and_Stage_Changes_v2.xlsx'
# filename = 'new_report_07052016.xlsx'
# filename = file_name
# xl = pd.ExcelFile(data_path + filename)

# # check out sheets
# print "Sheet names: ", xl.sheet_names
# sheet_name = xl.sheet_names[0]

# '''
# Read data file & partner ids
# '''
# data = xl.parse(sheet_name)

# partner_file = 'Opportunities_with_Partners_06282016.csv'
# partner_ids_exclude = pd.read_csv(data_path + partner_file)
# partner_ids_exclude.dropna(inplace = True)
# p_cols = partner_ids_exclude.columns
# p_cols = [clean_column(c) for c in p_cols]
# partner_ids_exclude.columns = p_cols
# # exclude partner opportunities
# id_exclude = partner_ids_exclude.OpportunityID.drop_duplicates()


'''
Prepare Stage information
'''
# actual stages
stages = ['Prospect', 'Qualified', 'Buying Process id.', 'Short List', 'Chosen Vendor', 'Negotiation/Review',
          'PO In Progress', 'Closed Won']
lost = ['Closed Deferred', 'Closed Lost']

# dictionary with (key,val) = (stage, probability)
probs = [10, 20, 40, 50, 70, 80, 90, 100, 0, 0]
opp_dict = dict(zip(stages, probs))

# create list of possible acceptable stages as a sequence (paired (oldvalue, newvalue)) that may be missing
opp_stages = zip(stages[::1], stages[1::1])
# IMPORTANT STAGES FOR CONVERSION RATES
impt_stages = list(itertools.combinations(stages[1:], 2))
# LIST OF POSSIBLE MISSING STAGES FOR CONVERSION RATES
stages_missing = [i for j, i in enumerate(impt_stages) if j not in [0, 6, 11, 15, 18, 20]]

# dictionary to filter out which stages are to be considered in nominator of conversion rate calculation
stage_dict = {'Qualified' : impt_stages[0:6], 'Buying Process id.' : impt_stages[6:11], 'Short List' : impt_stages[11:15],
              'Chosen Vendor' : impt_stages[15:18], 'Negotiation/Review' : impt_stages[18:20],
              'PO In Progress' : impt_stages[-1]}

to_stages_dict = {
    'Qualified': 'Qualified to Buying Process Id.',
    'Buying Process id.': 'Buying Process Id. to Short List',
    'Short List': 'Short List to Chosen Vendor',
    'Chosen Vendor': 'Chosen Vendor to Negotiation/Review',
    'Negotiation/Review': 'Negotiation/Review to PO In Progress',
    'PO In Progress': 'PO In Progress to Closed Won'}


# df.to_csv("~/Documents/dev/conv_rates/input_data/data_clean_07012016.csv", encoding = 'utf-8')
