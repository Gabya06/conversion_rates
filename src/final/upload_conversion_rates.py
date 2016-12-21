import os
import pandas as pd
from datetime import datetime as dt
import numpy as np
import itertools
from dateutil.relativedelta import relativedelta
import MySQLdb # for datamart upload
#import pymssql -  not using this
import pyodbc # for salesforce backups query


# set directory and project path
os.chdir("C:\SalesOps\ConversionRates")
project_dir = 'C:\\SalesOps\\ConversionRates\\'
data_dir = project_dir


def get_query(t):
    t_str = str(t)
    t_start = t - relativedelta(months=12)  # T-12
    t_start_str = str(t_start)
    q = "select ofh.OpportunityId, " + "'" + t_start_str + "' as ModifiedDateBegin, " + "'" + t_str + "' as EndDate, "
    q += "ofh.CreatedDate as EditDate, o.CreatedDate, o.LastModifiedDate, ofh.Field, "
    q += "ofh.OldValue, ofh.NewValue, o.Name, o.StageName, u.Name "
    q += "from OpportunityFieldHistory as ofh "
    q += "inner join Opportunity as o "
    q += "on ofh.OpportunityId = o.Id "
    q += "inner join [User] as u "
    q += "on u.Id = o.OwnerId "
    q += "where ofh.Field in ('StageName', 'created','Created by lead convert', 'opportunityCreatedFromLead') "
    q += "and o.Type = 'New Business' "
    q += "and ofh.CreatedDate > = " + "'" + t_start_str + "' "
    q += "and ofh.CreatedDate <= " + "'" + t_str + "';"
    return q


def clean_column(column_name):
    '''
    :param column_name: string to clean
    :return: string (column name cleaned)
    '''
    col_name = column_name.replace(' ', '').replace('[^\w\s]', '').replace('\\', '').replace('/', '')
    return col_name


def clean_dat(dat):
    # type: (DataFrame) -> DataFrame
    '''
    :param dat: DataFrame with columns:EditDate, OldValue, NewValue, FieldEvent, StageName, CreatedDate
    :return: DataFrame with OldNewStage, cleaned columns
    '''

    cols = dat.columns
    cols = [clean_column(c) for c in cols]
    dat.columns = cols

    dat.EditDate = pd.to_datetime(dat.EditDate)
    dat.loc[:, 'EditYear'] = dat.EditDate.map(lambda x: x.year)
    dat.loc[:, 'EditMonth'] = dat.EditDate.map(lambda x: x.month)

    dat.loc[:, 'ModifiedDate'] = dat.EditDate.map(lambda x: x.date())
    dat.ModifiedDate = pd.to_datetime(dat.ModifiedDate)

    today = [dt.today().date() for i in xrange(0, len(dat))]


    SDR = ['Christina Garza', 'Sean Arnold', 'Toby Shean', 'Steffan Davies', 'David', 'Mark Favelson',
           'David Iparraguire', 'David Iparraguirre', "Daniel O'Brien", 'Phil Arbetier', 'Phil', 'Tugce Akkalay',
           "Dan O'Brien", 'Dan Obrien', 'Phil Arbeiter', 'Doug Molumby', 'Tony Fitzgerald']

    dat.loc[dat.OldValue.isin(SDR), 'OldValue'] = np.nan
    dat.loc[dat.NewValue.isin(SDR), 'OldValue'] = np.nan

    dat.OldValue.fillna('Created', inplace=True)
    dat.NewValue.fillna('Created', inplace=True)

    dat.loc[dat.OldValue == 'Qualification', 'OldValue'] = 'Qualified'
    dat.loc[dat.NewValue == 'Qualification', 'NewValue'] = 'Qualified'

    dat.loc[dat.OldValue == 'Prospecting', 'OldValue'] = 'Prospect'
    dat.loc[dat.NewValue == 'Prospecting', 'NewValue'] = 'Prospect'
    # add created vs stage_changed
    event_created = ['Created by lead convert', 'Created.', 'opportunityCreatedFromLead']
    dat.loc[:, 'Event'] = ['stage_change' for i in dat.FieldEvent]
    dat.loc[dat.FieldEvent.isin(event_created), 'Event'] = 'opp_created'

    dat = dat[['OpportunityID', 'Event', 'FieldEvent', 'OldValue', 'NewValue', 'StageName',
               'CreatedDate', 'ModifiedDate', 'EditYear', 'EditMonth', 'Name', 'StartDate', 'EndDate']]

    dat['OldNewStage'] = zip(dat.OldValue, dat.NewValue)

    return dat


def get_rate(dat, stage, end_date_created, end_date_converted, denom_nom=True, return_ids=True):
    # function to get conversion rate within a timeframe
    # type: (DataFrame, str, datetime, datetime, Boolean) -> float
    '''

    :param return_ids: Boolean, defaults to True, if want to return set of ids for nominator & denominator
    :param dat: dataframe with data for which to calculate conversion rate.
                Need OpportunityID, ModifiedDate, OldValue, NewValue
    :param stage: str, stage for which to get conversion rate
    :param end_date_created: datetime, last date for an opp to be created, endDate in denominator
    :param end_date_converted: datetime, last date for an opp to get converted, endDate in numerator
    :param denom_nom: Boolean, defaults to True, if want to return numerator & denominator w. conversion rate
    :return: if denom_nom: [numerator, denominator, conversion rate]. Otherwise: conversion rate (float)
    '''
    conv_dat = dat.copy()
    try:
        conv_dat.ModifiedDate = pd.to_datetime(conv_dat.ModifiedDate)
    except:
        print "Type error"

    start_date = conv_dat.StartDate.drop_duplicates().tolist().pop()
    # end_date_converted is for numerator - to count of opportunities that were modified and converted in 12 months
    # end_date_created is for denominator - to count of opportunities that were modified in 9 months
    end_date_nom = end_date_converted
    end_date_denom = end_date_created
    try:
        end_date_nom = end_date_nom.date()
        end_date_denom = end_date_denom.date()
    except:
        pass

    # opportunities that were already in that stage at that time
    stage_old = conv_dat[
        (conv_dat.ModifiedDate >= start_date) & (conv_dat.ModifiedDate <= end_date_denom) & (
            conv_dat.OldValue == stage)]
    stage_old = stage_old[~stage_old.OpportunityID.duplicated()]

    # opportunities that got to the stage during that time
    stage_new = conv_dat[
        (conv_dat.ModifiedDate >= start_date) & (conv_dat.ModifiedDate <= end_date_denom) & (
            conv_dat.NewValue == stage)]
    stage_new = stage_new[~stage_new.OpportunityID.duplicated()]

    # DENOMINATOR: take set of all opportunities that were already in that stage or moved into that stage
    denominator = len(set(stage_old.OpportunityID).union(set(stage_new.OpportunityID)))
    denom_ids = set(stage_old.OpportunityID).union(set(stage_new.OpportunityID))

    # NOMINATOR: opportunities that are considered in that time period that moved forward to one of the other stages
    # these are given month lag time
    # if stage = PO in Progress, only 1 stage to look for in stage_dict
    if stage == stages[-2]:
        nom_data = conv_dat[(conv_dat.ModifiedDate >= start_date) & (conv_dat.ModifiedDate <= end_date_nom)
                            & (conv_dat.OpportunityID.isin(denom_ids)) & (conv_dat.OldNewStage == stage_dict[stage])]
    else:
        nom_data = conv_dat[(conv_dat.ModifiedDate >= start_date) & (conv_dat.ModifiedDate <= end_date_nom)
                            & (conv_dat.OpportunityID.isin(denom_ids)) & (conv_dat.OldNewStage.isin(stage_dict[stage]))]
    # remove duplicates
    nom_data = nom_data[~nom_data.OpportunityID.duplicated()]

    nominator = nom_data.OldNewStage.value_counts().sum()
    nom_ids = set(nom_data.OpportunityID)

    # conversion rate
    try:
        conv_rate = np.float(nominator) / denominator

        if denom_nom:
            if return_ids:
                return [conv_rate, nominator, denominator, nom_ids, denom_ids]
            else:
                return [conv_rate, nominator, denominator]
        else:
            return conv_rate



    except ZeroDivisionError:

        print "nominator:", nominator
        print "denominator", denominator
        conv_rate = 0.0
        if denom_nom:
            return [conv_rate, nominator, denominator]
        else:
            return conv_rate




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
# dictionary to keep track of stage movements
to_stages_dict = {
    'Qualified': 'Qualified to Buying Process Id.',
    'Buying Process id.': 'Buying Process Id. to Short List',
    'Short List': 'Short List to Chosen Vendor',
    'Chosen Vendor': 'Chosen Vendor to Negotiation/Review',
    'Negotiation/Review': 'Negotiation/Review to PO In Progress',
    'PO In Progress': 'PO In Progress to Closed Won'}



# login to server to query SalesForce backup database using pyodbc (mssql)
server = "10.0.123.212"
user = "dbamp"
password = "ua4yCAnolhxV"
# login to server to update datamart using MYSQL
admin_user = "salesadmin"
admin_pass = "salesadmin"
mysql_connect = MySQLdb.connect(server, admin_user, admin_pass, 'salesdatamart')

cursor_2 = mysql_connect.cursor()

# Populate conversion rates for each week
t = dt.today()
t = t.date()
t_dates = []

# add in report date when run
report_date = dt.today()
report_date = report_date.date()

for i in xrange(0, 1):
    t_date = t - relativedelta(weeks=i)
    t_dates.append(t_date)

for i in t_dates:
    q = get_query(i)
    conn = pyodbc.connect(driver ="{SQL Server}", server = "localhost", database = "salesforce backups", uid="dbamp", pwd ="ua4yCAnolhxV", trusted_connection = "yes")
    # put results in a DataFrame
    dat = pd.read_sql_query(q, conn)
    dat.columns = ['OpportunityID', 'StartDate', 'EndDate', 'EditDate', 'CreatedDate', 'LastModifiedDate', 'FieldEvent', 'OldValue', 'NewValue', 'OppName', 'StageName', 'Name']
    temp_dat = clean_dat(dat=dat)
    # DataFrame for conversion rates
    temp_rates = pd.DataFrame(index=stages[1:-1], columns=['FromDate', 'ToDate', 'ReportDate','FromStage', 'ToStage','ConversionRate',
                                                           'Numerator', 'Denominator'])

    # give opp 9 months to be modified
    t_created = i - relativedelta(months=3)  # end date for opp to be in time pd)
    t_start = i - relativedelta(months=12)  # T-12 (start date for opp to be in time pd)

    for j in stages[1:-1]:
        res = get_rate(dat=temp_dat, stage=j, end_date_converted=i, end_date_created=t_created, denom_nom=True,
                       return_ids=False)
        temp_rates.ix[j]['FromDate'] = t_start
        temp_rates.ix[j]['ToDate'] = i
        temp_rates.ix[j]['ReportDate'] = report_date
        temp_rates.ix[j]['FromStage'] = j
        temp_rates.ix[j]['ToStage'] = to_stages_dict[j]
        temp_rates.ix[j]['ConversionRate', 'Numerator', 'Denominator'] = res


    temp_rates = temp_rates.reset_index().drop('index', axis=1)
    print "\n ***Conversion End Date:", i, ",Conversion Start Date:", t_start, ",Created End Date:", t_created, "\n"
    print "\n temp_rates: \n", temp_rates

    # Insert into ConversionRates table:
    temp_rates.to_sql(name = 'ConversionRates', con = mysql_connect,  flavor = 'mysql',
                      if_exists = 'append', index=False)
    print "***** SUCCESSFULLY ADDED ROWS TO TABLE *****"
