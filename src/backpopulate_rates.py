import MySQLdb
import pymssql
from functions import *

'''
script to back-populate conversion rates (53 weeks back)
works on mac not on server
'''
# set directory & read data
os.chdir("/Users/Gabi/Documents/dev/conv_rates/")
data_path = os.getcwd() + '/data/input_data/'


# nom id, denom ids for every stage - for last week

# login to mssql for salesforce backup data query
server = "10.0.123.212"
user = "dbamp"
password = "ua4yCAnolhxV"

# login to mysqldb for datamart upload
admin_user = "salesadmin"
admin_pass = "salesadmin"
mysql_connect = MySQLdb.connect(server, admin_user, admin_pass, 'salesdatamart')

cursor_2 = mysql_connect.cursor()

# Populate conversion rates for each week for 50 weeks going back from 9/1/2016
# t = dt(2016, 9,1)
t = dt.today()
t = t.date()
t_dates = []

# add in report date when run
report_date = dt.today()
report_date = report_date.date()

for i in xrange(0, 53):
    t_date = t - relativedelta(weeks=i)
    t_dates.append(t_date)

for i in t_dates:
    q = get_query(i)

    conn = pymssql.connect(server, user, password, "salesforce backups")
    cursor = conn.cursor()
    cursor.execute(q)
    # put results in a DataFrame
    qr_res = cursor.fetchall()
    dat = pd.DataFrame(qr_res,
                       columns=['OpportunityID', 'StartDate', 'EndDate', 'EditDate', 'CreatedDate', 'LastModifiedDate',
                                'FieldEvent', 'OldValue', 'NewValue', 'OppName', 'StageName', 'Name'])
    temp_dat = clean_dat(dat=dat)
    # DataFrame for conv rates
    temp_rates = pd.DataFrame(index=stages[1:-1], columns=['FromDate', 'ToDate', 'ReportDate','FromStage', 'ToStage','ConversionRate',
                                                           'Numerator', 'Denominator']) #, 'Nom_ids','Denom_ids'])

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
        # temp_rates.ix[j]['ConversionRate', 'Numerator', 'Denominator', 'Nom_ids', 'Denom_ids'] = res

    temp_rates = temp_rates.reset_index().drop('index', axis=1)

    print "\n ***Conversion End Date:", i, ",Conversion Start Date:", t_start, ",Created End Date:", t_created, "\n"
    print "\n temp_rates: \n", temp_rates

    # TODO insert into ConversionRates table:
    # temp_rates.to_sql(name = 'ConversionRates', con = mysql_connect,  flavor = 'mysql',
    #                   if_exists = 'append', index=False)
    print "***** SUCCESSFULLY ADDED ROWS TO TABLE *****"
