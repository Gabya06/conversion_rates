from functions import *
from load import *

def usage():
	print """
	if need to clean file: python load.py [input_file] > [output_file]
	then: python conv_rates.py [output_file month_start month_end] 
	ex: python conv_rates.py [output_file 4 9]
	"""
if __name__ == '__main__':
	if len(sys.argv) < 4:
		usage()
		sys.exit(2)
	try:
		file_name = sys.argv[1] # clean data file
		start_month = int(sys.argv[2]) # conv start month
		end_month = int(sys.argv[3]) # conv end month
		if len(sys.argv) == 5:
			stage = sys.argv[4]
		else:
			stage = stages[2]
		if len(sys.argv) == 6:
			ret_denom_nom = sys.argv[5]
		else:
			ret_denom_nom = True


	except IOError:
		sys.stderr.write("Error: cant read input file %s.\n" % arg)
		sys.exit(1)

	start_date = make_time(start_month)
	end_date = make_time(end_month)

	df = pd.read_csv(file_name, index_col = False, encoding = 'utf-8')
	df.OldValue = df.OldValue.map(lambda x: str(x))
	df.NewValue = df.NewValue.map(lambda x: str(x))
	df.OldNewStage = zip(df.OldValue, df.NewValue)

	args = {'start_date': start_date, 'end_date' : end_date, 'stage': stage, 'denom_nom' : ret_denom_nom}

	print "*" * 20 
	print "Calculating Conversion rates for ", start_date , "through ", end_date, "and stage", stage
	print "*" * 20 
	# rate = get_rate(df, ** args)
	# print rate
	
	conv_df = get_conv_table(dat = df, **args)

	print "\nConversion Rates Results:\n", 
	print "\t",conv_df

# months_2 = dt.today() - timedelta(days = 60)
# months_2 = months_2.date()

# months_4 = dt.today() - timedelta(days = 124)
# months_4 = months_4.date()

# months_6 = dt.today() - timedelta(days = 120)
# months_6 = months_6.date()

# months_9 = dt.today() - timedelta(days = 274)
# months_9 = months_9.date()

# months_18 = dt.today() - timedelta(days = 540)
# months_18 = months_18.date()

# months_12 = dt.today() - timedelta(days = 360)
# months_12 = months_12.date()


# print months_4
# print months_9

# '''
# CLEAN DATA
# '''
# df = pd.read_csv("~/Documents/dev/conv_rates/input_data/data_clean_07012016.csv",  index_col=False, encoding = 'utf-8')
# df = df[df.FieldEvent != 'SDR Created By']
# df.OldValue = df.OldValue.map(lambda x: str(x))
# df.NewValue = df.NewValue.map(lambda x: str(x))
# df.OldNewStage = zip(df.OldValue, df.NewValue)


# '''
# Get Conversion rates for a few time periods
# '''
# # conversion rates table: 10/2015 - 03/2016
# args = {'start_date': months_9, 'end_date' : months_4, 'stage': stages[2], 'denom_nom' : True }


# print get_rate(dat = df, **args)

# conv_recent = get_conv_table(dat = df, **args)

# print "\nConversion\n", 
# print "\t",conv_recent


#conv_recent = pd.DataFrame(index = stages[1:-1], columns = ['conv_rate','nominator','denominator', 'start_date', 'end_date'])


# for e, j in enumerate(stages[1:-1]):
# 	rate_args['stage'] = j
# 	conv_rate, nom, denom = get_rate(dat = dat_recent, **rate_args)
# 	conv_recent.ix[e, 'conv_rate'] = conv_rate
# 	conv_recent.ix[e, 'nominator'] = nom
# 	conv_recent.ix[e, 'denominator'] = denom
# 	conv_recent.ix[e, 'start_date'] = rate_args['start_date']
# 	conv_recent.ix[e, 'end_date'] = rate_args['end_date']



# # conversion rates table: 07/2015 - 1/2016
# conv_12months = pd.DataFrame(index = stages[1:-1], columns = ['conv_rate','nominator','denominator', 'start_date', 'end_date'])
# rate_args = {'start_date': months_12, 'end_date' : months_6, 'stage': stage, 'denom_nom' : True }
# for e, j in enumerate(stages[1:-1]):
# 	rate_args['stage'] = j
# 	conv_rate, nom, denom = get_rate(dat = dat_12_months, **rate_args)
# 	conv_12months.ix[e, 'conv_rate'] = conv_rate
# 	conv_12months.ix[e, 'nominator'] = nom
# 	conv_12months.ix[e, 'denominator'] = denom
# 	conv_12months.ix[e, 'start_date'] = rate_args['start_date']
# 	conv_12months.ix[e, 'end_date'] = rate_args['end_date']

