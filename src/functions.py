from load import *



'''
Input: number of months wish to go back
Output: today - number of months

Usage: Function to create date based on how many months we want to go back in time
'''
def make_time(n_months):
	n_days = n_months * 30
	out_time = dt.today() - timedelta(days = n_days)
	out_time = out_time.date()
	return out_time

'''
Input: column name (string)
Output: cleaned string 

Function to clean column names
'''
def clean_column(column_name):
	col_name = column_name.replace(' ','').replace('[^\w\s]','').replace('\\','').replace('/','')
	return col_name


'''
Inputs: 
	1) Dataframe with columns: 
		EditDate, OldValue, NewValue, FieldEvent, Stage, Probability
		CreatedDate, StageChangeDate, AccountCountry, Age
	2) partner ids to exclude

Output: Dataframe with from_date, stagelength and oldnewstage

Usage: Function to:
	- Clean column names, dates and opp values, Remove partner ids & rows w 'SDR created by'
	- Add "from_date" to each group of OpportunityID's:
		- get modified_dates & loop through old values (from stage)
		- if stage == 'created' just add created date
		- else pop last item in modified_dates & assign it to that row
	- Add 'StageLength' & 'OldNewStage'
'''
def clean_dat(dat, exclude):
	id_exclude = exclude
	cols = dat.columns
	cols = [clean_column(c) for c in cols]
	dat.columns = cols
	# remove partners
	dat = dat[~dat.OpportunityID.isin(id_exclude)]
	dat.EditDate = pd.to_datetime(dat.EditDate)
	dat.loc[:,'EditYear'] = dat.EditDate.map(lambda x: x.year)
	dat.loc[:,'EditMonth'] = dat.EditDate.map(lambda x: x.month)	
	
	dat.loc[:,'ModifiedDate'] = dat.EditDate.map(lambda x: x.date())
	dat.ModifiedDate = pd.to_datetime(dat.ModifiedDate)

	today = [dt.today().date() for i in xrange(0, len(dat))]
	dat['from_date'] = pd.to_datetime(today)

	SDR = ['Christina Garza','Sean Arnold','Toby Shean','Steffan Davies','David','Mark Favelson','David Iparraguire','David Iparraguirre',"Daniel O'Brien",'Phil Arbetier','Phil','Tugce Akkalay',"Dan O'Brien",'Dan Obrien','Phil Arbeiter','Doug Molumby','Tony Fitzgerald']

	dat.loc[dat.OldValue.isin(SDR),'OldValue']= np.nan
	dat.loc[dat.NewValue.isin(SDR),'OldValue']= np.nan

	dat.OldValue.fillna('Created', inplace = True)
	dat.NewValue.fillna('Created', inplace = True)

	dat.loc[dat.OldValue == 'Qualification','OldValue'] = 'Qualified'
	dat.loc[dat.NewValue == 'Qualification','NewValue'] = 'Qualified'

	dat.loc[dat.OldValue == 'Prospecting','OldValue'] = 'Prospect'
	dat.loc[dat.NewValue == 'Prospecting','NewValue'] = 'Prospect'
	# add created vs stage_changed
	event_created = ['Created by lead convert', 'Created.']
	dat.loc[:,'Event'] = ['stage_change' for i in dat.FieldEvent]
	dat.loc[dat.FieldEvent.isin(event_created),'Event'] = 'opp_created'

	# group by opportunity & add from_date
	opp_grouped = dat.groupby('OpportunityID', as_index = False)
	for g, val in opp_grouped:

		modified_dates = val.ModifiedDate.tolist()
		modified_dates = [x.date() for x in modified_dates]
		modified_dates.sort(reverse = True)

		g_index = val.index.values
		for i, item in enumerate(val.OldValue):
			if item == 'Created':
				created = val[val.Event == 'opp_created']['CreatedDate'].values
				val.loc[val.OldValue == 'Created', 'from_date'] = created
			else:
				last_date = modified_dates.pop() 
				# print "last_date", last_date
				val.loc[g_index[i], 'from_date'] = last_date
				# print 'check', val.loc[g_index[i], 'from_date']
		dat.loc[val.index,'from_date'] = val.from_date
	# add in stage length & days in each stage
	dat['StageLength'] = dat.EditDate - dat.from_date
	dat['days_in_stage'] = dat.StageLength.map(lambda x: x.days)
	# re-order columns
	dat = dat[['OpportunityID', 'Event', 'FieldEvent','OldValue','NewValue','Stage','Probability','StageLength','days_in_stage','CreatedDate','ModifiedDate','from_date','EditDate','EditYear','EditMonth','StageChangeDate','AccountCountry','StageDuration','Age']]
	dat['OldNewStage'] = zip(dat.OldValue, dat.NewValue)

	return dat



'''
Input:
- Dataframe with columns: OpportunityID, ModifiedDate, OldValue, NewValue
- rate_args dict: {start_date, end_date, stage, denom_nom}
	- stage: stage want to get conversion rate for (Qualified, Buying Process id,...)
	- denom_nom: Boolean (True if want to return these numbers along w conv rate)
Output: rate for given satge

Usage: Function to get conversion rate within a timeframe:
'''
def get_rate(dat, **rate_args):	
	start_date = rate_args['start_date']
	end_date = rate_args['end_date']
	stage = rate_args['stage']
	denom_nom = rate_args['denom_nom']

	conv_dat = dat.copy()
	try:
		conv_dat.ModifiedDate = pd.to_datetime(conv_dat.ModifiedDate)
	except:
		print "Type error"

	# opportunities that were already in that stage at that time
	stage_old = conv_dat[(conv_dat.ModifiedDate >= start_date) & (conv_dat.ModifiedDate <= end_date) & (conv_dat.OldValue == stage)]
	stage_old = stage_old[~stage_old.OpportunityID.duplicated()]


	# opportunities that got to the stage during that time
	stage_new = conv_dat[(conv_dat.ModifiedDate >= start_date) & (conv_dat.ModifiedDate <= end_date) & (conv_dat.NewValue == stage)]
	stage_new = stage_new[~stage_new.OpportunityID.duplicated()]

	# DENOMINATOR: take set of all opportunities that were already in that stage or moved into that stage
	denominator = len(set(stage_old.OpportunityID).union(set(stage_new.OpportunityID)))
	denom_ids = set(stage_old.OpportunityID).union(set(stage_new.OpportunityID))
	
	# NOMINATOR: opportunities that are considered in that time period that moved forward to one of the other stages
	if stage == stages[-2]:
		nom_data = conv_dat[(conv_dat.ModifiedDate >= start_date) &  (conv_dat.OpportunityID.isin(denom_ids)) & (conv_dat.OldNewStage == stage_dict[stage])]
	else:
		# nom_data = conv_dat[(conv_dat.ModifiedDate >= start_date)]
		nom_data = conv_dat[(conv_dat.ModifiedDate >= start_date) &  (conv_dat.OpportunityID.isin(denom_ids)) & (conv_dat.OldNewStage.isin(stage_dict[stage]))]	
	# remove duplicates
	nom_data = nom_data[~nom_data.OpportunityID.duplicated()]
	
	nominator = nom_data.OldNewStage.value_counts().sum()
	nom_ids = nom_data.OpportunityID.drop_duplicates()
	
	# conversion rate
	try:
		conv_rate = np.float(nominator)/denominator

		if denom_nom:
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
Input: data and rate_dict: {start_date, end_date, stage, denom_nom}
Output: conversion table for all stages

Usage: Function to return conversion rates for all stages
'''
def get_conv_table(dat, **rate_args):
	conv_table = pd.DataFrame(index = stages[1:-1], columns = ['conv_rate','nominator','denominator', 'start_date', 'end_date'])
	for e, j in enumerate(stages[1:-1]):
		rate_args['stage'] = j
		conv_rate, nom, denom = get_rate(dat, **rate_args)
		conv_table.ix[e, 'conv_rate'] = conv_rate
		conv_table.ix[e, 'nominator'] = nom
		conv_table.ix[e, 'denominator'] = denom
		conv_table.ix[e, 'start_date'] = rate_args['start_date']
		conv_table.ix[e, 'end_date'] = rate_args['end_date']
	return conv_table




'''
Input: dataframe to add missing stages
Output: dtaframe with missing stages introduced and ones that skip removed

Usage: Funtion to add in missing stages:
		For each group (opportunityid):
			Loop through each stage and see if it's in stages_missing (that we care about)
			If so: add stages missed (these are based on opp_stages where opp_stages in logical sequence process should follow)
			If add_stages is not empty, add those stages to temporary dataframe and append that to data
'''
def add_stages(dat):
	opp_grouped = dat.groupby('OpportunityID', as_index = False)
	idx_drop = []
	for g, val in opp_grouped:
		# create new list of stages to add for each group
		add_stages = []
		# check if each stage w.i each group is in_missing
		for stage in val.OldNewStage:
			if stage in stages_missing:
				# get index
				stage_index = val[val.OldNewStage == stage].index.tolist()
				# if stage is one of stages that has skipped, add index to list
				idx_drop.extend(stage_index)
				# 20% stages missing (Qualified)
				if stage == stages_missing[0]:
					add_stages.extend(opp_stages[1:3])
				elif stage == stages_missing[1]:
					add_stages.extend(opp_stages[1:4])
				elif stage == stages_missing[2]:
					add_stages.extend(opp_stages[1:5])
				elif stage == stages_missing[3]:
					add_stages.extend(opp_stages[1:6])
				elif stage == stages_missing[4]:
					add_stages.extend(opp_stages[1:7])
				# 40% stages missing (Buying Process id.)
				elif stage == stages_missing[5]:
					add_stages.extend(opp_stages[2:4])
				elif stage == stages_missing[6]:
					add_stages.extend(opp_stages[2:5])
				elif stage == stages_missing[7]:
					add_stages.extend(opp_stages[2:6])
				elif stage == stages_missing[8]:
					add_stages.extend(opp_stages[2:7])
				# 50% stages missing (short list)
				elif stage == stages_missing[9]:
					add_stages.extend(opp_stages[3:5])
				elif stage == stages_missing[10]:
					add_stages.extend(opp_stages[3:6])
				elif stage == stages_missing[11]:
					add_stages.extend(opp_stages[3:7])
				# 70% stages missing (Chosen vendor)
				elif stage == stages_missing[12]:
					add_stages.extend(opp_stages[4:6])
				elif stage == stages_missing[13]:
					add_stages.extend(opp_stages[4:7])
				# 80% stages missing (negotations/review)
				elif stage == stages_missing[14]:
					add_stages.extend(opp_stages[5:7])
			# if stage is not in missing, pass
			else:
				pass		

		if len(add_stages) >0:
		# fill in temporary dataframe with known information
			temp_g = pd.DataFrame(columns = dat.columns)
			temp_g['OldNewStage'] = add_stages
			temp_g['OpportunityID'] = g
			temp_g['Event'] = 'skipped'
			temp_g['FieldEvent'] = 'stage_skipped'
			temp_g['OldValue'] = temp_g.OldNewStage.map(lambda x: x[0]).values
			temp_g['NewValue'] = temp_g.OldNewStage.map(lambda x: x[1]).values
			temp_g.loc[:,['AccountCountry']] = val.loc[:,['AccountCountry']].drop_duplicates().values
			#temp_g.loc[:,'Probability'] = temp_g.NewValue.map(lambda x: opp_dict[x]).values
			temp_g.loc[:,'StageLength'] = pd.to_timedelta(timedelta(days = 0))
			temp_g.loc[:,'days_in_stage'] = temp_g.StageLength.map(lambda x: x.days)
			# add missing stages to data
			dat = dat.append(temp_g, ignore_index = True)
	# remove stages that are in missing stages (those that skip around) since added in between stages
	# dat.drop(dat.index[idx_drop], inplace = True)
			# dat.index = [i for i in xrange(0, len(dat))]
	return dat

