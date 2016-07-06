from load import *

data = pd.read_csv("~/Documents/dev/conv_rates/data/input_data/report1467744167696.csv",  index_col=False, encoding = 'utf-8')
dat = clean_dat(data)
df = dat.copy()
df = df[df.FieldEvent != 'SDR Created By']
df.OldValue = df.OldValue.map(lambda x: str(x))
df.NewValue = df.NewValue.map(lambda x: str(x))
df.OldNewStage = zip(df.OldValue, df.NewValue)

# add quarter info
quarter_dict = {1:2, 2:3, 3:4, 4:1}
df['quarter'] = df.ModifiedDate.dt.quarter
df['quarter'] = df.quarter.map(lambda x: quarter_dict[x])


# % of deferred throughout quarters
df.set_index('EditDate', inplace=True)
df['yr_qt'] = df.index.to_period('Q-SEP')
df['yr_2'] = df.index.to_period('A-SEP')
df.OldNewStage.isin()


old ='Qualified'
new = 'Buying Process id.'
lost.extend([zip((stages[2], stages[1]))])
# deferred = df[(df.OldValue.isin(stages)) & (df.NewValue.isin(lost)) | (df.OldNewStage == tuple((new, old)))]
deferred = df[(df.NewValue.isin(lost)) | (df.OldNewStage == tuple((new, old)))]
deferred.yr_2 = deferred.yr_2.map(lambda x: str(x))
yrs = ['2014','2015','2016']
deferred = deferred[deferred.yr_2.isin(yrs)]
deferred.groupby(['yr_2','OldNewStage'])['days_in_stage'].agg(['count','sum'])  #.apply(lambda x: 100*x/float(x.sum()))
deferred_noms = deferred.groupby(['yr_qt','OldNewStage'])['days_in_stage'].agg(['count','sum'])  #.apply(lambda x: 100*x/float(x.sum()))

df_2 = df.copy()
df_2.yr_2 = df_2.yr_2.map(lambda x: str(x))
df_2 = df_2[df_2.yr_2.isin(yrs)]

df_2.groupby(['yr_qt','OldValue']).count().to_csv('~/Documents/dev/conv_rates/data/output/totals_2014_2016_v2.csv')