from load import *


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
    # dat['from_date'] = pd.to_datetime(today)
    # dat['startDate'] = dat.startDate.map(lambda x: x.date())

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


def get_ClosedWon_rate(dat, stage, end_date_created, end_date_converted, denom_nom=True):
    # function to get conversion rate within a timeframe
    # type: (DataFrame, str, datetime, datetime, Boolean) -> float
    '''

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

    # NOMINATOR: opportunities that are considered in that time period that moved forward Closed Won (last stage combination)
    # these are given month lag time

    if stage == stages[-2]:
        nom_data = conv_dat[(conv_dat.ModifiedDate >= start_date) & (conv_dat.ModifiedDate <= end_date_nom)
                            & (conv_dat.OpportunityID.isin(denom_ids)) & (conv_dat.OldNewStage == stage_dict[stage])]
    else:
        nom_data = conv_dat[(conv_dat.ModifiedDate >= start_date) & (conv_dat.ModifiedDate <= end_date_nom)
                            & (conv_dat.OpportunityID.isin(denom_ids)) & (
                                conv_dat.OldNewStage == stage_dict[stage][-1])]

    # remove duplicates
    nom_data = nom_data[~nom_data.OpportunityID.duplicated()]

    nominator = nom_data.OldNewStage.value_counts().sum()
    nom_ids = nom_data.OpportunityID.drop_duplicates()

    # conversion rate
    try:
        conv_rate = np.float(nominator) / denominator
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


def add_stages(dat):
    '''
    Input: dataframe to add missing stages
    Output: dtaframe with missing stages introduced and ones that skip removed
    Usage: Funtion to add in missing stages:
            For each group (opportunityid):
                Loop through each stage and see if it's in stages_missing (that we care about)
                If so: add stages missed (these are based on opp_stages where opp_stages in logical sequence process should follow)
                If add_stages is not empty, add those stages to temporary dataframe and append that to data
    '''
    opp_grouped = dat.groupby('OpportunityID', as_index=False)
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

        if len(add_stages) > 0:
            # fill in temporary dataframe with known information
            temp_g = pd.DataFrame(columns=dat.columns)
            temp_g['OldNewStage'] = add_stages
            temp_g['OpportunityID'] = g
            temp_g['Event'] = 'skipped'
            temp_g['FieldEvent'] = 'stage_skipped'
            temp_g['OldValue'] = temp_g.OldNewStage.map(lambda x: x[0]).values
            temp_g['NewValue'] = temp_g.OldNewStage.map(lambda x: x[1]).values
            temp_g.loc[:, ['AccountCountry']] = val.loc[:, ['AccountCountry']].drop_duplicates().values
            # temp_g.loc[:,'Probability'] = temp_g.NewValue.map(lambda x: opp_dict[x]).values
            temp_g.loc[:, 'StageLength'] = pd.to_timedelta(timedelta(days=0))
            temp_g.loc[:, 'days_in_stage'] = temp_g.StageLength.map(lambda x: x.days)
            # add missing stages to data
            dat = dat.append(temp_g, ignore_index=True)
            # remove stages that are in missing stages (those that skip around) since added in between stages
            # dat.drop(dat.index[idx_drop], inplace = True)
            # dat.index = [i for i in xrange(0, len(dat))]
    return dat
