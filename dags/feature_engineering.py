
def generate_holiday_df(session, start_date, end_date):
    from snowflake.snowpark import functions as F 
    import pandas as pd
    from pandas.tseries.holiday import USFederalHolidayCalendar

    cal = USFederalHolidayCalendar()
    holiday_list = pd.DataFrame(cal.holidays(start=start_date, end=end_date), columns=['DATE'], dtype=str)
    holiday_df = session.create_dataframe(holiday_list) \
                        .toDF(holiday_list.columns[0]) \
                        .with_column("HOLIDAY", F.lit(1))
    return holiday_df

def generate_precip_df(session, start_date, end_date):
    from snowflake.snowpark import functions as F 
    import pandas as pd

    tempdf = pd.read_csv('./include/weather.csv')
    tempdf['DATE']=pd.to_datetime(tempdf['dt_iso'].str.replace(' UTC', ''), 
                                 format='%Y-%m-%d %H:%M:%S %z', 
                                 utc=True).dt.tz_convert('America/New_York').dt.date
    tempdf = tempdf.groupby('DATE', as_index=False).agg(PRECIP=('rain_1h', 'mean')).round()

    precip_df = session.create_dataframe(tempdf).filter((F.col('DATE') >= start_date) &
                                                        (F.col('DATE') <= end_date))\
                       .fillna({'PRECIP':0})
    return precip_df

def generate_features(session, input_df, holiday_table_name, precip_table_name):
    import snowflake.snowpark as snp
    from snowflake.snowpark import functions as F 
    
    start_date, end_date = input_df.select(F.min('STARTTIME'), F.max('STARTTIME')).collect()[0][0:2]

    holiday_df = session.table(holiday_table_name)
    try: 
        _ = holiday_df.columns()
    except:
        holiday_df = generate_holiday_df(session=session,
                                         start_date=start_date,
                                         end_date=end_date)
        
    precip_df = session.table(precip_table_name)
    try: 
        _ = precip_df.columns()
    except:
        precip_df = generate_precip_df(session=session,
                                       start_date=start_date,
                                       end_date=end_date)

    feature_df = input_df.select(F.to_date(F.col('STARTTIME')).alias('DATE'),
                                 F.col('START_STATION_ID').alias('STATION_ID'))\
                         .replace({'NULL': None}, subset=['STATION_ID'])\
                         .groupBy(F.col('STATION_ID'), F.col('DATE'))\
                         .count()

    #Impute missing values for lag columns using mean of the previous period.
    mean_1 = round(feature_df.sort('DATE').limit(1).select(F.mean('COUNT')).collect()[0][0])
    mean_7 = round(feature_df.sort('DATE').limit(7).select(F.mean('COUNT')).collect()[0][0])
    mean_365 = round(feature_df.sort('DATE').limit(365).select(F.mean('COUNT')).collect()[0][0])

    date_win = snp.Window.orderBy('DATE')

    feature_df = feature_df.with_column('LAG_1', F.lag('COUNT', offset=1, default_value=mean_1) \
                                         .over(date_win)) \
                           .with_column('LAG_7', F.lag('COUNT', offset=7, default_value=mean_7) \
                                         .over(date_win)) \
                           .with_column('LAG_365', F.lag('COUNT', offset=365, default_value=mean_365) \
                                         .over(date_win)) \
                           .join(holiday_df, 'DATE', join_type='left').na.fill({'HOLIDAY':0}) \
                           .join(precip_df, 'DATE', 'inner') \
                          .na.drop() 

    return feature_df
