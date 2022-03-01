
def generate_feature_table(session, 
                           clone_table_name, 
                           feature_table_name, 
                           holiday_table_name, 
                           precip_table_name) -> list:
    
    from snowflake.snowpark import functions as F
    import snowflake.snowpark as snp
    
    clone_df = session.table(clone_table_name)
    holiday_df = session.table(holiday_table_name)
    precip_df = session.table(precip_table_name)

    window = snp.Window.partitionBy(F.col('STATION_ID')).orderBy(F.col('DATE').asc())
    sid_window = snp.Window.partitionBy(F.col('STATION_ID'))


    feature_df = clone_df.select(F.to_date(F.col('STARTTIME')).alias('DATE'),
                                 F.col('START_STATION_ID').alias('STATION_ID'))\
                         .groupBy(F.col('STATION_ID'), F.col('DATE'))\
                            .count()\
                         .withColumn('DAY_COUNT', F.count(F.col('DATE')).over(sid_window))\
                            .filter(F.col('DAY_COUNT') >= 365*2)\
                         .withColumn('LAG_1', F.lag(F.col('COUNT'), offset=1, default_value=None).over(window))\
                         .withColumn('LAG_7', F.lag(F.col('COUNT'), offset=7, default_value=None).over(window))\
                            .na.drop()\
                         .join(holiday_df, 'DATE', join_type='left').na.fill({'HOLIDAY':0})\
                         .join(precip_df, 'DATE', 'inner')\
                         .withColumn('DAY_COUNT', F.count(F.col('DATE')).over(sid_window))\
                            .filter(F.col('DAY_COUNT') >= 365*2)\
                         .drop('DAY_COUNT')
    
    feature_column_list = feature_df.columns
    feature_column_list.remove('\"STATION_ID\"')
    feature_column_list = [f.replace('\"', "") for f in feature_column_list]
    feature_column_array = F.array_construct(*[F.lit(x) for x in feature_column_list])

    feature_df_stuffed = feature_df.groupBy(F.col('STATION_ID'))\
                                   .agg(F.array_agg(F.array_construct(*feature_column_list)).alias('INPUT_DATA'))\
                                   .withColumn('INPUT_COLUMN_LIST', feature_column_array)\
                                   .withColumn('TARGET_COLUMN', F.lit('COUNT'))
    
    feature_df_stuffed.limit(50).write.mode('overwrite').saveAsTable(feature_table_name)        

    return feature_table_name

def train_predict_feature_table(session, station_train_pred_udf_name, feature_table_name, pred_table_name) -> str:
    from snowflake.snowpark import functions as F
    import pandas as pd
    import ast
    
    max_epochs=10

    output_list = session.table(feature_table_name)\
                         .select('STATION_ID', F.call_udf(station_train_pred_udf_name, 
                                                          'INPUT_DATA', 
                                                          'INPUT_COLUMN_LIST', 
                                                          'TARGET_COLUMN', 
                                                          F.lit(max_epochs)).alias('OUTPUT_DATA')).collect()
    df = pd.DataFrame()
    for row in range(len(output_list)):
        tempdf = pd.DataFrame(data = ast.literal_eval(output_list[row]['OUTPUT_DATA'])[0], 
                                    columns=ast.literal_eval(output_list[row]['OUTPUT_DATA'])[1]
                                    )
        tempdf['STATION_ID'] = str(output_list[row]['STATION_ID'])
        df = pd.concat([df, tempdf], axis=0)
        
    session.createDataFrame(df).write.mode('overwrite').saveAsTable(pred_table_name)
    
    return pred_table_name
