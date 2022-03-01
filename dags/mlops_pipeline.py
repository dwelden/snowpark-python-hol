
def materialize_holiday_table(session, start_date, end_date, holiday_table_name) -> str:
    from citibike_ml.feature_engineering import generate_holiday_df
    from datetime import datetime

    holiday_df = generate_holiday_df(session=session, start_date=start_date, end_date=datetime.now())
    holiday_df.write.mode('overwrite').saveAsTable(holiday_table_name)
    
    return holiday_table_name

def materialize_precip_table(session, start_date, end_date, precip_table_name) -> str:
    from citibike_ml.feature_engineering import generate_precip_df
    from datetime import datetime

    precip_df = generate_precip_df(session=session, start_date=start_date, end_date=datetime.now())
    precip_df.write.mode('overwrite').saveAsTable(precip_table_name)
    
    return precip_table_name

def deploy_pred_train_udf(session, function_name, model_stage_name) -> str:
    from citibike_ml.station_train_predict import station_train_predict_func

    dep = 'pytorch_tabnet.zip'
    source_dir = './include/'

    session.clearImports()
    session.addImport(source_dir+dep)
    session.addImport('citibike_ml')

    station_train_predict_udf = session.udf.register(station_train_predict_func, 
                                                  name="station_train_predict_udf",
                                                  is_permanent=True,
                                                  stage_location='@'+str(model_stage_name), 
                                                  replace=True)
    return station_train_predict_udf.name


def generate_feature_views(session, 
                           clone_table_name, 
                           feature_view_name, 
                           holiday_table_name, 
                           precip_table_name, 
                           target_column, 
                           top_n) -> list:
    from citibike_ml.feature_engineering import generate_features
    from snowflake.snowpark import functions as F

    feature_view_names = list()
    
    top_n_station_ids = session.table(clone_table_name).filter(F.col('START_STATION_ID').is_not_null()) \
                                                       .groupBy('START_STATION_ID') \
                                                       .count() \
                                                       .sort('COUNT', ascending=False) \
                                                       .limit(top_n) \
                                                       .collect()
    top_n_station_ids = [stations['START_STATION_ID'] for stations in top_n_station_ids]

    for station in top_n_station_ids:
        feature_df = generate_features(session=session, 
                                       input_df=session.table(clone_table_name)\
                                                       .filter(F.col('START_STATION_ID') == station), 
                                       holiday_table_name=holiday_table_name, 
                                       precip_table_name=precip_table_name)

        input_columns_str = str(' ').join(feature_df.columns).replace('\"', "")

        feature_df = feature_df.select(F.array_agg(F.array_construct(F.col('*'))).alias('input_data'), 
                                       F.lit(station).alias('station_id'),
                                       F.lit(input_columns_str).alias('input_column_names'),
                                       F.lit(target_column).alias('target_column'))  

        station_feature_view_name = feature_view_name.replace('<station_id>', station)
        feature_df.createOrReplaceView(station_feature_view_name)
        feature_view_names.append(station_feature_view_name)

    return feature_view_names


def train_predict_feature_views(session, station_train_pred_udf_name, feature_view_names, pred_table_name) -> str:
    from snowflake.snowpark import functions as F
    import pandas as pd
    import ast
    
    cutpoint=365
    max_epochs=1000
    
    for view in feature_view_names:
        feature_df = session.table(view)
        output_df = feature_df.select(F.call_udf(station_train_pred_udf_name, 
                                                 'INPUT_DATA', 
                                                 'INPUT_COLUMN_NAMES', 
                                                 'TARGET_COLUMN', 
                                                 F.lit(cutpoint), 
                                                 F.lit(max_epochs))).collect()

        df = pd.DataFrame(data = ast.literal_eval(output_df[0][0])[0], 
                      columns = ast.literal_eval(output_df[0][0])[1])

        df['DATE'] = pd.to_datetime(df['DATE']).dt.date
        df['STATION_ID'] = feature_df.select('STATION_ID').collect()[0][0]

        output_df = session.createDataFrame(df).write.saveAsTable(pred_table_name)
    
    return pred_table_name
