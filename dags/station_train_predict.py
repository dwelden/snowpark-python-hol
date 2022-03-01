
def station_train_predict_func(input_data: list, 
                               input_columns_str: str, 
                               target_column: str,
                               cutpoint: int, 
                               max_epochs: int) -> str:
    
    input_columns = input_columns_str.split(' ')
    feature_columns = input_columns.copy()
    feature_columns.remove('DATE')
    feature_columns.remove(target_column)
    
    from torch import tensor
    import pandas as pd
    from pytorch_tabnet.tab_model import TabNetRegressor
    
    model = TabNetRegressor()

    df = pd.DataFrame(input_data, columns = input_columns)
    
    y_valid = df[target_column][-cutpoint:].values.reshape(-1, 1)
    X_valid = df[feature_columns][-cutpoint:].values
    y_train = df[target_column][:-cutpoint].values.reshape(-1, 1)
    X_train = df[feature_columns][:-cutpoint].values
    
    batch_df = pd.DataFrame(range(2,65,2), columns=['batch_size'])
    batch_df['batch_remainder'] = len(X_train)%batch_df['batch_size']
    optimal_batch_size=int(batch_df['batch_size'].where(batch_df['batch_remainder']==batch_df['batch_remainder'].min()).max())
    
    print('Selected batch size '+str(optimal_batch_size)+' for input data size: '+str(len(X_train)))

    model.fit(
        X_train, y_train,
        eval_set=[(X_valid, y_valid)],
        max_epochs=max_epochs,
        patience=100,
        batch_size=optimal_batch_size, 
        virtual_batch_size=optimal_batch_size/2,
        num_workers=0,
        drop_last=True)
    
    df['PRED'] = model.predict(tensor(df[feature_columns].values))
   
    return [df.values.tolist(), df.columns.tolist()]
