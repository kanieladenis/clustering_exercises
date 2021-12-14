import pandas as pd
import numpy as np
import os




def acquire_zillow():
    
    # import env file for hostname, username, password, and db_name
    from env import host, user, password, db_name

    # Pass env file authentication to container 'url'
    url = f'mysql+pymysql://{user}:{password}@{host}/{db_name}'


    # define sql search for all records from all tables
    sql ='''
    SELECT prop17.parcelid, prop17.calculatedfinishedsquarefeet,
    prop17.yearbuilt, prop17.bedroomcnt, prop17.bathroomcnt, prop17.taxvaluedollarcnt, prop17.taxamount,
    prop17.fips, prop17.regionidzip, prop17.regionidneighborhood, prop17.poolcnt,
    prop17.lotsizesquarefeet, prop17.garagecarcnt, prop17.latitude, prop17.longitude, 
    pred17.transactiondate
    FROM properties_2017 prop17
    JOIN predictions_2017 pred17  USING (parcelid)
    LEFT JOIN propertylandusetype using (propertylandusetypeid)
    WHERE propertylandusetypeid='261'
    '''


    # load zillow data from saved csv or pull from sql server and save to csv
    import os
    file = 'zillow_data.csv'
    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0)
    else:
        df = pd.read_sql(sql,url)
        df.to_csv(file)
        
    return df




def clean_zillow(df):

    # drop record from 2018
    df = df.drop(index=52441)

    # rename columns for readability
    df = df.rename(columns = {'parcelid':'parcel_id',
                              'calculatedfinishedsquarefeet':'area',
                              'yearbuilt':'year_built',
                              'bedroomcnt':'bedrooms', 
                              'bathroomcnt':'bathrooms',
                              'taxvaluedollarcnt':'tax_value',
                              'taxamount':'tax_amount',
                              'transactiondate':'transaction_date',
                              'regionidzip':'zipcode',
                                 'regionidneighborhood':'neighborhood',
                                 'poolcnt':'pools',
                                 'lotsizesquarefeet':'lot_size',
                              'garagecarcnt':'garages'
                             })


    # Drop columns no longer needed. 
    df = df.drop(columns=['transaction_date', 'parcel_id'])


    # replace banks with NaN 
    df = df.replace('', np.nan)


    # replace pool, lot_size, l nan with 0
    df.pools = df.pools.replace(np.nan, 0)


    # replace lot_size, lot_size, l nan with meian
    df.lot_size = df.lot_size.replace(np.nan, df.lot_size.mean())
    
    #Set year_built to 1955, most used year
    df.year_built = df.year_built.replace(np.nan, 1955)

    # replace area Nan with most used area (1120)
    df.area = df.area.replace(np.nan, 1120)


    # Drop garages and neighborhood, too many NaN in garage and neighborhoo
    df = df.drop(columns=['garages','neighborhood'])


    # NaN left are few, dropping all NaNs
    df = df.dropna()


    # convert all columnst to integer for readability
    df = df.astype('int')
    
    return df


def get_zillow():
    
    df = acquire_zillow()
    
    df = clean_zillow(df)
    
    return df






