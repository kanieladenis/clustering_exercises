# ignore warnings
import warnings
warnings.filterwarnings("ignore")

# Wrangling
import pandas as pd
import numpy as np


def get_zillow():

    # import env file for hostname, username, password, and db_name
    from env import host, user, password, db_name

    # Pass env file authentication to container 'url'
    url = f'mysql+pymysql://{user}:{password}@{host}/{db_name}'

        # define sql search for all records from all tables in Zillow database
    sql = """
    SELECT prop.*,
           pred.logerror, 
           pred.transactiondate, 
           air.airconditioningdesc, 
           arch.architecturalstyledesc, 
           build.buildingclassdesc, 
           heat.heatingorsystemdesc, 
           landuse.propertylandusedesc, 
           story.storydesc, 
           construct.typeconstructiondesc 
    FROM   properties_2017 prop  
    JOIN (SELECT parcelid,
                      logerror,
                      Max(transactiondate) transactiondate 
               FROM   predictions_2017 
               GROUP  BY parcelid, logerror) pred
           USING (parcelid)
    JOIN propertylandusetype USING (propertylandusetypeid)
    LEFT JOIN airconditioningtype air USING (airconditioningtypeid) 
    LEFT JOIN architecturalstyletype arch USING (architecturalstyletypeid) 
    LEFT JOIN buildingclasstype build USING (buildingclasstypeid) 
    LEFT JOIN heatingorsystemtype heat USING (heatingorsystemtypeid) 
    LEFT JOIN propertylandusetype landuse USING (propertylandusetypeid) 
    LEFT JOIN storytype story USING (storytypeid) 
    LEFT JOIN typeconstructiontype construct USING (typeconstructiontypeid) 
    WHERE  prop.latitude IS NOT NULL 
    AND prop.longitude IS NOT NULL
    AND transactiondate < '2018-01-01' 
    AND propertylandusetypeid = 261 
    """
    
    # load zillow data from saved csv or pull from sql server and save to csv
    import os
    file = 'zillow_data.csv'
    if os.path.isfile(file):
        df = pd.read_csv(file, index_col=0)
    else:
        df = pd.read_sql(sql,url)
        df.to_csv(file)
        
    return df


def get_null_tables():
    
    # create dataframe that has column name as first column
    col_nulls = pd.DataFrame()
    col_nulls['columns_name'] = df.isna().sum().index
    
    # create new column that hold the sum of nulls from each column
    col_nulls['row_null_count'] = df.isna().sum().values
    
    # create new column that hold the average of nulls from each column
    col_nulls['row_null_percent'] = df.isna().mean().values
    
    # sort values by percent
    col_nulls = col_nulls.sort_values(by=['row_null_percent'], ascending=False)
    
    # create df for column with null count
    row_nulls = pd.DataFrame(df.isna().sum(axis=1), columns=['num_null_cols'])    
    
    # Create df with number of rows with a specific number of null columns
    row_nulls = pd.DataFrame(df.isna().sum(axis=1).value_counts(), columns=['num_rows_with_n_null_cols'])
    
    # make first columnb the number of nulls
    row_nulls = dft2.reset_index()
    
    # rename index to match values
    row_nulls = dft2.rename(columns={'index':'n_null_cols'})
    
    # create columsn for percent of null cols
    row_nulls['percent_null_cols'] = row_nulls.n_null_cols / df.shape[1]
    
    # sort df by percentn of null cols
    row_nulls = row_nulls.sort_values(by=['percent_null_cols'], ascending=False)
    
    return col_nulls, row_nulls


def handle_missing_values(df, percent_required_cols = .5, percent_required_rows = .7):
    
    # set threshold for min of values in columns for dropping
    thresh_col = int(round(percent_required_cols * df.shape[0]))
    
    # drop columns that don't meed threshhold for non-null values (rows without nulls)
    df = df.dropna(axis=1, thresh=thresh_col)
    
    # set threshold for min non-null values for rows (cols without nulls)
    thresh_row = int(round(percent_required_rows * df.shape[1]))
    
    # drop rows with don't meet threshold for non-null values for columns
    df = df.dropna(axis=0, thresh=thresh_row)
    
    return df


def remove_nulls(df):
    
    # remove columns that are not useful
    df = df.drop(columns=['parcelid',
                    'id',
                     #Description of the allowed land uses (zoning) for that property
                     'propertyzoningdesc', 
                     # Finished living area
                     'finishedsquarefeet12',
                     #  Census tract and block ID combined - also contains blockgroup assignment by extension
                     'censustractandblock',
                          # Type of land use the property is zoned for
                          'propertylandusetypeid',
                          #  Type of home heating system
                          'heatingorsystemdesc',
                         'buildingqualitytypeid',
                          'unitcnt'
                         ])



    # relacing nulls with 'None'
    df.heatingorsystemtypeid.fillna('None', inplace=True)


    # dropping the rest of the nulls
    df.dropna(inplace=True)

    return df



def remove_outliers(df):

    # prep for outlier removal: not including categories fips, pools, zipcode
    cols_list = df.drop(columns=[
 'fips',

 'propertycountylandusecode',
 'rawcensustractandblock',
 'regionidcity',
 'regionidcounty',
 'regionidzip',
 
 'propertylandusedesc',
 
 'heatingorsystemtypeid',
    'transactiondate'])


    # remove outliers from each column in cols_list
    for col in cols_list:

        q1, q3 = df[col].quantile([.25, .75])  # get quartiles

        iqr = q3 - q1   # calculate interquartile range

        upper_bound = q3 + 2 * iqr   # get upper bound
        lower_bound = q1 - 2 * iqr   # get lower bound

        # return dataframe without outliers

        df = df[(df[col] > lower_bound) & (df[col] < upper_bound)]
        
    return df


def remove_outliers(df):

    # prep for outlier removal: not including categories fips, pools, zipcode
    cols_list = df.drop(columns=[
    'heatingorsystemtypeid',
     
     'calculatedbathnbr',
     'calculatedfinishedsquarefeet',
     'fips',
     'fullbathcnt',
     
  
     'propertycountylandusecode',
     'rawcensustractandblock',
     'regionidcity',
     'regionidcounty',
     'regionidzip',
     'roomcnt',
     'yearbuilt',
 
     
     'assessmentyear',
     'landtaxvaluedollarcnt',
    
     'propertylandusedesc',
     'logerror',
     'transactiondate'])


    # remove outliers from each column in cols_list
    for col in cols_list:

        q1, q3 = df[col].quantile([.25, .75])  # get quartiles

        iqr = q3 - q1   # calculate interquartile range

        upper_bound = q3 + 2 * iqr   # get upper bound
        lower_bound = q1 - 2 * iqr   # get lower bound

        # return dataframe without outliers

        df = df[(df[col] > lower_bound) & (df[col] < upper_bound)]
        
    return df
    
    
    
    

def split(df):
    
    from sklearn.model_selection import train_test_split
    
    # use remove outliers function
    df = remove_outliers(df)
    
    # train/validate/test split
    train_validate, test = train_test_split(df, test_size=.2, random_state=123)
    train, validate = train_test_split(train_validate, test_size=.3, random_state=123)
    
    return train, validate, test



def wrangle_zillow():
    
    df = get_zillow()
    
    df = handle_missing_values(df, percent_required_cols = .5, percent_required_rows = .7)
    
    df = remove_nulls(df)
    
    df = remove_outliers(df)
    
    train, validate, test = split(df)
    
    return df, train, validate, test
    


    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

