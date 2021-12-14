import pandas as pd
import numpy as np
import acquire




def remove_outliers(df):

    # prep for outlier removal: not including categories fips, pools, zipcode
    cols_list = df.drop(columns=['fips','zipcode', 'pools'])


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
    
    return train, validate, test, df


# drop columns not needed from tain, validate, test
def prep(train,validate, test):
    
    # Drop columns not needed in train, validate, test
    train = train.drop(columns=['tax_amount'])
    validate = validate.drop(columns=['tax_amount'])
    test = test.drop(columns=['tax_amount'])
    
    return train, validate, test



def x_y(train,validate, test): 

    # establish target column
    target = 'tax_value'

    # create X & y version of train, validate, test with y the target and X are the features. 
    X_train = train.drop(columns=[target])
    y_train = train[target]

    X_validate = validate.drop(columns=[target])
    y_validate = validate[target]

    X_test = test.drop(columns=[target])
    y_test = test[target]
    
    return X_train, y_train, X_validate, y_validate, X_test, y_test


def scaler(X_train, y_train, X_validate, y_validate, X_test, y_test):
    
    from sklearn.preprocessing import MinMaxScaler

    # Create the scale container
    scaler = MinMaxScaler()


    # Fit the scaler to the features
    scaler.fit(X_train)

    # create scaled X versions 
    X_train_scaled = scaler.transform(X_train)
    X_validate_scaled = scaler.transform(X_validate)
    X_test_scaled = scaler.transform(X_test)

    # Convert numpy array to pandas dataframe for feature Engineering
    X_train_scaled = pd.DataFrame(X_train_scaled, index=X_train.index, columns=X_train.columns.to_list())
    X_validate_scaled = pd.DataFrame(X_validate_scaled, index=X_validate.index, columns=X_validate.columns.to_list())
    X_test_scaled = pd.DataFrame(X_test_scaled, index=X_test.index, columns=X_test.columns.to_list())
    
    return  X_train_scaled, X_validate_scaled, X_test_scaled



def selectkbest(X_train_scaled, y_train):
    from sklearn.feature_selection import SelectKBest, f_regression

    # Use f_regression stats test each column to find best 3 features
    f_selector = SelectKBest(f_regression, k=3)

    # find tthe best correlations with y
    f_selector.fit(X_train_scaled, y_train)

    # Creaet boolean mask of the selected columns. 
    feature_mask = f_selector.get_support()

    # get list of top K features. 
    skb_feature = X_train_scaled.iloc[:,feature_mask].columns.tolist()

    return skb_feature


def rfe(X_train_scaled, y_train):
    from sklearn.linear_model import LinearRegression
    from sklearn.feature_selection import RFE

    # create the ML algorithm container
    lm = LinearRegression()

    # create the rfe container with the the number of features I want. 
    rfe = RFE(lm, n_features_to_select=3)

    # fit RFE to the data
    rfe.fit(X_train_scaled,y_train)  

    # get the mask of the selected columns
    feature_mask = rfe.support_

    # get list of the column names. 
    rfe_feature = X_train_scaled.iloc[:,feature_mask].columns.tolist()

    return rfe_feature



def wrangle(train, validate, test):
   
    # drop columns not needed from train, validate, test
    train, validate, test = prep(train,validate, test)
        
    # Create X_train and y_train set for modeling
    X_train, y_train, X_validate, y_validate, X_test, y_test = x_y(train,validate, test)
    
    # Scale x sets for modeling
    X_train_scaled, X_validate_scaled, X_test_scaled = scaler(X_train, y_train, X_validate, y_validate, X_test, y_test)
    
    # feature engineering using selectKbest with stat test f_regression
    skb_feature = selectkbest(X_train_scaled, y_train)
    
    # feature engineering suing recursive featur elemination with linear regressin modeling
    rfe_feature = rfe(X_train_scaled, y_train)
    
    return X_train, X_validate, X_test, y_train, y_validate, y_test, X_train_scaled, X_validate_scaled, X_test_scaled, skb_feature, rfe_feature
    
    
    
    
    
    
    
    




    
    
    
    
    
    
    
    
    
    