

import pandas as pd
import numpy as np

from sklearn.metrics import mean_squared_error
from sklearn.linear_model import LinearRegression, LassoLars, TweedieRegressor
from sklearn.preprocessing import PolynomialFeatures
from sklearn.metrics import explained_variance_score

import prepare
from sklearn.metrics import mean_squared_error


# Removing Noise before modeling
# X_train = X_train.drop(columns=['pools','lot_size','bedrooms','bathrooms'])
# X_validate = X_validate.drop(columns=['pools','lot_size','bedrooms','bathrooms'])
# X_test = X_test.drop(columns=['pools','lot_size','bedrooms','bathrooms'])



# Create baseline model
def baseline(y_train, y_validate, y_test):

    # We need y_train and y_validate to be dataframes to append the new columns with predicted values. 
    y_train = pd.DataFrame(y_train)
    y_validate = pd.DataFrame(y_validate)
    y_test = pd.DataFrame(y_test)


    # Add target mean column as baseline check
    y_train['mean_pred'] = y_train.tax_value.mean()
    y_validate['mean_pred'] = y_validate.tax_value.mean()

    # Create Baseline RMSE of target mean
    from sklearn.metrics import mean_squared_error
    rmse_train = mean_squared_error(y_train.tax_value, y_train.mean_pred) ** .5
    rmse_validate = mean_squared_error(y_validate.tax_value, y_validate.mean_pred) ** .5
    
    # holds metrics from rmse eval
    metric_df = pd.DataFrame(data=[
                {
                    'model': 'mean_baseline', 
                    'RMSE_train': rmse_train,
                    'RMSE_validate': rmse_validate
                    }
                ])
    
    
    return y_train, y_validate, y_test, metric_df



# Create OLS (Linear Regression Model)
def ols(X_train_scaled, X_validate_scaled, y_train, y_validate, metric_df):
    from sklearn.linear_model import LinearRegression
     
    # create, fit, predict ols model
    ols = LinearRegression()
    ols.fit(X_train_scaled, y_train.tax_value)
    y_train['ols_pred'] = ols.predict(X_train_scaled)

    # create rmse
    rmse_train = mean_squared_error(y_train.tax_value, y_train.ols_pred) ** .5

    # predict validate
    y_validate['ols_pred'] = ols.predict(X_validate_scaled)

    # evaluate rmse of train and validate
    rmse_validate = mean_squared_error(y_validate.tax_value, y_validate.ols_pred) ** .5
    
    # add to eval to metric holder
    metric_df = metric_df.append({
        'model': 'ols_regressor', 
        'RMSE_train': rmse_train,
        'RMSE_validate': rmse_validate,
        }, ignore_index=True)
    
    return metric_df, y_validate


# create LassoLars Model
def lars(X_train_scaled, X_validate_scaled, y_train, y_validate, metric_df):
    
    from sklearn.linear_model import LassoLars
    
    # create object of model
    lars = LassoLars(alpha=0.01)

    # fit object to train data. Specify y_train column since it converted to a dataframe
    lars.fit(X_train_scaled, y_train.tax_value)

    # predict on train
    y_train['lars_pred'] = lars.predict(X_train_scaled)

    # create rmse
    rmse_train = mean_squared_error(y_train.tax_value, y_train.lars_pred) ** .5

    # predict validate
    y_validate['lars_pred'] = lars.predict(X_validate_scaled)

    # create rmse
    rmse_validate = mean_squared_error(y_validate.tax_value, y_validate.lars_pred) ** .5
    
    # add to metric holder
    metric_df = metric_df.append({
    'model': 'lasso_lars_.01', 
    'RMSE_train': rmse_train,
    'RMSE_validate': rmse_validate,
    }, ignore_index=True)

    return metric_df, y_validate



# create TweedieRegressor Model
def glm(X_train_scaled, X_validate_scaled, y_train, y_validate, metric_df):
    
    from sklearn.linear_model import TweedieRegressor
    
    # create the model object
    glm = TweedieRegressor(power=1, alpha=.01)


    # fit the model train data. Specify y_train columns since it was converted to dataframe 
    glm.fit(X_train_scaled, y_train.tax_value)

    # predict train
    y_train['glm_pred'] = glm.predict(X_train_scaled)

    # create rmse
    rmse_train = mean_squared_error(y_train.tax_value, y_train.glm_pred) ** .5

    # predict validate
    y_validate['glm_pred'] = glm.predict(X_validate_scaled)

    # evaluate train and validate rmse
    rmse_validate = mean_squared_error(y_validate.tax_value, y_validate.glm_pred) ** .5
    
    
    # added to metric holder
    metric_df = metric_df.append({
        'model': 'glm_poisson_.01', 
        'RMSE_train': rmse_train,
        'RMSE_validate': rmse_validate,
        }, ignore_index=True)
    
    return metric_df, y_validate



def poly(X_train_scaled, X_validate_scaled, X_test_scaled, y_train, y_validate, metric_df):
    
    # create polynomial features
    pf = PolynomialFeatures(degree=2)

    # fit and transform X_train_scaled
    X_train_degree2 = pf.fit_transform(X_train_scaled)

    # transform X_validate_scaled & X_test_scaled to new sets
    X_validate_degree2 = pf.transform(X_validate_scaled)
    X_test_degree2 =  pf.transform(X_test_scaled)

    # create the model object
    lm2 = LinearRegression()

    # fit the model train data. Specify y_train columns since it was converted to dataframe  
    lm2.fit(X_train_degree2, y_train.tax_value)

    # predict train
    y_train['lm2_pred'] = lm2.predict(X_train_degree2)

    # create rmse
    rmse_train = mean_squared_error(y_train.tax_value, y_train.lm2_pred) ** .5

    # predict validate
    y_validate['lm2_pred'] = lm2.predict(X_validate_degree2)

    # evaluate rmse
    rmse_validate = mean_squared_error(y_validate.tax_value, y_validate.lm2_pred) ** .5
    
    # add to metric holder
    metric_df = metric_df.append({
        'model': 'PolynomialRegressor', 
        'RMSE_train': rmse_train,
        'RMSE_validate': rmse_validate,
        }, ignore_index=True)

    return X_test_degree2, lm2, metric_df, y_validate



def poly_final(y_test, X_test_degree2, lm2):
    # predict train
    y_test['lm2_pred'] = lm2.predict(X_test_degree2)

    # create rmse
    rmse_test = mean_squared_error(y_test.tax_value, y_test.lm2_pred) ** .5
    
    return rmse_test



def metric(y_train, y_validate, y_test,X_train_scaled, X_validate_scaled, X_test_scaled):
    
    # run 
    y_train, y_validate, y_test, metric_df = baseline(y_train, y_validate, y_test)
    
    
    metric_df, y_validate = ols(X_train_scaled, X_validate_scaled, y_train, y_validate, metric_df)
    
    
    metric_df, y_validate = lars(X_train_scaled, X_validate_scaled, y_train, y_validate, metric_df)
    
    
    metric_df, y_validate = glm(X_train_scaled, X_validate_scaled, y_train, y_validate, metric_df)
    
    
    X_test_degree2, lm2, metric_df, y_validate = poly(X_train_scaled, X_validate_scaled, X_test_scaled, y_train, y_validate, metric_df)
    
    return metric_df, X_test_degree2, lm2, y_test, y_validate


















