import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def viz_1(y_validate):
    # y_validate.head()
    plt.figure(figsize=(16,8))
    plt.plot(y_validate.tax_value, y_validate.mean_pred, alpha=.5, color="gray", label='_nolegend_')
    plt.annotate("Baseline: Predict Using Mean", (16, 9.5))
    plt.plot(y_validate.tax_value, y_validate.tax_value, alpha=.5, color="black", label='_nolegend_')
    plt.annotate("The Ideal Line: Predicted = Actual", (.5, 3.5), rotation=20)

    plt.scatter(y_validate.tax_value, y_validate.ols_pred, 
                alpha=.5, color="red", s=100, label="Model: LinearRegression")
    plt.scatter(y_validate.tax_value, y_validate.glm_pred, 
                alpha=.5, color="yellow", s=100, label="Model: TweedieRegressor")
    plt.scatter(y_validate.tax_value, y_validate.lm2_pred, 
                alpha=.5, color="green", s=100, label="Model: Polynomial w/ 2nd Degree")
    plt.legend()
    plt.xlabel("Actual Tax Value")
    plt.ylabel("Predicted Tax Value")
    plt.title("Models Need Improvement")
    # plt.annotate("The polynomial model appears to overreact to noise", (2.0, -10))
    # plt.annotate("The OLS model (LinearRegression)\n appears to be most consistent", (15.5, 3))
    plt.show()


def viz_2(y_validate):    
    # plot to visualize actual vs predicted. 
    plt.figure(figsize=(16,8))
    plt.hist(y_validate.tax_value, color='blue', alpha=.5, label="Actual Tax Values")
    plt.hist(y_validate.ols_pred, color='red', alpha=.5, label="Model: LinearRegression")
    plt.hist(y_validate.glm_pred, color='yellow', alpha=.5, label="Model: TweedieRegressor")
    plt.hist(y_validate.lm2_pred, color='green', alpha=.5, label="Model Polynomial w/ 2nd Degree")
    plt.xlabel("Tax Values")
    plt.ylabel("Number of Homes ")
    plt.title("Comparing the Distribution of Actual Tax Values to Distributions of Predicted Tax Values for the Top Models")
    plt.legend()
    plt.show()
    
    
    
def viz_3(y_validate):
    # y_validate.head()
    plt.figure(figsize=(16,8))
    plt.axhline(label="No Error")
    plt.scatter(y_validate.tax_value,  y_validate.tax_value - y_validate.ols_pred, 
                alpha=.5, color="red", s=100, label="Model: LinearRegression")
    plt.scatter(y_validate.tax_value, y_validate.tax_value - y_validate.glm_pred, 
                alpha=.5, color="yellow", s=100, label="Model: TweedieRegressor")
    plt.scatter(y_validate.tax_value, y_validate.tax_value - y_validate.lm2_pred, 
                alpha=.5, color="green", s=100, label="Model: Polynomial w/ 2nd Degree")
#     plt.scatter(y_validate.tax_value, y_validate.ols_pred - y_validate.tax_value , 
#                 alpha=.5, color="red", s=100, label="Model: LinearRegression")
#     plt.scatter(y_validate.tax_value, y_validate.glm_pred - y_validate.tax_value, 
#                 alpha=.5, color="yellow", s=100, label="Model: TweedieRegressor")
#     plt.scatter(y_validate.tax_value, y_validate.lm2_pred - y_validate.tax_value, 
#                 alpha=.5, color="green", s=100, label="Model: Polynomial w/ 2nd Degree")
    plt.legend()
    plt.xlabel("Actual Tax Value")
    plt.ylabel("Residual/Error: Predicted Tax Value - Actual Tax Value")
    # plt.title("Do the size of errors change as the actual value changes?")
    # plt.annotate("The polynomial model appears to overreact to noise", (2.0, -10))
    # plt.annotate("The OLS model (LinearRegression)\n appears to be most consistent", (15.5, 3))
    plt.show()

    
    

# ---------------------------------

# # Show house vlaues create than mean by coorindates, shows a map
# value_map = df[df.tax_value > df.tax_value.mean()]
# sns.relplot(data=value_map, x='longitude', y='latitude', hue='tax_value')
# plt.show()


# # Shows house value distribution across counties
# plt.figure(figsize=(10,5))
# sns.displot(x=train.tax_value, hue=train.fips)
# plt.xlabel('House Tax Value')
# plt.show()


# # Show heat map of features
# corr_table = train.drop(columns=['fips']).corr()
# plt.figure(figsize=(10,8))
# sns.heatmap(corr_table, cmap='Purples', annot=True, linewidth=0.5, mask= np.triu(corr_table))
# plt.show()



# # Shows that value increases with area & year_built
# sns.relplot(x='year_built', y='area', data=train,  hue='tax_value', kind='scatter')
# plt.xlabel('Year Built')
# plt.ylabel('House Area Sqft')
# plt.show()


# #shows that increasd area tends to have increases bathrooms. Slight correlation to increased value.
# sns.relplot(x='area', y='tax_value', data=train, hue='bathrooms', kind='scatter')
# plt.xlabel('House Area in Sqft')
# plt.ylabel('House Tax Value')
# plt.show()

# #show average tax value of homes across couty fips
# sns.barplot(x=df.fips, y=df.tax_value)
# plt.xlabel('Average Tax Amount')
# plt.ylabel('County Identifer')
# plt.show()



# # Shows tax value distribution by fips
# sns.displot(x=train.tax_value, hue=train.fips)
# plt.xlabel('Home Tax Value')
# plt.ylabel('Home Count')
# plt.show()



# # Shows increased area has year increased and more high areas have high value
# sns.relplot(x='year_built', y='tax_value', data=train, hue='area', kind='scatter')
# plt.xlabel('Year Built')
# plt.ylabel('House Tax Value')
# plt.show()

# # compare area vs value by fips
# sns.relplot(x='area', y='tax_value', data=train, hue='fips', kind='scatter')
# plt.ylabel('House Tax Value')
# plt.xlabel('House Area in Sqft')
# plt.show()


# # shows that 6037 has majority of properties, 6059 has more high value properties
# sns.relplot(x='year_built', y='tax_value', data=train, hue='fips', kind='scatter')
# plt.ylabel('House Tax Value')
# plt.xlabel('Year Built')
# plt.show()