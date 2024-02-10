from pyspark.sql import DataFrame
from plotting_functions import *
from constants import *
import pandas as pd
from sklearn.feature_selection import RFE
from sklearn.ensemble import RandomForestRegressor
from pandas import DataFrame
import numpy as np
from statsmodels.tsa.stattools import adfuller
from sklearn.metrics import mean_squared_error
import time
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.seasonal import seasonal_decompose

class DataModeller:
    def __init__(self):
        return
    def rfe_hyperparameter_tuning(self, train_x, train_y, validation_x, validation_y) -> tuple:
        """
        Tune the hyperparameters of a random forest regression
        - Parameters:
            - train_x: features in train set
            - train_y: target in train set
            - validation_x: features in validation set
            - validation_y : target in validation set
        - Return:
            - tuple containing best fit hyperparameters
        """
        param_grid = {
        'n_trees': [25, 50, 100, 150],
        'max_features': ['sqrt', 'log2', None],
        'max_depth': [3, 6, 9],
        'max_leaf_nodes': [3, 6, 9],
        'num_features': [25, 50, 100, 200]
        }
        
        min_rmse = float("inf")
        parameters = ()

        # We are doing parameter selection like this BECAUSE
        # we cannot mix data from past and future
        for n in param_grid['n_trees']:
            for max_feature in param_grid['max_features']:
                for depth in param_grid['max_depth']:
                    for leaf in param_grid['max_leaf_nodes']:
                        for n_features in param_grid['num_features']:
                            # print(f'DOING: {n} {max_feature} {depth} {leaf} {n_features}')
                            rfe = RFE(RandomForestRegressor(n_estimators=n, 
                                                            random_state=1, 
                                                            max_leaf_nodes=leaf,
                                                            max_depth=depth,
                                                            max_features=max_feature), 
                                                            n_features_to_select=n_features)
                            rfe_fit = rfe.fit(train_x, train_y)
                            y_pred = rfe_fit.predict(validation_x)
                            rmse = mean_squared_error(validation_y, y_pred, squared=False)
                            if rmse < min_rmse:
                                min_rmse = rmse
                                parameters = (n, max_feature, depth, leaf, n_features)
        return parameters


    def rfe_check_uncertainty(self, model, validation_x, validation_y) -> None:
        """
        Test how uncertain the data is through bootstrapping samples
        - Parameters:
            - model: RFE Model used to predict
            - validation_x: use this to predict and compare with validation set
            - validation_y : target in validation set, will be bootstrapped
                            to create multiple test sets
        - Return:
            - Float signifying uncertainty, the sample variance between the RMSEs
        """
        possible_values = list(validation_y.index)
        vals_length = len(possible_values)
        TRIALS = 30
        rmse_vals = []
        
        # We will loop through each trial creating a boostrapped sample
        # And then we will get the variance of the RMSE values
        for _ in range(TRIALS):
            bootstrap_indices = np.random.choice(validation_y, size=(1, vals_length), replace=True)
            new_test_y = validation_y.iloc[bootstrap_indices[0]]
            new_test_x = validation_x.iloc[bootstrap_indices[0]]
            y_pred = model.predict(new_test_x)
            new_rmse = mean_squared_error(new_test_y, y_pred, squared=False)
            rmse_vals.append(new_rmse)

        mean_rmse = sum(rmse_vals)/len(rmse_vals)
        sample_var_top = 0
        for val in rmse_vals:
            sample_var_top += (val - mean_rmse)**2
        
        return sample_var_top/(len(rmse_vals)-1)


    def make_random_forest_regression(self)-> None:
        """
        Make our random forest regression, involving train, validation, test phases
        Saves results as a text file in /data/curated/
        """
        final_df = pd.read_parquet(FINAL_DF)

        # First check the result of this test

        # Verify the data is not SATIONARY, 
        # so the data is useful can see that the p-value
        # is < 0.05, so significant
        # result = adfuller(predictor.dropna())
        # print('p-value: %.30f' % result[1])

        # We want to introduce lag variables
        # This will make it so that our past values
        # PREDICT our future values
        dates = list(final_df[DATE])
        dataframe = DataFrame()
        LAG = 12
        for i in range(LAG, 0, -1):
            dataframe['t-' + str(i)] = final_df[TAXI_COUNT].shift(i)
        final_df = pd.concat([final_df, dataframe], axis=1)
        final_df.dropna(inplace=True)

        # Now we will make the relative indices of every entry
        # based on the unix time stamp
        TIME_INDEX = "time_index"
        seconds_to_days = 60*60*24
        first_time = int(time.mktime(final_df.iloc[0, :][DATE].timetuple()))
        # We also reset the index to the relativee time in days away
        # from the starting day
        final_df[TIME_INDEX] = final_df[DATE].apply(lambda dt: int(time.mktime(dt.timetuple())))
        final_df[TIME_INDEX] = final_df[TIME_INDEX].apply(lambda dt: int((dt - first_time)/seconds_to_days))
        final_df = final_df.reset_index(drop=True)
        final_df.set_index(TIME_INDEX, inplace=True)
        final_df.drop(columns=[DATE], axis=1, inplace=True)

        # Now do the train_test_split
        # Get the indexes required for the train_test split
        num_rows = len(final_df)
        train_index = round(num_rows * 0.6)
        validation_index = round(train_index + (num_rows * 0.2))
        test_index = round(validation_index + (num_rows * 0.2))

        # Now that you have the indices, make the train-validation-test splits
        # et's fit hyperparameters into the model
        train_set = final_df.iloc[:train_index, :]
        validation_set = final_df.iloc[train_index:validation_index, :]
        test_set = final_df.iloc[validation_index:test_index, :]
        test_set = final_df.iloc[validation_index:test_index, :]

        train_set_x = train_set.loc[:, train_set.columns != TAXI_COUNT]
        train_set_y = train_set.loc[:, TAXI_COUNT]

        validation_set_x = validation_set.loc[:, train_set.columns != TAXI_COUNT]
        validation_set_y = validation_set.loc[:, TAXI_COUNT]

        test_set_x = test_set.loc[:, train_set.columns != TAXI_COUNT]
        test_set_y = test_set.loc[:, TAXI_COUNT]

        # Now get the parameters you need
        n_trees, max_feature, depth, leaf, n_features =\
            self.rfe_hyperparameter_tuning(train_set_x, train_set_y, validation_set_x, validation_set_y)
        params = [n_trees, max_feature, depth, leaf, n_features]
        # n_trees, max_feature, depth, leaf, n_features = 50, "log2", 9, 6, 25

        # Now we can test the model
        test_forest = RandomForestRegressor(n_estimators=n_trees, random_state=1, \
                    max_leaf_nodes=leaf,\
                        max_depth=depth,\
                            max_features=max_feature)
        
        test_rfe = RFE(test_forest, n_features_to_select=n_features)
        test_rfe.fit(train_set_x, train_set_y)
        y_pred = test_rfe.predict(test_set_x)
        

        rmse = mean_squared_error(test_set_y, y_pred, squared=False)
        uncertainty = round(self.rfe_check_uncertainty(test_rfe, validation_set_x, 
                                    validation_set_y), 5)

        # We will save our results in a text file, this will be used 
        with open(f'{CURATED_DIR}/rfe_results.txt', 'w') as fp:
            fp.writelines(" ".join([str(date) for date in dates[validation_index:test_index]]))
            fp.write('\n')
            fp.writelines(str(params))
            fp.write('\n')
            fp.writelines(" ".join([str(x) for x in y_pred.tolist()]))
            fp.write('\n')
            fp.writelines(" ".join([str(x) for x in test_set_y.tolist()]))
            fp.write('\n')
            fp.writelines(str(rmse))
            fp.write('\n')
            fp.writelines(str(uncertainty))

        # print(f'FINAL RMSE: {rmse}')
        # print(f'UNCERTAINTY OF RMSE: {uncertainty}')
        return


    def arima_hyperparamter_tuning(self, train_x, train_y, validation_x, validation_y) -> tuple:
        """
        Set up hyperparametesr of the SARIMA time series
        - Parameters
            - p: Indicates number of previous values current value
                is dependent on
            - d: Difference order, degree to which data needs to be
                differenced to achieve stationarity
            - q: Number of lagged forecast errors used in the model
        - Returns
            - Tuple containing desired parameters
        """

        # Stationarity is present in the data, so no differencing
        # is neeed to achieve stationarity
        param_grid = {
        'p': [1, 3, 5, 7, 10],
        'q': [1, 3, 5, 7, 10]
        }

        # Store this to keep track of min error
        min_rmse = float("inf")
        params = ()

        # We pick parameters based on the standarad error
        # with the validation set
        for p in param_grid['p']:
            for q in param_grid['q']:
                print(f'Doing: {p}, {1}, {q}')
                model = ARIMA(endog=train_y, order=(p, 1, q),
                                exog=train_x)
                model_fit = model.fit()
                # Forecast using the ARIMA model
                forecast_steps = len(validation_y)
                fit_vals = model_fit.forecast(steps=forecast_steps,
                                                exog=validation_x)
                rmse = mean_squared_error(fit_vals, validation_y)
                if rmse < min_rmse:
                    min_rmse = rmse
                    params = (p, 1, q)
        return params


    def arima_check_uncertainty(self, model, validation_x, validation_y) -> None:
        """
        Test how uncertain the data is through bootstrapping samples
        - Parameters:
            - model: RFE Model used to predict
            - validation_x: use this to predict and compare with validation set
            - validation_y : target in validation set, will be bootstrapped
                            to create multiple test sets
        - Return:
            - Float signifying uncertainty, the variance between the RMSEs
            of the ARIMA model
        """
        possible_values = list(validation_y.index)
        vals_length = len(possible_values)
        TRIALS = 30
        rmse_vals = []
        
        # We will loop through each trial creating a boostrapped sample
        # And then we will get the variance of the RMSE values
        for _ in range(TRIALS):
            bootstrap_indices = np.random.choice(validation_y, 
                                                size=(1, vals_length), 
                                                replace=True)
            # print(bootstrap_indices)
            new_test_y = validation_y.iloc[bootstrap_indices[0]]
            new_test_x = validation_x.iloc[bootstrap_indices[0]]
            forecast_steps = len(new_test_y)
            y_pred = model.forecast(steps=forecast_steps,
                                    exog=new_test_x)
            new_rmse = mean_squared_error(new_test_y, y_pred, squared=False)
            rmse_vals.append(new_rmse)

        mean_rmse = sum(rmse_vals)/len(rmse_vals)
        sample_var_top = 0
        for val in rmse_vals:
            sample_var_top += (val - mean_rmse)**2
        
        return sample_var_top/(len(rmse_vals)-1)


    def make_arima_model(self) -> None:
        """
        Make a SARIMA model from our taxi shootings dataset
        Saves results as a txt file /data/curated/
        """
        final_df = pd.read_parquet(FINAL_DF)
        dates = list(final_df[DATE])
        # No need to set our date to datetime datatype
        final_df.set_index(DATE, inplace=True)
        

        # Now we want to do seasonal decomposition
        decomposition = seasonal_decompose(
        final_df[TAXI_COUNT], model='additive', period=12)

        # We will do the same train-validation-test split
        
        # Now do the train_test_split
        # Get the indexes required for the train_test split
        num_rows = len(final_df)
        train_index = round(num_rows * 0.6)
        validation_index = round(train_index + (num_rows * 0.2))
        test_index = round(validation_index + (num_rows * 0.2))

        # Now that you have the indices, make the train-validation-test splits
        # et's fit hyperparameters into the model
        train_set = final_df.iloc[:train_index, :]
        validation_set = final_df.iloc[train_index:validation_index, :]
        test_set = final_df.iloc[validation_index:test_index, :]
        test_set = final_df.iloc[validation_index:test_index, :]

        train_set_x = train_set.loc[:, train_set.columns != TAXI_COUNT]
        train_set_y = train_set.loc[:, TAXI_COUNT]

        validation_set_x = validation_set.loc[:, train_set.columns != TAXI_COUNT]
        validation_set_y = validation_set.loc[:, TAXI_COUNT]

        params = self.arima_hyperparamter_tuning(train_set_x, train_set_y, 
                                            validation_set_x, validation_set_y)
        p, d, q = params

        # Now that we have the parameters for the model, we can do
        # testing
        test_set_x = test_set.loc[:, train_set.columns != TAXI_COUNT]
        test_set_y = test_set.loc[:, TAXI_COUNT]

        test_model = model = ARIMA(endog=train_set_y, order=(p, d, q),
                        exog=train_set_x)
        model_fit = model.fit()
        forecast_steps = len(test_set_y)
        y_pred = model_fit.forecast(steps=forecast_steps,
                                            exog=test_set_x)
        
        # Get the RMSE of this dataset as well as the 
        # uncertainty of this RMSE
        rmse = mean_squared_error(y_pred, test_set_y)
        uncertainty = self.arima_check_uncertainty(model_fit, validation_set_x, 
                                validation_set_y)

        # Now we can save all the data once again in a txt file
        with open(f'{CURATED_DIR}/arima_results.txt', 'w') as fp:
            fp.writelines([str(date) for date in dates[validation_index:test_index]])
            fp.write('\n')
            fp.writelines(str(params))
            fp.write('\n')
            fp.writelines(" ".join([str(x) for x in y_pred.tolist()]))
            fp.write('\n')
            fp.writelines(" ".join([str(x) for x in test_set_y.tolist()]))
            fp.write('\n')
            fp.writelines(str(rmse))
            fp.write('\n')
            fp.writelines(str(uncertainty))
            
        # print(f'FINAL RMSE: {rmse}')
        # print(f'UNCERTAINTY OF RMSE: {uncertainty}')
        return


if __name__ == "__main__":
    data_modeller = DataModeller()
    data_modeller.make_random_forest_regression()
    data_modeller.make_arima_model()