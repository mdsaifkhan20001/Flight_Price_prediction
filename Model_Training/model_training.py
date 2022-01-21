from sklearn.metrics import mean_absolute_error, r2_score
import pandas as pd
import matplotlib.pyplot as plt
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
# from sklearn.ensemble import RandomForestRegressor
import pickle
import seaborn as sns
import os
import shutil


class trainModel:
    """
        This class will be used for dataPreprocessing
    """

    def __init__(self, logger, data_path, log_file_path, clients_path):
        try:

            self.log = logger
            self.log.info('Entered into class >>>>>> trainModel')
            self.df = pd.read_csv(data_path)
            self.clients_path = clients_path
            self.log_file_path = log_file_path

        except Exception as e:
            self.log.warning('Exception occurred during class initialization, {}'.format(e))
            shutil.copy(self.log_file_path, self.clients_path + '/' + 'logfile.txt')

            raise Exception

    def choose_model(self, df):
        """
        Method Name:choose_model
        Description:It will choose the best model for our data

        Input:dataframe
        Output: best_model
        On Failure: Raise Exception
        """

        try:
            if not os.path.isdir('Models_histplot'):
                os.makedirs('Models_histplot')

            xgb = XGBRegressor()

            param_grid_xgb = {
                'learning_rate': [0.04],
                'max_depth': [15],
                'n_estimators': [155]}

            # param_grid_xgb = {
            #     'learning_rate': [0.1, .03, 0.05, 0.04],
            #     'max_depth': [3, 5, 6, 7, 12, 15, 16],
            #     'n_estimators': [100, 135, 155, 165]}

            X = df.drop(['Price'], axis=1)
            y = df['Price']

            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

            selection = GridSearchCV(xgb, cv=3, param_grid=param_grid_xgb,
                                     scoring='neg_mean_absolute_error')
            self.log.info('Hyperparameter tuning started')
            selection.fit(X_train, y_train)
            self.log.info('Hyperparameter tuning finished with best params as {}', selection.best_params_)

            best_model = XGBRegressor(learning_rate=selection.best_params_['learning_rate'],
                                      max_depth=selection.best_params_['max_depth'],
                                      n_estimators=selection.best_params_['n_estimators'])

            # best_model = RandomForestRegressor() If you want to use rf then uncomment the top line
            best_model.fit(X_train, y_train)
            mae = mean_absolute_error(y_test, best_model.predict(X_test))
            self.log.info('{} is error for the model'.format(mae))

            r2 = r2_score(y_test, best_model.predict(X_test))
            self.log.info('{} is r2 score for the model'.format(r2, 'XGBRegressor'))

            if not os.path.isdir('Models_plot'):
                os.makedirs('Models_plot')
                self.log.info('Created {} folder'.format('Models_plot'))

            sns.histplot(y_test - best_model.predict(X_test))
            plt.title('Error')
            plt.xlabel('Values')
            plt.ylabel('Counts')
            # plt.show()
            plt.savefig('Models_plot/model_hist.PNG')

            # Saving the second figure
            plt.scatter(y_test, best_model.predict(X_test))
            plt.plot(range(0, max(y_test)), range(0, max(y_test)))
            plt.title("How's the predicted data")
            plt.xlabel('y_test')
            plt.ylabel('Y_predicted')
            # plt.show()
            plt.savefig('Models_plot/plot2.PNG')

            return best_model

        except Exception as e:
            self.log.warning('Exception occurred inside method choose_model, {}'.format(e))
            shutil.copy(self.log_file_path, self.clients_path + '/' + 'logfile.txt')
            raise Exception

    def saving_models(self, model):
        """
            Method Name:saving_models
            Description:This will simply save the models presented inside the dictionary models
            Input:dictionary models
            Output: Nothing
            On Failure: Raise Exception
        """
        try:

            if not os.path.isdir('Model_for_prediction'):
                os.makedirs('Model_for_prediction')

            filename = 'Model_for_prediction/model' + '.pickle'
            pickle.dump(model, open(filename, 'wb'))

        except Exception as e:
            self.log.warning('Exception occurred while saving models, {}'.format(e))
            shutil.copy(self.log_file_path, self.clients_path + '/' + 'logfile.txt')
            raise Exception

    def start_training(self):
        df = self.df.copy()
        best_model = self.choose_model(df)
        self.saving_models(best_model)
        self.log.info('Exiting from class <<<<<< trainModel')
    #       Now we have got our respective models for each clusters
    #       now we are going to save it which will be needed later for prediction part
