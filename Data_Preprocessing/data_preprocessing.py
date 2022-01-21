import pandas as pd
import pickle
import os
import numpy as np
import shutil


class dataPreprocessing:
    """
        This class will be used for dataPreprocessing
    """

    def __init__(self, logger, train_path, log_file_path, clients_path, ):
        self.log = logger
        self.path = train_path
        self.log_file_path = log_file_path
        self.clients_path = clients_path
        self.log.info('Entered into class >>>>>> dataPreprocessing')

    def handling_categorical_var_for_training_data(self, df):
        """
            Method Name:handling_categorical_var_for_training_data
            Description:Handles categorical variable for training data
            Input:dataframe
            Output: dataframe and all_col_for_test which will be used while testing purpose
            On Failure: Raise Exception
        """
        try:

            columns = ['Airline', 'Source', 'Destination', 'Total_Stops', 'Additional_Info']
            all_col_for_test = {}
            for col in columns:
                new = df.groupby([col])['Price']

                u_val = df[col].unique()

                all_max = {}
                for val in u_val:
                    all_max[val] = max(new.get_group(val))

                def sort_max(x):
                    return all_max[x]

                new = sorted(all_max, key=sort_max, reverse=False)

                all_max = dict(zip(new, range(1, len(new) + 1)))
                all_col_for_test[col] = all_max.copy()
                df[col] = df[col].map(all_max)

            self.log.info('Handling Categorical Variables Completed')
            return df, all_col_for_test

        except Exception as e:
            self.log.warning('Exception occurred while handling Categorical Variables {}'.format(e))
            shutil.copy(self.log_file_path, self.clients_path + '/' + 'logfile.txt')
            raise Exception

    def date_time_var(self, df):
        """
            Method Name:date_time_var
            Description:Handles datetime variables present inside dataframe df
            Input:dataframe
            Output: dataframe
            On Failure: Raise Exception
            """
        try:

            # Departure time is when a plane leaves the gate.
            # Similar to Date_of_Journey we can extract values from Dep_Time

            # Extracting Hours
            df["Dep_hour"] = pd.to_datetime(df["Dep_Time"]).dt.hour

            # Extracting Minutes
            df["Dep_min"] = pd.to_datetime(df["Dep_Time"]).dt.minute

            # Now we can drop Dep_Time as it is of no use
            df.drop(["Dep_Time"], axis=1, inplace=True)

            df['Dep_hour'] = df['Dep_hour'] + round(df['Dep_min'] / 60, 1)
            df.drop('Dep_min', axis=1, inplace=True)
            df.drop('Arrival_Time', axis=1, inplace=True)

            df["Journey_day"] = pd.to_datetime(df.Date_of_Journey, format="%d/%m/%Y").dt.day
            df["Journey_month"] = pd.to_datetime(df["Date_of_Journey"], format="%d/%m/%Y").dt.month
            df.drop('Date_of_Journey', axis=1, inplace=True)

            self.log.info('Successfully completed method date_time_var')
            return df

        except Exception as e:
            self.log.warning('Exception occurred inside method date_time_var {}'.format(e))
            shutil.copy(self.log_file_path, self.clients_path + '/' + 'logfile.txt')
            raise Exception

    def handle_duration_col(self, df):
        """
            Method Name:handling_duration_col
            Description:Handles column duration
            Input:dataframe
            Output: dataframe
            On Failure: Raise Exception
        """
        try:
            duration = list(df["Duration"])

            for i in range(len(duration)):
                if len(duration[i].split()) != 2:  # Check if duration contains only hour or mins
                    if "h" in duration[i]:
                        duration[i] = duration[i].strip() + " 0m"  # Adds 0 minute
                    else:
                        duration[i] = "0h " + duration[i]  # Adds 0 hour

            duration_hours = []
            duration_mins = []
            for i in range(len(duration)):
                duration_hours.append(int(duration[i].split(sep="h")[0]))  # Extract hours from duration
                duration_mins.append(
                    int(duration[i].split(sep="m")[0].split()[-1]))  # Extracts only minutes from duration

            df["Duration_hours"] = duration_hours
            df["Duration_mins"] = duration_mins
            df.drop(["Duration"], axis=1, inplace=True)
            df['Duration_hours'] = df['Duration_hours'] + round(df['Duration_mins'] / 60, 1)
            df.drop('Duration_mins', axis=1, inplace=True)

            self.log.info('Successfully completed handle_duration_col')
            return df

        except Exception as e:
            self.log.warning('Exception occurred inside handle_duration_col {}'.format(e))
            shutil.copy(self.log_file_path, self.clients_path + '/' + 'logfile.txt')
            raise Exception

    def start_preprocessing_for_training(self):
        """
            Method Name:start_preprocessing_for_training
            Description:This is from where the methods will be called from
            Input:Null
            Output: Null
            On Failure: Raise Exception
        """
        try:

            df = pd.read_csv(self.path)
            df.drop(['Route', 'Id'], axis=1, inplace=True)

            # I am dropping the null values rows
            df.dropna(inplace=True)

            # I hope u are getting this why we have done this  because we have Delhi and New Delhi in our data set
            # which are same
            df['Source'] = np.where(df['Source'] == 'Delhi', 'New Delhi', df['Source'])
            df['Destination'] = np.where(df['Destination'] == 'Delhi', 'New Delhi', df['Destination'])

            df['Additional_Info'] = np.where(df['Additional_Info'] == 'No info', 'No Info', df['Additional_Info'])
            # df.to_csv('Extra_Training.csv', index=True)

            df, all_col_for_test = self.handling_categorical_var_for_training_data(df)
            pickle.dump(all_col_for_test, open("handle_categorical_col_for_testing.pickle", 'wb'))

            df = self.date_time_var(df)
            df = self.handle_duration_col(df)

            if not os.path.isdir('Preprocessed_data'):
                os.makedirs('Preprocessed_data')

            df.to_csv('Preprocessed_data/training_preprocessed_data.csv', index=False)

            self.log.info('Successfully executed start_preprocessing_for_training')
            self.log.info('Exiting from class <<<<<< dataPreprocessing')
            return 'Preprocessed_data/training_preprocessed_data.csv'

        except Exception as e:
            self.log.warning('Exception occurred during start_preprocessing_for_training {}'.format(e))
            shutil.copy(self.log_file_path, self.clients_path + '/' + 'logfile.txt')
            raise Exception

    def handling_categorical_var_for_testing_data(self, df):
        """
            Method Name:handling_categorical_var_for_training_data
            Description:Handles categorical variable
            Input:dataframe
            Output: dataframe
            On Failure: Raise Exception
        """

        try:
            main_thing = pickle.load(open('handle_categorical_col_for_testing.pickle', 'rb'))
            columns = ['Airline', 'Source', 'Destination', 'Total_Stops', 'Additional_Info']
            for col in columns:
                df[col] = df[col].map(main_thing[col])

            self.log.info("Method handling_categorical_var_for_testing_data completed successfully")
            return df

        except Exception as e:
            self.log.warning(
                'Exception occurred inside method handling_categorical_start_preprocessing_for_testing_data, {}'.format(
                    e))
            shutil.copy(self.log_file_path, self.clients_path + '/' + 'logfile.txt')
            raise Exception

    def start_preprocessing_for_testing(self):
        """
            Method Name:start_preprocessing_for_testing
            Description:This is from where the methods will be called from
            Input:Null
            Output: Null
            On Failure: Raise Exception
        """
        try:
            for file in os.listdir(self.path):
                self.log.info('Starting start_preprocessing_for_testing for {}'.format(file))

                df = pd.read_csv(self.path + '/' + file)
                df.drop(['Route'], axis=1, inplace=True)

                # I am dropping the null values rows
                df.dropna(inplace=True)

                # I hope u are getting this why we have done this  because we have Delhi and New Delhi in our data set
                # which are same
                df['Source'] = np.where(df['Source'] == 'Delhi', 'New Delhi', df['Source'])
                df['Destination'] = np.where(df['Destination'] == 'Delhi', 'New Delhi', df['Destination'])

                df['Additional_Info'] = np.where(df['Additional_Info'] == 'No info', 'No Info', df['Additional_Info'])
                # df.to_csv('Extra_Testing.csv', index=True)

                df = self.handling_categorical_var_for_testing_data(df)
                df = self.date_time_var(df)
                df = self.handle_duration_col(df)

                if not os.path.isdir('Preprocessed_data'):
                    os.makedirs('Preprocessed_data')

                filename = 'preprocessed_data--' + file

                df.to_csv('Preprocessed_data/' + filename, index=False)

                self.log.info('Successfully executed start_preprocessing_for_testing for {}'.format(file))
                self.log.info('Exiting from class <<<<<< dataPreprocessing for {}'.format(file))
            return 'Preprocessed_data/'

        except Exception as e:
            self.log.warning('Exception occurred during start_preprocessing_for_testing {}'.format(e))
            shutil.copy(self.log_file_path, self.clients_path + '/' + 'logfile.txt')
            raise Exception
