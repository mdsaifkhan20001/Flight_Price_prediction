import pickle
import pandas as pd
import os
import shutil


class predict:
    def __init__(self, test_data_path, logger, g_path, log_file_path, clients_path):
        self.data_path = test_data_path
        self.log = logger
        self.clients_path = clients_path
        self.log_file_path = log_file_path

        self.G_path = g_path

        filename = 'Model_for_prediction/model' + '.pickle'
        self.model = pickle.load(open(filename, 'rb'))
        self.log.info('Entered into class >>>>>> predict')

    def predict_price(self, file):
        """
            Method Name:predict_price
            Description:This will predict price  for each file
            Input:df
            Output: dataframe
            On Failure: Raise Exception
        """
        try:

            df = pd.read_csv(self.data_path + 'preprocessed_data--' + file)
            real_df = pd.read_csv(self.G_path + file)

            real_df['Price'] = self.model.predict(df)
            self.log.info('Price column created and prices are predicted for {}'.format(file))

            filename = self.clients_path + '/Output_Files/' + 'Output_File_Of----' + file

            real_df.to_csv(filename, index=False, header=True)

        except Exception as e:
            self.log.warning(
                'Exception occurred inside method predict_price for file {} and error message {}'.format(file, e))
            shutil.copy(self.log_file_path, self.clients_path + '/' + 'logfile.txt')
            raise Exception

    def start_prediction(self):
        if not os.path.isdir(self.clients_path + '/Output_Files/'):
            os.makedirs(self.clients_path + '/Output_Files/')

        for file in os.listdir(self.G_path):
            self.predict_price(file)

        file = os.listdir(self.clients_path + '/Output_Files/')[0]
        df = pd.read_csv(self.clients_path + '/Output_Files/' + file)

        self.log.info('Exiting from class <<<<< predict')
        return df.head().to_json(orient='records')
