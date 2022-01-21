import json
import os
import pandas as pd
import shutil


class Raw_Data_Validation_Testing:
    """
        This class shall be used for Raw_Data_Validation like
        checking_schema and data type of the columns and after
        that moving the file to Good_Data or Bad_Data. folders
    """

    def __init__(self, raw_data_path, schema_path, logger, log_file_path):
        try:

            self.log = logger
            self.raw_data_path = raw_data_path
            self.schema_path = schema_path
            self.log_file_path = log_file_path

            self.log.info('Entered into class >>>>>> Raw_Data_Validation_Testing')
        except Exception as e:
            self.log.warning('Exception occurred during class initialization {}'.format(e))
            shutil.copy(self.log_file_path, self.raw_data_path + '/' + 'logfile.txt')
            raise Exception

    def checking_schema(self, schema_dtypes, schema_columns):
        """
            Method Name: checking_schema
            Description: Gets the data types according to the schema
            Output: copies raw data to Good_Data folder or bad_data folder
            On Failure: Raise Exception
        """

        G_path = os.path.join('Testing_Batch_Files/Good_Data/')
        B_path = os.path.join('Testing_Batch_Files/Bad_Data/')

        try:

            if os.path.isdir(G_path):
                shutil.rmtree(G_path)
                self.log.info('Deleted existing {} folder'.format(G_path))

                os.makedirs(G_path)
                self.log.info('Created {} folder'.format(G_path))

            else:
                os.makedirs(G_path)
                self.log.info('Created {} folder'.format(G_path))

            # For Bad Data Folder
            #

            if os.path.isdir(B_path):
                shutil.rmtree(B_path)
                self.log.info('Deleted existing {} folder'.format(B_path))

                os.makedirs(B_path)
                self.log.info('Created {} folder'.format(B_path))

            else:
                os.makedirs(B_path)
                self.log.info('Created {} folder'.format(B_path))

            for file in os.listdir(self.raw_data_path):
                df = pd.read_csv(self.raw_data_path + file)
                file_columns = df.columns
                raw_cols = []
                for i in df.columns:
                    raw_cols.append(df[i].dtype)

                if raw_cols == schema_dtypes and list(file_columns) == schema_columns:
                    shutil.copy(self.raw_data_path + file, G_path)
                    self.log.info('Copied {} to Good_Data Folder'.format(file))

                else:
                    shutil.copy(self.raw_data_path + file, B_path)
                    self.log.info('Copied {} to Bad_Data Folder'.format(file))

            self.log.info('Exiting from class <<<<<< Raw_Data_Validation_Testing')
            return G_path

        except Exception as e:
            self.log.warning('Exception occurred while checking schema of the files {}'.format(e))
            shutil.copy(self.log_file_path, self.raw_data_path + '/' + 'logfile.txt')
            raise Exception

    def loading_schema(self):
        """
            Method Name:loading_schema
            Description: Load the schema from Training_schema.json file
            Output: data types of the columns
            On Failure: Raise Exception
        """

        try:
            f = open(self.schema_path)
            data = json.load(f)
            schema_dtypes = []
            for i in data:
                schema_dtypes.append(data[i])

            columns = list(data.keys())

            self.log.info('Loading schema from {} completed'.format(self.schema_path))
            return schema_dtypes, columns

        except Exception as e:
            self.log.warning('Exception occurred while loading schema {}'.format(e))
            shutil.copy(self.log_file_path, self.raw_data_path + '/' + 'logfile.txt')
            raise Exception

    def start_validation(self):
        schema_dtypes, columns = self.loading_schema()
        G_path = self.checking_schema(schema_dtypes, columns)
        return G_path
