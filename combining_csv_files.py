import csv
import os
import pandas as pd


class csvFiles:
    def __init__(self, test_data_path, logger):
        """
            This class will be used for combining multiple csv files into one file
        """

        self.path = test_data_path
        self.log = logger
        self.log.info('Entered into class >>>>>> csvFiles')

    def start_combining(self):
        """
            Method Name:start_combining
            Description:This will simply copy each file content into a single file that is Testing_file.csv
            Input:Nothing
            Output: Testing_file.csv path
            On Failure: Raise Exception
        """
        try:

            filename = open('Testing_file.csv', 'w')
            filename.truncate()
            filename = open('Testing_file.csv', 'a')
            csv_writer = csv.writer(filename)
            csv_writer.writerow(tuple(['Airline', 'Date_of_Journey', 'Source', 'Destination', 'Route',
                                       'Dep_Time', 'Arrival_Time', 'Duration', 'Total_Stops',
                                       'Additional_Info']))

            for file in os.listdir(self.path):
                # We are going to delete the null rows
                df = pd.read_csv(self.path + '/' + file)
                df.dropna(inplace=True)

                f = open(self.path + file, 'r')

                csvreader = csv.reader(f)
                next(csvreader)
                for row in csvreader:
                    csv_writer.writerow(tuple(row))
                f.close()

            self.log.info('Successfully completed method start_combining')
            self.log.info('Exiting from class <<<<<< csvFiles')
            return 'Testing_file.csv'

        except Exception as e:
            self.log.warning('Exception occurred inside method start_combining, {}'.format(e))
            raise Exception
