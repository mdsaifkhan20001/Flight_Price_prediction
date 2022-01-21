from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv
import os
import shutil

class All_DB_func:
    """
        This class shall be used for importing the data and exporting
    """

    def __init__(self, logger, g_path, clients_path, log_file_path):
        try:
            self.log = logger
            self.G_path = g_path
            self.log_file_path = log_file_path
            # cloud_config = {
            #     'secure_connect_bundle': './secure-connect-flight-fare-master.zip'
            # }
            # auth_provider = PlainTextAuthProvider('bTiAqRLnWXBnromkirhAZgEf','gTH.dBpX++UfrT-2Lpw++Zgz0d1zPbA2LzyHnZskSQ1HfBwFs7C2.''uy6b2EnIzPMZHf5Q33nKN.mZOZ+2-Kni_q-Ttf0y1ZF2P+ncdGMtzSdQKeKWF''atvKpyc,KMlrvY')
            # cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            # self.session = cluster.connect()
            #
            # self.session.execute(""" CREATE TABLE if not exists training.all_train_files(
            #         id INT PRIMARY KEY,Airline text,Date_of_Journey text,Source text,Destination text,
            #         Route text,Dep_Time text,Arrival_Time text,Duration text,Total_Stops text,
            #         Additional_Info text,Price INT);""")
            #
            # self.session.execute('use training;')
            self.log.info('Entered into class >>>>>> All_DB_func')

        except Exception as e:
            self.log.warning('Exception occurred during class initialization {}'.format(e))
            shutil.copy(self.log_file_path, self.clients_path + '/' + 'logfile.txt')
            raise Exception

    def uploading_data(self):
        """
            Method Name: uploading_data
            Description: Uploads the data to the Cassandra database.
            Output: Nothing
            On Failure: Raise Exception
        """
        try:
            # G_path is good Data Folder
            self.log.info('Data Uploading Started')
            # for file in os.listdir(self.G_path):
            #     csv_reader = csv.reader(open(self.G_path + file))
            #     next(csv_reader)
            #
            #
            #     for rows in csv_reader:
            #         query = """INSERT INTO training.all_train_files (
            #         id,Airline ,Date_of_Journey,Source ,Destination ,
            #         Route ,Dep_Time ,Arrival_Time ,Duration ,Total_Stops ,Additional_Info,Price)
            #         VALUES (%d,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',%d);""" % (
            #             int(rows[0]), str(rows[1]), str(rows[2]), str(rows[3]), str(rows[4]), str(rows[5]),
            #             str(rows[6]),
            #             str(rows[7]), str(rows[8]), str(rows[9]), str(rows[10]), int(rows[11]))
            #         self.session.execute(query)
            #     self.log.info('{} Uploaded Successfully'.format(file))
            self.log.info("Uploading_data to Cassandra Database Completed")

        except Exception as e:
            self.log.warning('Exception occurred during uploading_data {} '.format(e))
            shutil.copy(self.log_file_path, self.clients_path + '/' + 'logfile.txt')

            raise Exception

    def extracting_data(self):
        """
            Method Name: extracting_data
            Description: Stores the data to the local machine from Database
            Output: Returns Filename for Training_data
            On Failure: Raise Exception
        """
        try:
            #
            main_train_data_path = 'Training_file.csv'
            # f = open(main_train_data_path, 'w', encoding='utf8')
            # f.truncate()
            # writer = csv.writer(f)
            #
            # # Writing the column names
            # writer.writerow(tuple(['Id', 'Airline', 'Date_of_Journey', 'Source', 'Destination', 'Route',
            #                        'Dep_Time', 'Arrival_Time', 'Duration', 'Total_Stops',
            #                        'Additional_Info', 'Price']))
            # for val in self.session.execute('select * from training.all_train_files;'):
            #     row = [
            #         val.id, val.airline, val.date_of_journey, val.source, val.destination, val.route,
            #                 val.dep_time, val.arrival_time, val.duration, val.total_stops, val.additional_info,
            #                 val.price]
            #     # write each row to the csv file
            #     writer.writerow(tuple(row))
            #
            # f.close()
            self.log.info('Retrieving data from DB Completed and stored in {}'.format(main_train_data_path))
            return main_train_data_path

        except Exception as e:
            self.log.warning('Exception occurred during retrieving from DB {}'.format(e))
            shutil.copy(self.log_file_path, self.clients_path + '/' + 'logfile.txt')

            raise Exception

    def start_db(self):
        self.uploading_data()
        train_path = self.extracting_data()
        return train_path
