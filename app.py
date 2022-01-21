import numpy
from flask import Flask, request, render_template
from flask import Response
import os
from flask_cors import CORS, cross_origin
import json
import shutil
from wsgiref import simple_server

from Raw_Data_Validation.Raw_Data_Validation_Training import Raw_Data_Validation_Training
from Raw_Data_Validation.Raw_Data_Validation_Testing import Raw_Data_Validation_Testing
import pandas as pd
import datetime
import pickle
import logging
from Data_Preprocessing.data_preprocessing import dataPreprocessing
from Files_For_Prediction.prediction import predict
from Cassandra_DB_Folder.General_DB_func import All_DB_func
from Model_Training.model_training import trainModel
import Model_for_prediction

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)

CORS(app)


@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template('home.html')


# @app.route("/predict", methods=['POST'])
# @cross_origin()
# def predictRouteClient():
#     try:
#         logging.basicConfig(filename='All_logs/Testing_logs.log',
#                             filemode='w',
#                             level=logging.INFO,
#                             format='%(asctime)s %(levelname)s %(module)s---- %(message)s',
#                             datefmt='%Y-%m-%d %H:%M:%S')
#         logger = logging.getLogger('')
#         f = open('All_logs/Testing_logs.log', 'w')
#         f.truncate()
#         f.close()
#
#         if request.form is not None:
#
#             clients_path = request.form['filepath']
#             if os.path.isdir(clients_path + '/Output_Files/'):
#                 shutil.rmtree(clients_path + '/Output_Files')
#                 logger.info('Deleted Existing Output_Files In path specified')
#
#             # path is just a folder name in which raw data are kept
#
#             obj = Raw_Data_Validation_Testing(clients_path + '/', 'Testing_Schema.json', logger, f.name)
#             G_path = obj.start_validation()  # G_path is Good Data folder path
#
#             obj = dataPreprocessing(logger, G_path, f.name, clients_path)
#             preprocessed_data_path = obj.start_preprocessing_for_testing()
#
#             obj = predict(preprocessed_data_path, logger, G_path, f.name, clients_path)
#             json_predictions = obj.start_prediction()
#             shutil.copy(f.name, clients_path + '/' + 'logfile.txt')
#
#             return Response(
#                 "Prediction File created at !!!" + str(clients_path) + ' and few of the predictions are ' + str(
#                     json.loads(json_predictions)))
#         else:
#             print('Nothing Matched')
#     except ValueError:
#         return Response("Error Occurred! %s" % ValueError)
#     except KeyError:
#         return Response("Error Occurred! %s" % KeyError)
#     except Exception as e:
#         return Response("Error Occurred! %s" % e)
@app.route("/predict", methods=['GET','POST'])
@cross_origin()
def predict():
    try:
        if request.method == "POST":
            pred_list = []
            airline = request.form["Airline"]
            if airline == "Trujet":
                pred_list.append(1)
            elif airline =="Vistara Premium economy":
                pred_list.append(2)
            elif airline =="Air Asia":
                pred_list.append(3)
            elif airline =="Multiple carriers Premium economy":
                pred_list.append(4)
            elif airline =="Vistara":
                pred_list.append(5)
            elif airline =="IndiGo":
                pred_list.append(6)
            elif airline =="GoAir":
                pred_list.append(7)
            elif airline =="SpiceJet":
                pred_list.append(8)
            elif airline =="Air India":
                pred_list.append(9)
            elif airline =="Multiple carriers":
                pred_list.append(10)
            elif airline =="Jet Airways":
                pred_list.append(11)
            elif airline =="Jet Airways Business":
                pred_list.append(12)

            # source
            source = request.form["Source"]
            if source == "Chennai":
                pred_list.append(1)
            elif source == "Mumbai":
                pred_list.append(2)
            elif source == "Kolkata":
                pred_list.append(3)
            elif source == "Delhi":
                pred_list.append(4)
            elif source == "Banglore":
                pred_list.append(5)

            destination = request.form["Destination"]
            if destination == "Kolkata":
                pred_list.append(1)
            elif destination == "Hyderabad":
                pred_list.append(2)
            elif destination == "Banglore":
                pred_list.append(3)
            elif destination == "Cochin":
                pred_list.append(4)
            elif destination == "Delhi":
                pred_list.append(5)

            # Total Stops
            stops = int(request.form["Total_stops"])
            pred_list.append(stops)

            # Additional Info
            additional_info = int(request.form["Additional_Info"])
            pred_list.append(additional_info)

            # Date of Journey
            date_dep = request.form["Dep_Time"]
            print("Departure Date: ", date_dep)

            day_of_departure = int(pd.to_datetime(date_dep, format="%Y-%m-%dT%H:%M").day)
            month_of_departure = int(pd.to_datetime(date_dep, format="%Y-%m-%dT%H:%M").month)
            print(day_of_departure, month_of_departure)

            dep_hour = int(pd.to_datetime(date_dep, format="%Y-%m-%dT%H:%M").hour)
            dep_min = int(pd.to_datetime(date_dep, format="%Y-%m-%dT%H:%M").minute)
            print(dep_hour, dep_min)
            pred_list.append(dep_hour)
            pred_list.append(day_of_departure)
            pred_list.append(month_of_departure)


            # print(dep_hour, dep_min)

            date_arrival = request.form["Arrival_Time"]
            print("Arrival Date: ", date_arrival)
            day_of_arrival = int(pd.to_datetime(date_arrival, format="%Y-%m-%dT%H:%M").day)
            month_of_arrival = int(pd.to_datetime(date_arrival, format="%Y-%m-%dT%H:%M").month)
            print(day_of_arrival, month_of_arrival)

            arrival_hour = int(pd.to_datetime(date_arrival, format="%Y-%m-%dT%H:%M").hour)
            arrival_min = int(pd.to_datetime(date_arrival, format="%Y-%m-%dT%H:%M") .minute)
            print(arrival_hour, arrival_min)

            dep_journey = datetime.datetime.fromisoformat(date_dep)
            arr_journey = datetime.datetime.fromisoformat(date_arrival)
            print(dep_journey, arr_journey)

            duration = arr_journey - dep_journey
            duration_in_seconds = duration.total_seconds()
            journey_in_minutes = divmod(duration_in_seconds, 60)[0]
            journey_hours = journey_in_minutes // 60
            journey_minutes = journey_in_minutes % 60
            pred_list.append(journey_hours)
            # print("Journey Hours: {} Journey Minutes: {}".format(journey_hours, journey_minutes))
            print(f"Pred List : {pred_list}")
            model = pickle.load(open("Model_for_prediction\model.pickle","rb"))
            pred_list = numpy.array(pred_list).reshape((1,-1))
            pred_price = model.predict(pred_list)
            print(f"Price is : {pred_price}")
            return render_template('home.html', prediction_text="Your Flight price is Rs. {}".format(round(pred_price[0],2)))
    except Exception as e:
        return f"Exception occurred {e}"


@app.route("/train", methods=['POST'])
@cross_origin()
def trainRouteClient():
    try:
        logging.basicConfig(filename='All_logs/Training_logs.log',
                            filemode='w',
                            level=logging.INFO,
                            format='%(asctime)s %(levelname)s ---- %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')
        logger = logging.getLogger('')

        f = open('All_logs/Training_logs.log', 'w')
        f.truncate()
        f.close()

        if request.json['folderPath'] is not None:
            path = request.json['folderPath']
            # path is just a folder name in which raw data are kept

            obj = Raw_Data_Validation_Training(path + '/', 'Training_Schema.json', logger, f.name)
            G_path = obj.start_validation()

            # Now our files are moved to Good Data and Bad Data folders
            # Its time for uploading data to cloud storage

            obj = All_DB_func(logger, G_path, path, f.name)
            train_path = obj.start_db()

            obj = dataPreprocessing(logger, train_path, f.name, path)
            preprocessed_data_path = obj.start_preprocessing_for_training()

            obj = trainModel(logger, preprocessed_data_path, f.name, path)
            obj.start_training()
            shutil.copy(f.name, path + '/' + 'logfile.txt')

            if os.path.isdir(path + '/' + 'Models_plot'):
                shutil.rmtree(path + '/' + 'Models_plot/')
                os.makedirs(path + '/' + 'Models_plot/')
            else:
                os.makedirs(path + '/' + 'Models_plot/')

            for plot in os.listdir('Models_plot/'):
                shutil.copy('Models_plot' + '/' + plot, path + '/Models_plot/')

    except ValueError:

        return Response("Error Occurred! %s" % ValueError)

    except KeyError:

        return Response("Error Occurred! %s" % KeyError)

    except Exception as e:

        return Response("Error Occurred! %s" % e)
    return Response("Training successful!!")



if __name__ == "__main__":
    port = int(os.getenv("PORT"))
    #clApp = ClientApp()
    host = '0.0.0.0'
    httpd = simple_server.make_server(host=host,port=port, app=app)
    httpd.serve_forever()