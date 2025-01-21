####################################################################
#This code  is very intricate and  confidential and legal property.#
# Do not mess , annalyze  , and moddify without the permission     #
# By: sinnikhy(Nikhil Singh)                                       #
####################################################################
import schedule
import time
import threading
import os
import sys
import glob
from datetime import datetime
import requests
import pandas as pd
import pickle


def qsr_schedule():
    current_time = datetime.now()
    print(f"Running QSR at {current_time.strftime('%c')}")
    downloads_folder = os.path.expanduser('~/Downloads')
    expected_header_qsr = ['Queue', 'Count', 'Age']
    last_processed_file_path = os.path.expanduser('~/.last_processedqsr_file.pkl')
    csv_files = glob.glob(os.path.join(downloads_folder, '*.csv'))
    csv_files.sort(key=os.path.getmtime, reverse=True)
    latest_csv_files = csv_files[:20]
    last_processed_file = None
    if os.path.exists(last_processed_file_path):
        with open(last_processed_file_path, 'rb') as f:
            last_processed_file = pickle.load(f)
    latest_correct_file = None
    for file in latest_csv_files:
        try:
            df = pd.read_csv(file, nrows=1)
            header = list(df.columns)

            if header == expected_header_qsr:
                print(f"File for QSR with correct header found: {file}")
                latest_correct_file = file
                break
        except Exception as e:
            print(f"Error reading {file}: {e}")

    if latest_correct_file == last_processed_file:
        print(f"The latest file with correct header is the same as the last processed one. Running fallback...")
        alert_qsradh()
    elif latest_correct_file:
        with open(last_processed_file_path, 'wb') as f:
            pickle.dump(latest_correct_file, f)
        print(f"Processed new correct file: {latest_correct_file}")
        # here i will process  the new  csv
    else:
        print("No CSV file matched the expected header. Running fallback function...")
        alert_qsradh()

def adh_schedule():
    current_time = datetime.now()
    print(f"Running Adherence at {current_time.strftime('%c')}")
    downloads_folder = os.path.expanduser('~/Downloads')
    expected_header_adh = ['Queue', 'Count', 'Age']
    last_processed_file_path = os.path.expanduser('~/.last_processedadh_file.pkl')
    csv_files = glob.glob(os.path.join(downloads_folder, '*.csv'))
    csv_files.sort(key=os.path.getmtime, reverse=True)
    latest_csv_files = csv_files[:20]
    last_processed_file = None
    if os.path.exists(last_processed_file_path):
        with open(last_processed_file_path, 'rb') as f:
            last_processed_file = pickle.load(f)
    latest_correct_file = None
    for file in latest_csv_files:
        try:
            df = pd.read_csv(file, nrows=1)
            header = list(df.columns)
            if header == expected_header_adh:
                # print(f"File for Adherence with correct header found: {file}")
                latest_correct_file = file
                break
        except Exception as e:
            print(f"Error reading {file}: {e}")

    if latest_correct_file == last_processed_file:
        print(f"The latest file with correct header is the same as the last processed one. Running fallback...")
        alert_qsradh()
    elif latest_correct_file:
        with open(last_processed_file_path, 'wb') as f:
            pickle.dump(latest_correct_file, f)
        print(f"Processe new correct file: {latest_correct_file}")
        # here i will process  the new  csv
    else:
        print("No CSV file matched the expected header. Running fallback function...")
        alert_qsradh()


schedule.every(1).minutes.do(adh_schedule)
schedule.every(1).minutes.do(qsr_schedule)
# schedule.every().hour.at(":00").do(function2)


def run_scheduled_jobs():
    while True:
        schedule.run_pending()
        time.sleep(1)

def start_background_task():
    thread = threading.Thread(target=run_scheduled_jobs)
    thread.daemon = True 
    thread.start()

def automation_start():
    webhook_url = 'https://hooks.chime.aws/incomingwebhooks/5f87f4ef-11a1-42f5-80ac-beac0586c07c?token=eTVlV2NWUkZ8MXw2c3BBeEFzaGxCMk5PNl93OThWLXZKVEk1TkNlcnZteEc0YWl0R2l2M0VN'
    message = f"/md```QSR and Adherence is now Started by {os.getlogin()}```"
    payload = {"Content": message}
    response = requests.post(webhook_url, json=payload)
    if response.status_code == 200:
        print("Alert Generated for QSR run")
    pass 
def alert_qsradh():
    webhook_url = 'https://hooks.chime.aws/incomingwebhooks/5f87f4ef-11a1-42f5-80ac-beac0586c07c?token=eTVlV2NWUkZ8MXw2c3BBeEFzaGxCMk5PNl93OThWLXZKVEk1TkNlcnZteEc0YWl0R2l2M0VN'
    message = f"/md```QSR and Adherence Stopped {os.getlogin()}```"
    payload = {"Content": message}
    response = requests.post(webhook_url, json=payload)
    if response.status_code == 200:
        print("Alert sent for QSR run")
        sys.exit()
    pass


if __name__ == "__main__":
    automation_start()
    print(f"You Started Automation for QSR and Aherence")
    start_background_task()    
    try:
        while True:
            time.sleep(1)  
    except KeyboardInterrupt:
        print("Program interrupted. Exiting...")

