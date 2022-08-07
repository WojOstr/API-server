import flask
from flask import request, jsonify
import csv
from datetime import datetime, timezone
from apscheduler.schedulers.background import BackgroundScheduler
import os
from minio import Minio

app = flask.Flask(__name__)
app.config["DEBUG"] = True
app.config['JSON_SORT_KEYS'] = False
output_path = 'processed_data/output.csv'
access_key = 'admin'
secret_key = 'password'
last_data = []



def clear_duplicate_users(users: list)-> list:
    """
    Delete duplicates from users list

    Parameters
    ----------
    users
        nested list consisitng users
    
    Return
    ------
    list
        nested list consisting users without duplicates
    """
    return [user for user in users if users.count(user) == 1]

def check_for_files(path: str)-> list:
    """
    Check names of files in directory path

    Parameters
    ----------
    path
        directory path
    
    Return
    ------
    list
        list with files names
    """
    l = os.listdir(path)
    names = [x.split('.')[0] for x in l]
    return names

def check_csv_files(path: str)-> list:
    """
    Check for all files in directory with csv extension

    Parameters
    ----------
    path
        directory path
    
    Return
    ------
    list
        list with csv file names from directory
    """
    csv_files = [file for file in os.listdir(path) if file.endswith('.csv')]
    return csv_files

def open_csv(path: str, csv_files: list, file_names: list) -> list:
    """
    Open csv files and create dataset of users

    Parameters
    ----------
    path
        directory path
    csv_files
        list with csv file names from directory
    file_names
        list with files names
    
    Return
    ------
    list
        nested list of all users from csv files
    """
    temp_data = []
    for idx, filename in enumerate(csv_files):
        if file_names.count(filename.rstrip('.csv')) == 2:
            with open(f'{path}/{filename}', newline='', encoding="utf-8") as file_open:
                next(file_open)
                for row in file_open:
                    first_name, last_name, birthts = row.split()
                    temp_data.append([
                        idx,
                        first_name.replace(',', ''),
                        last_name.replace(',', ''),
                        birthts.replace(',', ''),
                        path+'/'+filename.replace('.csv', '.png')])
    return temp_data

def write_csv_to_file(data) -> None:
    """
    Writes data to csv file

    Parameters
    ----------
    data
        nested list of all users from csv files
    """
    with open('processed_data/output.csv', 'w', newline='', encoding="utf-8") as file_write:
        print(file_write)
        writer = csv.writer(file_write)
        header = ['user_id', 'first_name', 'last_name', 'birthts', 'img_path']
        writer.writerow(header)
        writer.writerows(clear_duplicate_users(data))

def minio_connect(host: str, a_key: str, s_key: str, sec: bool) -> Minio:
    """
    Creates connection object with Minio and returns it
    
    Parameters
    ----------
    host
        endpoint name (eg. 'localhost:9000')
    a_key
        access key (login)
    s_key
        security key (password)
    sec
        is connection secure
    
    Return
    ------
    Minio
        Minio type connection object
    """
    minio_connection = Minio(endpoint = host,
                        access_key = a_key, 
                        secret_key = s_key,
                        secure = sec)
    return minio_connection

def minio_upload_csv(minio_client: Minio, bucket: str, output_name: str, input_csv: str)-> None:
    """
    Upload csv to minio bucket

    Parameters
    ----------
    minio_client
        Minio type connection object
    bucket
        minio bucket name
    output_name
        file name in bucket
    input_csv
        input csv path
    """
    minio_client.fput_object(bucket,
                            output_name,
                            input_csv
                            )


def sensor(path = '02-src-data/')-> None:
    """
    Looped function wrapping function from main.py file, which read csv files, 
    process them  and save to minio bucket and output csv. This happens every 
    30 minutes or by calling endpoint /data with POST method

    Parameters
    ----------
    path 
        input directory path
    """
    if len(last_data) == 3:
        del last_data[0]
    pair_of_files = check_for_files(path)
    csv_files = check_csv_files(path)
    data = open_csv(path, csv_files, pair_of_files)
    if len(last_data) == 2 and last_data[0] != last_data[1]:
        write_csv_to_file(data)
        minio_conn = minio_connect('localhost:9000', access_key, secret_key, False)
        minio_upload_csv(minio_conn, 'datalake', 'output.csv', 'processed_data/output.csv')
    last_data.append(data)

sched = BackgroundScheduler(daemon=True)
sched.add_job(sensor,'interval',minutes=30)
sched.start()

def open_output_csv(path:str)-> list:
    """
    Reads csv file with DictReader method and returns list of dicts

    Parameters
    ----------
    path 
        input path file

    Return
    ------
    list
        list of dictonaries with every containing each user
    """

    with open(path, newline='', encoding="utf-8") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        return [row for row in csv_reader]

def calculate_age(time_in_millis:str)-> int:
    """
    Calculate age from milliseconds

    Parameters
    ----------
    time_in_millis 
        time in milliseconds with str type

    Return
    ------
    int
        time in years
    """

    born = datetime.fromtimestamp(int(time_in_millis) / 1000.0, tz=timezone.utc)
    today = datetime.now(timezone.utc)
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

def get_users(*params)->list:
    """
    Get users from csv file based on filters

    Parameters
    ----------
    *params
        input parameters, possible to resize
    
    Return
    ------
    list
        list of dictonaries with each containig user meeting criteria
    """
    json_csv = []
    image_exists, min_a, max_a = params
    if image_exists == 'False':
        img = False
    elif image_exists == 'True':
        img = True

    csv_file = open_output_csv(output_path)
    for row in csv_file:
        age = calculate_age(row['birthts'])
        if min_a <= age <= max_a:
            if  not img and not row['img_path'] or img == True and row['img_path'] != '':
                json_csv.append(row)
    return json_csv

def calculate_average_user_age(users:list)-> int:
    """
    Calculate average user age

    Parameters
    ----------
    users
        list of users
    
    Return
    ------
    int
        average user age from users list
    """
    all_years = [calculate_age(row['birthts']) for row in users]
    return sum(all_years) // len(all_years) if users is not None else 0

@app.route('/data', methods=['GET', 'POST'])
def api_get_or_post_data():
    if request.method == 'GET':
        is_image_exists = request.args.get('is_image_exists', default='True', type=str)
        min_age = request.args.get('min_age', default=0, type=int)
        max_age = request.args.get('max_age', default=100, type=int)
        js_csv = get_users(is_image_exists, min_age, max_age)
        return jsonify(js_csv) if len(js_csv) > 0 else 'No records found!'

    if request.method == 'POST':
        sensor()
        return 'src-data updated'
        

@app.route('/stats', methods=['GET'])
def api_get_stats():
    is_image_exists = request.args.get('is_image_exists', default='True', type=str)
    min_age = request.args.get('min_age', default=0, type=int)
    max_age = request.args.get('max_age', default=100, type=int)
    js_csv = get_users(is_image_exists, min_age, max_age)
    mean_age = calculate_average_user_age(js_csv)
    return jsonify(mean_age) if mean_age > 0 else 'No records for this filters found!'

app.run(host = 'localhost', port = 8080)

