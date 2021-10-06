import os
import boto3
import datetime
import pytesseract as pt
import logging
import logging.handlers

from mysql.connector import connect, Error, errors
from io import BytesIO
from PIL import Image
from pathlib import Path
from mysql.connector import errorcode

log_file_path = f'{os.getcwd()}/logs/'
Path(log_file_path).mkdir(parents=True, exist_ok=True)
log_file = f'{log_file_path}{datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}'
handler = logging.handlers.WatchedFileHandler(
    os.environ.get("LOGFILE", log_file))
formatter = logging.Formatter(logging.BASIC_FORMAT)
handler.setFormatter(formatter)
root = logging.getLogger()
root.setLevel(os.environ.get("LOGLEVEL", "INFO"))
root.addHandler(handler)

# variables
bucket_name = 'instacaffeteria01'
aws_access_key_id = 'AKIA5JWEI5SGQTWJEPHG'
aws_secret_access_key = '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'
region_name = 'us-east-2'

mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = ''
mysql_database = 'instabase_cafeteria'

# Creating the low level functional client
client = boto3.client(
    's3',
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key,
    region_name = region_name
)

s3 = boto3.resource(
    's3',
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key,
    region_name = region_name
)

bucket1 = s3.Bucket(bucket_name)

'''
Instabase Cafeteria
Check #: 0100 7/21/2021
Server: Jeff 2:41 PM
Table: 8 Guests: 1
1 Insta Burrito 6.40
1 Insta Onion O's 2.90
1 Sprite 1.50
Sub-total 10.80
Sales TAX 0.78
Tips 1.62
Total 13.20
Come Back Soon!
'''

def check_float(potential_float):
    try:
        float(potential_float)
        return True
    except ValueError:
        return False

def check_int(potential_int):
    try:
        int(potential_int)
        return True
    except ValueError:
        return False


def parse_text(key, text):

    try:
        file_path = f'{os.getcwd()}/unstructured_checks_text/'
        Path(file_path).mkdir(parents=True, exist_ok=True)
        check_text = f'{file_path}{key}.txt'
        with open(check_text, 'w+') as f:
            f.write(text)
            f.close
    except FileNotFoundError as e:
        logging.error(e)

    check_dict = {}
    # Split the string into individual rows and cleanup
    rows = text.split("\n")
    rows = [row.strip() for row in rows if not row.isspace() and row]
    try:
        for row in rows:
            items = row.split(" ")
            if items[0] == 'Tips':
                if len(items) < 2:
                    logging.info(f'Check - {key} has invalid entry for tips - {row}. Setting tips as - $0')
                    check_dict['tips'] = '0.0'
                    continue
                if check_float(items[1]):
                    check_dict['tips'] = items[1]
                else:
                    logging.info(f'Check - {key} has invalid entry for tips - {items[1]}. Setting tips as - $0')
                    check_dict['tips'] = '0.0'
            if items[0] == 'Total':
                if len(items) < 2:
                    logging.info(f'Check - {key} has invalid entry for total - {row}. Setting total as - $0')
                    check_dict['total'] = '0.0'
                    continue
                if check_float(items[1]):
                    check_dict['total'] = items[1]
                else:
                    logging.info(f'Check - {key} has invalid entry for total - {items[1]}. Setting it as - $0')
                    check_dict['total'] = '0.0'
            if items[0] == 'Server:':
                if len(items) < 4:
                    logging.info(f'Check - {key} has invalid entry for Server or Time - {row}. setting time as 10:10 AM')
                    in_time = datetime.datetime.strptime("10:10 AM", "%I:%M %p")
                    out_time = datetime.datetime.strftime(in_time, "%H:%M")
                    if len(items) < 2:
                        logging.info(f'Check - {key} has invalid entry for Server or Time - {row}. setting server name as - Unknown')
                        check_dict['server'] = 'Unknown'
                    else:
                        check_dict['server'] = items[1]
                    continue
                if len(items) < 2:
                    logging.info(f'Check - {key} has invalid entry for Server or Time - {row}. setting server name as - Unknown')
                    check_dict['server'] = 'Unknown'
                else:
                    check_dict['server'] = items[1]
                hours = str(items[2]).split(":")
                if int(hours[1]) > 59:
                    time = hours[0] + ":30 " + items[3]
                else:
                    time = items[2] + " " + items[3]
                in_time = datetime.datetime.strptime(time, "%I:%M %p")
                out_time = datetime.datetime.strftime(in_time, "%H:%M")
            if items[0] == 'Check':
                if len(items) < 4:
                    logging.info(f'Check - {key} has invalid entry for date or check number - {row}. setting date as 10/03/2021 and check number to 0')
                    check_date = datetime.datetime.strptime('10/03/2021', "%m/%d/%Y").strftime("%Y-%m-%d")
                    check_dict['check_number'] = items[2]
                    continue
                check_date = datetime.datetime.strptime(items[3], "%m/%d/%Y").strftime("%Y-%m-%d")
                if check_int(items[2]):
                    check_dict['check_number'] = items[2]
                else:
                    logging.info(f'Check - {key} has invalid entry for check number - {items[2]}. Setting check number as - 0')
                    check_dict['check_number'] = 0

            if items[0] == 'Table:':
                if len(items) < 4:
                    logging.info(f'Check - {key} has invalid entry for table number or guests - {row}. setting those to 0')
                    check_dict['table_number'] = 0
                    check_dict['guests'] = 0
                    continue
                if check_int(items[1]):
                    check_dict['table_number'] = int(items[1])
                else:
                    logging.info(f'Check - {key} has invalid entry for table number - {items[2]}. Setting table number as - 0')
                    check_dict['table_number'] = 0
                if check_int(items[3]):
                    check_dict['guests'] = int(items[3])
                else:
                    logging.info(f'Check - {key} has invalid entry for guests - {items[3]}. Setting guests as - 0')
                    check_dict['guests'] = 0
            if items[0] == 'Sub-total':
                if len(items) < 2:
                    logging.info(f'Check - {key} has invalid entry for sub total - {row}. Setting sub total as - $0')
                    check_dict['sub_total'] = '0.0'
                    continue
                if check_float(items[1]):
                    check_dict['sub_total'] = items[1]
                else:
                    logging.info(f'Check - {key} has invalid entry for sub total - {items[1]}. Setting it as - $0')
                    check_dict['sub_total'] = '0.0'
            if items[0] == 'Sales':
                if len(items) < 3:
                    logging.info(f'Check - {key} has invalid entry for sales tax - {row}. Setting sales tax as - $0')
                    check_dict['sales_tax'] = '0.0'
                    continue
                if check_float(items[2]):
                    check_dict['sales_tax'] = items[2]
                else:
                    logging.info(f'Check - {key} has invalid entry for sales tax - {items[2]}. Setting it as - $0')
                    check_dict['sales_tax'] = '0.0'

    except Error as e:
        logging.error(e)

    date_time = datetime.datetime.strptime(check_date + " " + out_time, "%Y-%m-%d %H:%M")
    date_time = datetime.datetime.strftime(date_time, "%Y-%m-%d %H:%M")
    check_dict['date_time'] = date_time

    return check_dict


def insert_check(key, check_dict):
    try:
        with connect(
                host= mysql_host,
                user= mysql_user,
                password= mysql_password,
                database= mysql_database,
        ) as connection:
            with connection.cursor() as cursor:
                insert_query = f"INSERT INTO checks (check_name, check_number, server, table_number, guests, sub_total, sales_tax, date_time, tips, total) " \
                               f"values('{key}', '{check_dict['check_number']}', '{check_dict['server']}', '{check_dict['table_number']}', '{check_dict['guests']}', '{check_dict['sub_total']}', '{check_dict['sales_tax']}', '{check_dict['date_time']}', {check_dict['tips']}, {check_dict['total']})"
                cursor.execute(insert_query)
                connection.commit()
                cursor.close()
                connection.close()
    except errors.IntegrityError as e:
        logging.error(e)
        print(e)
    except Error as e:
        logging.error(e)
        print(e)

for obj in bucket1.objects.all():
        file_byte_string = client.get_object(Bucket=obj.bucket_name, Key=obj.key)['Body'].read()
        key = str(obj.key).split(".")[0]
        logging.info(key)
        print(key)
        try:
            file_path = f'{os.getcwd()}/'
            Path(file_path).mkdir(parents=True, exist_ok=True)
            check_file_names = f'{file_path}check_file_names-{datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}.txt'
            with open(check_file_names, 'w+') as f:
                f.write(key + "\n")
                f.close
        except FileNotFoundError as e:
            logging.error(e)
        img = Image.open(BytesIO(file_byte_string))
        check_dict = parse_text(key, pt.image_to_string(img))
        insert_check(key, check_dict)
