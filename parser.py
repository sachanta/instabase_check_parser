import boto3
import datetime
import pytesseract as pt
import logging
from getpass import getpass
from mysql.connector import connect, Error
from io import BytesIO
from PIL import Image

logging.basicConfig(filename='app.log', format='%(asctime)s - %(message)s')
# Creating the low level functional client
client = boto3.client(
    's3',
    aws_access_key_id = 'AKIA5JWEI5SGQTWJEPHG',
    aws_secret_access_key = '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',
    region_name = 'us-east-2'
)
s3 = boto3.resource("s3",
                  region_name='us-east-1',
                  aws_access_key_id='AKIA5JWEI5SGQTWJEPHG',
                  aws_secret_access_key='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')

bucket1 = s3.Bucket('instacaffeteria01')
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

def get_cursor():
    try:
        with connect(
                host="localhost",
                user=input("Enter username: "),
                password=getpass("Enter password: "),
        ) as connection:
            with connection.cursor() as cursor:
                return cursor
    except Error as e:
        print(e)
    return None


def parse_text(key, text):
    logging.info(text)
    check_dict = {}
    # Split the string into individual rows and cleanup
    # Write code to populate the list below
    rows = text.split("\n")
    rows = [row.strip() for row in rows if not row.isspace() and row]
    try:
        for row in rows:
            items = row.split(" ")
            if items[0] == 'Tips':
                if len(items) < 2:
                    print(f'Check - {key} has invalid entry for tips - {row}. Setting tips as - $0')
                    check_dict['tips'] = '0.0'
                    continue
                if check_float(items[1]):
                    check_dict['tips'] = items[1]
                else:
                    print(f'Check - {key} has invalid entry for tips - {items[1]}. Setting tips as - $0')
                    check_dict['tips'] = '0.0'
            if items[0] == 'Total':
                if len(items) < 2:
                    print(f'Check - {key} has invalid entry for total - {row}. Setting total as - $0')
                    check_dict['total'] = '0.0'
                    continue
                if check_float(items[1]):
                    check_dict['total'] = items[1]
                else:
                    print(f'Check - {key} has invalid entry for total - {items[1]}. Setting it as - $0')
                    check_dict['total'] = '0.0'
            if items[0] == 'Server:':
                if len(items) < 4:
                    print(f'Check - {key} has invalid entry for Server or Time - {row}. setting time as 10:10 AM')
                    in_time = datetime.datetime.strptime("10:10 AM", "%I:%M %p")
                    out_time = datetime.datetime.strftime(in_time, "%H:%M")
                    if len(items) < 2:
                        print(f'Check - {key} has invalid entry for Server or Time - {row}. setting server name as - Unknown')
                        check_dict['server'] = 'Unknown'
                    else:
                        check_dict['server'] = items[1]
                    continue
                if len(items) < 2:
                    print(f'Check - {key} has invalid entry for Server or Time - {row}. setting server name as - Unknown')
                    check_dict['server'] = 'Unknown'
                else:
                    check_dict['server'] = items[1]
                #time = items[2] + " " + items[3]
                time = '10:10 AM'
                in_time = datetime.datetime.strptime(time, "%I:%M %p")
                out_time = datetime.datetime.strftime(in_time, "%H:%M")
            if items[0] == 'Check':
                if len(items) < 4:
                    print(f'Check - {key} has invalid entry for date - {row}. setting date as 10/03/2021')
                    check_date = datetime.datetime.strptime('10/03/2021', "%m/%d/%Y").strftime("%Y-%m-%d")
                    continue
                check_date = datetime.datetime.strptime(items[3], "%m/%d/%Y").strftime("%Y-%m-%d")
    except Error as e:
        logging.ERROR(e)

    date_time = datetime.datetime.strptime(check_date + " " + out_time, "%Y-%m-%d %H:%M")
    date_time = datetime.datetime.strftime(date_time, "%Y-%m-%d %H:%M")
    check_dict['date_time'] = date_time

    return check_dict


def insert_check(check_dict):
    try:
        with connect(
                host="localhost",
                user="root",
                password="",
                database="instabase_cafeteria",
        ) as connection:
            with connection.cursor() as cursor:
                query = f"insert into checks(server, date_time, tips, total) " \
                        f"values('{check_dict['server']}','{check_dict['date_time']}',{check_dict['tips']},{check_dict['total']})"
                # insert_query = """
                # INSERT INTO checks (server, date_time, tips, total)
                # VALUES
                #     ('Srikar', '2021-07-17 11:27', 1.25,10.15)
                # """
                insert_query = f"INSERT INTO checks (server, date_time, tips, total) " \
                               f"SELECT * FROM (SELECT '{check_dict['server']}','{check_dict['date_time']}',{check_dict['tips']},{check_dict['total']}) AS TMP " \
                               f"WHERE NOT EXISTS (SELECT * FROM CHECKS WHERE server='{check_dict['server']}' AND date_time='{check_dict['date_time']}' " \
                               f"AND tips={check_dict['tips']} AND total={check_dict['total']})"
                cursor.execute(insert_query)
                connection.commit()
                print("affected rows = {}".format(cursor.rowcount))
                cursor.close()
                connection.close()
    except Error as e:
        logging.ERROR(e)
        print(e)


def get_connection():
    try:
        with connect(
                host="localhost",
                user=input("Enter username: "),
                password=getpass("Enter password: "),
                database="instabase_cafeteria",
        ) as connection:
            return connection
    except Error as e:
        print(e)
        return None
    return None


count = 0
for obj in bucket1.objects.all():
    if count > 12800 :
        file_byte_string = client.get_object(Bucket=obj.bucket_name, Key=obj.key)['Body'].read()
        logging.info(obj.key)
        print(obj.key)
        img = Image.open(BytesIO(file_byte_string))
        check_dict = parse_text(obj.key, pt.image_to_string(img))
        insert_check(check_dict)
        #print(check_dict)
    count += 1
print(count)
