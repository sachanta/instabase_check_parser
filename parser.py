import boto3
import datetime
import pytesseract as pt
import logging
from getpass import getpass
from mysql.connector import connect, Error
from io import BytesIO
from PIL import Image

logging.basicConfig(filename='app.log', format='%(asctime)s - %(message)s')

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
    #print(text)
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
                hours = str(items[2]).split(":")
                if int(hours[1]) > 59:
                    time = hours[0] + ":30 " + items[3]
                else:
                    time = items[2] + " " + items[3]
                in_time = datetime.datetime.strptime(time, "%I:%M %p")
                out_time = datetime.datetime.strftime(in_time, "%H:%M")
            if items[0] == 'Check':
                if len(items) < 4:
                    print(f'Check - {key} has invalid entry for date or check number - {row}. setting date as 10/03/2021 and check number to 0')
                    check_date = datetime.datetime.strptime('10/03/2021', "%m/%d/%Y").strftime("%Y-%m-%d")
                    check_dict['check_number'] = items[2]
                    continue
                check_date = datetime.datetime.strptime(items[3], "%m/%d/%Y").strftime("%Y-%m-%d")
                if check_int(items[2]):
                    check_dict['check_number'] = items[2]
                else:
                    print(f'Check - {key} has invalid entry for check number - {items[2]}. Setting check number as - 0')
                    check_dict['check_number'] = 0

            if items[0] == 'Table:':
                if len(items) < 4:
                    print(f'Check - {key} has invalid entry for table number or guests - {row}. setting those to 0')
                    check_dict['table_number'] = 0
                    check_dict['guests'] = 0
                    continue
                if check_int(items[1]):
                    check_dict['table_number'] = int(items[1])
                else:
                    print(f'Check - {key} has invalid entry for table number - {items[2]}. Setting table number as - 0')
                    check_dict['table_number'] = 0
                if check_int(items[3]):
                    check_dict['guests'] = int(items[3])
                else:
                    print(f'Check - {key} has invalid entry for guests - {items[3]}. Setting guests as - 0')
                    check_dict['guests'] = 0
            if items[0] == 'Sub-total':
                if len(items) < 2:
                    print(f'Check - {key} has invalid entry for sub total - {row}. Setting sub total as - $0')
                    check_dict['sub_total'] = '0.0'
                    continue
                if check_float(items[1]):
                    check_dict['sub_total'] = items[1]
                else:
                    print(f'Check - {key} has invalid entry for sub total - {items[1]}. Setting it as - $0')
                    check_dict['sub_total'] = '0.0'
            if items[0] == 'Sales':
                if len(items) < 3:
                    print(f'Check - {key} has invalid entry for sales tax - {row}. Setting sales tax as - $0')
                    check_dict['sales_tax'] = '0.0'
                    continue
                if check_float(items[2]):
                    check_dict['sales_tax'] = items[2]
                else:
                    print(f'Check - {key} has invalid entry for sales tax - {items[2]}. Setting it as - $0')
                    check_dict['sales_tax'] = '0.0'

    except Error as e:
        logging.ERROR(e)

    date_time = datetime.datetime.strptime(check_date + " " + out_time, "%Y-%m-%d %H:%M")
    date_time = datetime.datetime.strftime(date_time, "%Y-%m-%d %H:%M")
    check_dict['date_time'] = date_time

    return check_dict


def insert_check(check_dict):
    try:
        with connect(
                host= mysql_host,
                user= mysql_user,
                password= mysql_password,
                database= mysql_database,
        ) as connection:
            with connection.cursor() as cursor:

                insert_query = f"INSERT INTO checks (check_number, server, table_number, guests, sub_total, sales_tax, date_time, tips, total) " \
                               f"SELECT * FROM (SELECT '{check_dict['check_number']}' as check_num, '{check_dict['server']}' as server , '{check_dict['table_number']}' as table_num, '{check_dict['guests']}' as quests, '{check_dict['sub_total']}' as sub_total, '{check_dict['sales_tax']}' as sales_tax, '{check_dict['date_time']}' as date_time, {check_dict['tips']} as tips,{check_dict['total']} as total) AS TMP " \
                               f"WHERE NOT EXISTS (SELECT * FROM CHECKS WHERE server='{check_dict['server']}' AND date_time='{check_dict['date_time']}' AND tips={check_dict['tips']} AND total={check_dict['total']})"
                #print(insert_query)
                cursor.execute(insert_query)
                connection.commit()
                cursor.close()
                connection.close()
    except Error as e:
        logging.ERROR(e)
        print(e)


count = 0
for obj in bucket1.objects.all():
    if count == 28:
        file_byte_string = client.get_object(Bucket=obj.bucket_name, Key=obj.key)['Body'].read()
        logging.info(obj.key)
        print(obj.key)
        img = Image.open(BytesIO(file_byte_string))
        check_dict = parse_text(obj.key, pt.image_to_string(img))
        insert_check(check_dict)
    count += 1
