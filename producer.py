from json import dumps
from kafka import KafkaProducer
from time import sleep
import csv

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8'))

# https://code.tutsplus.com/tutorials/how-to-read-and-write-csv-files-in-python--cms-29907
with open('dataset_lite.csv', newline='') as File:
    reader = csv.reader(File)
    # https://www.edureka.co/community/1792/how-to-print-array-list-without-brackets-in-python
    # https://www.daniweb.com/programming/software-development/threads/372151/getting-rid-of-brackets-and-quotations#
    for row in reader:
        clean_row = ','.join(row)
        producer.send('test', value=clean_row)
        print(clean_row)