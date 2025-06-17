"""
How to use RxPY to prepare batches for asyncio client.
"""
import asyncio
from csv import DictReader

import reactivex as rx
from reactivex import operators as ops
from reactivex.scheduler.eventloop import AsyncIOScheduler

from influxdb_client import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.domain.write_precision import WritePrecision

import os
import csv
import json

empty_folder_path = 'csv-source/bad-csvs'

def is_csv_file_empty(file_path):
    return os.path.getsize(file_path) == 0

def get_config():
    # Opening JSON file
    f = open('abbreviated_datapoints.json')
        
    # returns JSON object as 
    # a dictionary
    data = json.load(f)
        
    # Iterating through the json
    # list
    #for i in data['dataPoints']:
    #print(i)
        
    # Closing file
    f.close()
    return data

ab_dps = get_config()


def generate_influx_point(data_dict):
    point = Point.from_dict(data_dict,
                                        write_precision=WritePrecision.NS,
                                        record_measurement_key="table",
                                        record_time_key="time",
                                        record_tag_keys=["dlid", "did", "f"],
                                        record_field_keys=["value"])
    return point

def decimal_to_binary_mapping_and_return_influx_point(table_name,ts,dlid,did,f,value,mapping):
    # Convert number to binary and pad it to 16 bits
    binary_string = bin(int(value))[2:].zfill(16)  # remove '0b' and pad with zeros to 16 bits

    # Ensure the mapping list is also of length 16
    if len(mapping) != 16:
        raise ValueError("Mapping list must have exactly 16 elements.")
    
    # Map each bit position to a string using the provided mapping
    dictionary = {}
    dictionary['table'] = table_name
    dictionary['time'] = ts
    dictionary['dlid'] = dlid
    dictionary['did'] = did
    for i, bit in enumerate(binary_string):
        key = str(i)
        if(mapping[key] == ""):
            continue
        dictionary['f'] = mapping[key]
        dictionary['value'] = float(bit)
        yield generate_influx_point(dictionary)

    

def getMappingOfabdps(needed_item):
    # Iterate through each field in the 'fields' array
    for field in ab_dps['fields']:
        if 'field_name' in field and field['field_name'] == needed_item:
            return field['binary_splitup']
        else:
            return False

def getAbdps():
    ab_dps_alone = []
    # Iterate through each field in the 'fields' array 
    for field in ab_dps['fields']:
        ab_dps_alone.append(field['field_name'])
    return ab_dps_alone


def csv_to_generator():
    folder_path = 'csv-source'
    csv_files = [f for f in os.listdir(folder_path) if f.endswith(('.csv'))]
    for file in csv_files:
        filename = file.split(".csv")
        dlid = filename[0].split("_")[0]
        whole_path = os.path.join(folder_path, file)
        print(whole_path)
        if is_csv_file_empty(whole_path):
            if not os.path.exists(empty_folder_path):
                os.makedirs(empty_folder_path)
            # Get the new path for the file in the empty folder
            new_file_path = os.path.join(empty_folder_path, os.path.basename(whole_path))
            # Rename/move the file to the empty folder
            os.rename(whole_path, new_file_path)
            continue
        """
        Parse your CSV file into generator
        """
        # Create a DictReader object
        reader = csv.reader(open(whole_path, 'r'))
        #remove the csv once it is read
        os.remove(whole_path)

        for row in reader:
            # Define headers manually
            headers = row
            break
        ## Chuck file (partial buffered data in header); so skipping that csv file##    
        if(headers[0]!="ts"):
            os.remove(whole_path)
            print("Chunk file, Removed "+whole_path+" from parsing directory")
            continue
            
        dictionary = {}
        for csvrow in reader:
            for i, row in enumerate(csvrow):
                if(i == 0):
                    continue
                did = headers[i].split("_")[0]
                ts = ((int(csvrow[0])-19800)*1000000000)
                dictionary['table'] = "wattmon_std_mv"
                dictionary['time'] = ts
                dictionary['dlid'] = dlid
                dictionary['did'] = did
                f = headers[i].split("_", 1)[1]
                dictionary['f'] = f
                if type(csvrow[i]) == type(None):
                    continue
                elif csvrow[i] == "":
                    continue
                #elif type(csvrow[i]) == str:
                #    continue

                elif csvrow[i] == "-":
                    continue
                else:
                    dictionary['value'] = float(csvrow[i])
                    #checking if current field is available in abbreviated_datapoints.json
                    if(dlid+"_"+did+"_"+f in getAbdps()):
                        if(mapping := getMappingOfabdps(dlid+"_"+did+"_"+f)):
                            yield from decimal_to_binary_mapping_and_return_influx_point("wattmon_std_mv",ts,dlid,did,f,csvrow[i],mapping)                 
                yield generate_influx_point(dictionary)


async def main():
    async with InfluxDBClientAsync(url='', token='', org='') as client:
        write_api = client.write_api()

        """
        Async write
        """

        async def async_write(batch):
            """
            Prepare async task
            """
            await write_api.write(bucket='', record=batch)
            return batch

        """
        Prepare batches from generator
        """
        batches = rx \
            .from_iterable(csv_to_generator()) \
            .pipe(ops.buffer_with_count(5000)) \
            .pipe(ops.map(lambda batch: rx.from_future(asyncio.ensure_future(async_write(batch)))), ops.merge_all())

        done = asyncio.Future()

        """
        Write batches by subscribing to Rx generator
        """
        batches.subscribe(on_next=lambda batch: print(f'Written batch... {len(batch)}'),
                          on_error=lambda ex: print(f'Unexpected error: {ex}'),
                          on_completed=lambda: done.set_result(0),
                          scheduler=AsyncIOScheduler(asyncio.get_event_loop()))
        """
        Wait to finish all writes
        """
        await done


if __name__ == "__main__":
    asyncio.run(main())
