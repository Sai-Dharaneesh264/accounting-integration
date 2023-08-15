import json
import pandas as pd
import os
import logging

path = './raw_files'
dir_list = os.listdir(path)
logger = logging.getLogger(__name__)

# TODO: should convert list to enum
months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

def get_raw_files():
    path = './raw_files'
    dir_list = os.listdir(path)
    return dir_list

def get_common_data():
    month = input('Enter month: ')
    if month.lower() not in months:
        raise Exception('Enter Valid Month')
    year = int(input('Enter Year: '))
    if not 2020 < year <= 2023:
        raise Exception('Enter Valid Year')
    dest_folder = input('Enter Destination Folder: ')
    return month, year, dest_folder

def get_config(org_code: str, loc_code: str, file_name: str, month: str, year: int):
    return {
        "schema": org_code,
        "org_code": org_code,
        "month": month,
        "year": year,
        "location_code": loc_code,
        "file_name": file_name
    }

def create_config_json_file(config_list, file_name):
    with open(file_name, "w") as f:
        f.write(json.dumps(config_list))

def create_files(df, df_new, actual_columns):
    org_code = input('Enter the org code: ')
    logger.info(f'organization code = {org_code}')
    config_list = []
    month, year, dest_folder = get_common_data()
    for idx, value in enumerate(actual_columns):
        df_new = df.iloc[:, [0, idx + 1]]
        p_and_l_file_name = f'{value}-{month}{year}.csv'
        if not os.path.exists(f"./{dest_folder}"):
            paths = os.path.join('./', dest_folder)
            os.mkdir(paths)
        path = f"./{dest_folder}/{p_and_l_file_name}"
        d = get_config(
            org_code=org_code,
            loc_code=value,
            file_name=p_and_l_file_name,
            month=month,
            year=year,
        )
        config_list.append(d)

        df_new.to_csv(path, header=False, index=False)
    config_file = f"./{dest_folder}/{month}-{year}.json"
    create_config_json_file(config_list, config_file)


def process_files(raw_file):
    logger.info(f'current raw file = {raw_file}')
    raw_file_path = f'./raw_files/{raw_file}'
    logger.info(f'raw files path = {raw_file_path}')
    df = pd.read_csv(raw_file_path, sep=',')
    df_columns = [column for column in df]
    actual_columns = df_columns[1:]
    df_new = df.iloc[:, 1:]
    create_files(
        df=df,
        df_new=df_new, 
        actual_columns=actual_columns
    )

def move_processed_file(raw_file):
    source = f'./raw_files/{raw_file}'
    destination = f'./processed_files/{raw_file}'
    os.rename(source, destination)

if __name__ == '__main__':
    logger.info('started the ingestion')
    raw_file = get_raw_files()[0]
    logger.info(f'raw file = {raw_file}')
    process_files(raw_file)
    move_processed_file(raw_file)
    logger.info('ingestion ended')
