from bs4 import BeautifulSoup
from urllib.request import urlopen, urlretrieve 
from zipfile import ZipFile
from io import BytesIO
from os import path
import csv
import requests


def searching_already_read_data(_filename : str = 'data_already_read.csv') -> list:
    _data_already_read = list()

    if path.exists(_filename):
        with open(_filename) as file:
            _csv_reader = csv.reader(file)
            
            for row in _csv_reader:
                for i in range(len(row)):
                    _data_already_read.append(row[i])
            
    return _data_already_read

def save_read_data(data : list, _filename : str = 'data_already_read.csv'):
    _data_out = ''

    for url in data:
        _data_out += f'{url},'

    with open(_filename, 'w') as file:
        file.write(_data_out[:-1])

def fetch_zipped_data(url : str, class_tag : str, tag : str, attribute_tag : str) -> list:
    _html = urlopen(url) 
    _soup = BeautifulSoup(_html, 'html.parser')
    return [ _element[attribute_tag] for _element in _soup.findAll(tag, {'class', class_tag}) ]

def unzip_data(compressed_urls : list, folder : str, _total_compressed_data : int = 1):
    
    _size_compressed_data = len(compressed_urls)
    print(f'Compressed URL list size: {_size_compressed_data}')

    _data_already_read = searching_already_read_data()
    print(f'Searching list of already read data, total: {len(_data_already_read)}')
    
    for url in compressed_urls:
        try:
            if url in _data_already_read:
                print(f'{_total_compressed_data}/{_size_compressed_data} Already unzipped url: {url}')
            else:
                print(f'{_total_compressed_data}/{_size_compressed_data} Current Compressed URL: {url}')
                _url_content = requests.get(url)
                _compressed = ZipFile(BytesIO(_url_content.content))
                _compressed.extractall(path=folder)    

        except Exception as ex:
            continue

        finally:
            _total_compressed_data += 1