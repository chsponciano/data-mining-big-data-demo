from data_mining_demo import fetch_zipped_data, unzip_data, save_read_data
from data_analysis_demo import get_cvs_files, initialize_dataframe, data_visualization


if __name__ == "__main__":
    _url                    = 'http://dados.gov.br/dataset/filiados-partidos-politicos'
    _class_tag              = 'resource-url-analytics'
    _tag                    = 'a'
    _attribute_tag          = 'href'
    _folder                 = 'data/'
    _action                 =  0

    _main = '=== Data Mining & Big Data === \n[1] Get Party Affiliate Data \n[2] Initialize Dataframe \n[3] View party graph by affiliates \n[4] Settings \n[5] Exit \nChoose one of the above: '
    
    while _action != 5:
        _action = int(input(_main))

        if _action == 1:
            print('\nInitializing data mining')
            print(f'Fetching compressed data from URL: {_url}')

            _compressed_urls = fetch_zipped_data(_url, _class_tag, _tag, _attribute_tag)
            unzip_data(_compressed_urls, _folder)

            save_read_data(_compressed_urls)
            print('Saving list of found data')
            print('Data Mining Completion\n')

        elif _action == 2:
            print('\nDataframe initialization')
            csv_file_list = get_cvs_files(_folder)

            print(f'Total CSV files in folder {_folder}: {len(csv_file_list)}\n')
            initialize_dataframe(csv_file_list)
        
        elif _action == 3:
            print('\nPlot initialization\n')
            data_visualization()
        
        elif _action == 4:
            print('\n=== Setting ===')
            print(f'DATA URL: {_url}')
            print(f'TAG: <{_tag}>')
            print(f'CLASS TAG: class.{_class_tag}')
            print(f'ATTRIBUTE TAG: <{_tag}>.attr_{_attribute_tag}')
            print(f'EXPORT FOLDER: {_folder}\n')