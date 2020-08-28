# Imports
import pkg_resources.py2_warn
import pandas as pd
import numpy as np
import random
import os
import json
import requests
import time

from tqdm.auto import tqdm
from pathlib import Path
from datetime import datetime, timedelta


class BaseExtractor:
    """
    Base Extractor class where all other Extractor classes inherit from.
    Extracts information from the carrier Portal.

    Methods
    -------
    get_location_id:
        Gets the countryID mappings via the CountryID API in order to use the Search Schedules API.

    prepare:
        A single query to the API can provide information to multiple lines on the delay_sheet.
        Further filters self.delay_sheet to a smaller list of searches needed to fulfill all the lines on the
            delay_sheet. This reduces the total number of calls made to the Search Schedules API and prevents
            duplication of API calls.

    call_api:
        Makes calls to the respective API, using information from the prepare method as parameters in the
        API request. Also saves the API responses into a subdirectory "responses/<today_date>".

    extract:
        Extracts information from the JSON responses from the call_api method and assembles the final dataframe.
    """

    def __init__(self):
        pass

    def get_location_id(self):
        pass

    def prepare(self):
        pass

    def call_api(self):
        pass

    def extract(self):
        pass


class ONEExtractor(BaseExtractor):
    def __init__(self, main_delay_sheet: pd.DataFrame, interval: tuple, carrier_mapping: dict):
        self.carrier_mapping = carrier_mapping
        self.interval = interval
        self.port_id = {}
        self.session = requests.Session()

        self.delay_sheet = (main_delay_sheet.query(f"`Fwd Agent` in {[k for k,v in self.carrier_mapping.items() if v == 'ONE']}")
                            .drop(['updated_etd', 'updated_eta', 'No. of days delayed ETD',
                                   'No. of days delayed ETA', 'Reason of Delay'], axis=1)
                            .copy())

        self.port_mapping = {v['Port Code']: v['Port Name'] for k, v in (pd.read_excel('../../data/Port Code Mapping - ONE.xlsx')
                                                                         .to_dict('index').items())}

        self.delay_sheet = self.delay_sheet.assign(pol_name=lambda x: x['Port of Loading'],
                                                   pod_name=lambda x: x['Port of discharge']).copy()

        self.delay_sheet.pod_name = self.delay_sheet.pod_name.replace(
            self.port_mapping)

    def prepare(self):
        key = ['pol_name', 'pod_name']
        self.reduced_df = self.delay_sheet.drop_duplicates(key)[
            key].sort_values(key)

        self.reduced_df.dropna(inplace=True)

    def call_api(self):
        def get_schedules(pol_name: str, pod_name: str):
            url = "https://ecomm.one-line.com/ecom/CUP_HOM_3001GS.do"

            first_day = datetime.today().replace(day=1).strftime('%Y-%m-%d')
            last_day = datetime.today().replace(day=1).replace(
                month=datetime.today().month+3).strftime('%Y-%m-%d')

            payload = f'f_cmd=3&por_cd={pol_name}&del_cd={pod_name}&rcv_term_cd=Y&de_term_cd=Y&frm_dt={first_day}&to_dt={last_day}&ts_ind=&skd_tp=L'
            headers = {
                'Connection': 'keep-alive',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'X-Requested-With': 'XMLHttpRequest',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Origin': 'https://ecomm.one-line.com',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Dest': 'empty',
                'Referer': 'https://ecomm.one-line.com/ecom/CUP_HOM_3001.do?sessLocale=en',
                'Accept-Language': 'en-GB,en;q=0.9',
            }

            return self.session.post(url, headers=headers, data=payload)

        self.response_jsons = []
        for row in tqdm(self.reduced_df.itertuples(), total=len(self.reduced_df)):
            response_filename = f'ONE {row.pol_name}-{row.pod_name}.json'
            if response_filename not in os.listdir():
                response = get_schedules(row.pol_name, row.pod_name)
                self.response_jsons.append(response.json())
                if len(response.json()):
                    write_json(response.json(), response_filename)
                time.sleep(random.randint(*self.interval))
            else:
                with open(response_filename, 'r') as f:
                    self.response_jsons.append(json.load(f))

    def extract(self):
        def get_relevant_fields(response, i):
            def get_vv(response, i):
                return response.get('list')[i]['n1stVslNm'].rsplit(maxsplit=1)

            return {
                'pol_name': response.get('list')[i]['polYdCd'][:5],
                'pod_name': response.get('list')[i]['lstPodYdCd'][:5],
                'Vessel': get_vv(response, i)[0],
                'Voyage': get_vv(response, i)[1],
                'updated_etd': response.get('list')[i]['polEtdDt'],
                'updated_eta': response.get('list')[i]['lstPodEtaDt']
            }

        self.response_df = pd.DataFrame(([get_relevant_fields(response, i)
                                          for response in self.response_jsons
                                          if response.get('list')
                                          for i in range(len(response.get('list')))]))

        if len(self.response_df):
            merge_key = ['pol_name', 'pod_name', 'Vessel', 'Voyage']
            self.response_df = self.response_df.sort_values(
                'updated_eta').drop_duplicates(merge_key)

            self.response_df.updated_eta = pd.to_datetime(
                self.response_df.updated_eta)
            self.response_df.updated_etd = pd.to_datetime(
                self.response_df.updated_etd)

            self.delay_sheet = (self.delay_sheet.reset_index().
                                merge(self.response_df[merge_key + ['updated_eta', 'updated_etd']],
                                      on=merge_key, how='left')
                                .set_index('index')
                                .copy())

        else:
            self.response_df = pd.DataFrame({
                'pol_name': [], 'pod_name': [],
                'Vessel': [], 'Voyage': [],
                'updated_eta': [], 'updated_etd': []})


class COSCOExtractor(BaseExtractor):
    def __init__(self, main_delay_sheet: pd.DataFrame, interval: tuple, carrier_mapping: dict):
        self.carrier_mapping = carrier_mapping
        self.interval = interval
        self.port_id = {}
        self.session = requests.Session()

        self.delay_sheet = (main_delay_sheet.query(f"`Fwd Agent` in {[k for k,v in self.carrier_mapping.items() if v == 'COSCO']}")
                            .drop(['updated_etd', 'updated_eta', 'No. of days delayed ETD',
                                   'No. of days delayed ETA', 'Reason of Delay'], axis=1)
                            .copy())

        self.port_mapping = {v['Port Code']: (v['Port Name'], v['Port Number']) for k, v in (pd.read_excel('../../data/Port Code Mapping - COSCO.xlsx')
                                                                                             .to_dict('index').items())}

        self.delay_sheet = self.delay_sheet.assign(pol=lambda x: x['Port of Loading'],
                                                   pod=lambda x: x['Port of discharge'],
                                                   pol_name=lambda x: x['Port of Loading'].apply(
                                                       lambda y: self.port_mapping.get(y)[0]),
                                                   pod_name=lambda x: x['Port of discharge'].apply(
                                                       lambda y: self.port_mapping.get(y)[0]),
                                                   pol_code=lambda x: x['Port of Loading'].apply(
                                                       lambda y: self.port_mapping.get(y)[1]),
                                                   pod_code=lambda x: x['Port of discharge'].apply(lambda y: self.port_mapping.get(y)[1])).copy()

    def prepare(self):
        key = ['pol', 'pod', 'pol_name', 'pod_name', 'pol_code', 'pod_code']
        self.reduced_df = self.delay_sheet.drop_duplicates(key)[
            key].sort_values(key)

        self.reduced_df.dropna(inplace=True)

    def call_api(self):
        def get_schedules(pol_code: str, pod_code: str, pol_name: str, pod_name: str):
            url = "https://elines.coscoshipping.com/ebschedule/public/purpoShipmentWs"
            payload = {
                "fromDate": f"{datetime.today().strftime('%Y-%m-%d')}",
                "pickup": "B",
                "delivery": "B",
                "estimateDate": "D",
                "toDate": f"{(datetime.today() + timedelta(days=89)).strftime('%Y-%m-%d')}",
                "originCityUuid": f"{pol_code}",
                "destinationCityUuid": f"{pod_code}",
                "originCity": f"{pol_name}",
                "destinationCity": f"{pod_name}",
                "cargoNature": "All"
            }

            headers = {
                'Connection': 'keep-alive',
                'Accept': '*/*',
                'language': 'en_US',
                'sys': 'eb',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
                'Content-Type': 'application/json',
                'Origin': 'https://elines.coscoshipping.com',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Dest': 'empty',
                'Referer': 'https://elines.coscoshipping.com/ebusiness/sailingSchedule/searchByCity/resultByCity',
                'Accept-Language': 'en-GB,en;q=0.9',
            }

            return self.session.post(url, headers=headers, json=payload)

        self.response_jsons = []
        for row in tqdm(self.reduced_df.itertuples(), total=len(self.reduced_df)):
            response_filename = f'COSCO {row.pol}-{row.pod}.json'
            if response_filename not in os.listdir():
                response = get_schedules(int(row.pol_code), int(
                    row.pod_code), row.pol_name, row.pod_name)
                self.response_jsons.append(response.json())
                if len(response.json()):
                    write_json(response.json(), response_filename)
                time.sleep(random.randint(*self.interval))
            else:
                with open(response_filename, 'r') as f:
                    self.response_jsons.append(json.load(f))

    def extract(self):
        def get_relevant_fields(response, i):
            return {
                'pol_code': response['data']['content']['data'][i]['pol'],
                'pod_code': response['data']['content']['data'][i]['pod'],
                'Voyage': response['data']['content']['data'][i]['extVoyage'],
                'Vessel': response['data']['content']['data'][i]['vessel'],
                'updated_etd': response['data']['content']['data'][i]['etd'],
                'updated_eta': response['data']['content']['data'][i]['eta']
            }

        self.response_df = pd.DataFrame(([get_relevant_fields(response, i)
                                          for response in self.response_jsons
                                          if len(response)
                                          for i in range(len(response['data']['content']['data']))]))

        # Create reverse mapping from port_code to name
        port_id_reversed = {v['First Name']: v['Port Name'] for k, v in (pd.read_excel('../../data/Port Code Mapping - COSCO.xlsx')
                                                                         .to_dict('index').items())}

        if len(self.response_df):
            self.response_df['pol_name'] = self.response_df.pol_code.map(
                port_id_reversed)
            self.response_df['pod_name'] = self.response_df.pod_code.map(
                port_id_reversed)

            merge_key = ['pol_name', 'pod_name', 'Vessel', 'Voyage']
            self.response_df = self.response_df.sort_values(
                'updated_eta').drop_duplicates(merge_key)

            self.delay_sheet = (self.delay_sheet.reset_index().
                                merge(self.response_df[merge_key + ['updated_eta', 'updated_etd']],
                                      on=merge_key, how='left')
                                .set_index('index')
                                .copy())

            self.delay_sheet.updated_eta = pd.to_datetime(
                self.delay_sheet.updated_eta.str[:10], format='%Y-%m-%d')
            self.delay_sheet.updated_etd = pd.to_datetime(
                self.delay_sheet.updated_etd.str[:10], format='%Y-%m-%d')
        else:
            self.response_df = pd.DataFrame({
                'pol_name': [], 'pod_name': [],
                'Vessel': [], 'Voyage': [],
                'updated_eta': [], 'updated_etd': []})


class CMAExtractor(BaseExtractor):
    def __init__(self, main_delay_sheet: pd.DataFrame, interval: tuple, carrier_mapping: dict):
        self.carrier_mapping = carrier_mapping
        self.interval = interval
        self.session = requests.Session()

        self.delay_sheet = (main_delay_sheet.query(f"`Fwd Agent` in {[k for k,v in self.carrier_mapping.items() if v == 'CMA']}")
                            .drop(['updated_etd', 'updated_eta', 'No. of days delayed ETD',
                                   'No. of days delayed ETA', 'Reason of Delay'], axis=1)
                            .copy())

        self.port_mapping = {v['Port Code']: v['Port Name'] for k, v in (pd.read_excel('../../data/Port Code Mapping - CMA.xlsx')
                                                                         .to_dict('index').items())}

        self.delay_sheet = self.delay_sheet.assign(pol_name=lambda x: x['Port of Loading'],
                                                   pod_name=lambda x: x['Port of discharge']).copy()

        self.delay_sheet.pod_name = self.delay_sheet.pod_name.replace(
            self.port_mapping)

    def get_location_id(self):
        port_id_file = 'CMA portID.json'
        if port_id_file not in os.listdir():
            def get_id(response):
                if len(response.json()):
                    return tuple(response.json()[0].get('ActualName').split(' ; '))
                return None

            def query_id(port: str):
                url = f"https://www.cma-cgm.com/api/PortsWithInlands/GetAll?id={port}&manageChineseRegions=true"
                headers = {
                    'X-Requested-With': 'XMLHttpRequest',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Referer': 'https://www.cma-cgm.com/ebusiness/schedules',
                    'Sec-Fetch-Site': 'same-origin',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Dest': 'empty',
                    'Accept': '*/*',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
                    'Accept-Language': 'en-GB,en;q=0.9',
                }
                return self.session.get(url, headers=headers)

            locations = (list(self.delay_sheet.pol_name.unique(
            )) + list(self.delay_sheet.pod_name.unique()))
            self.port_id = {location: get_id(
                query_id(location)) for location in tqdm(locations)}
            if len(self.port_id):
                write_json(self.port_id, port_id_file)

            # PODs with no pod_id
            exception_cases = [
                k for k, v in self.port_id.items() if v is None]
            if len(exception_cases):
                write_json(exception_cases, 'cma_exceptions.txt')
        else:
            read_config(self, 'port_id', port_id_file)

    def prepare(self):
        key = ['pol_name', 'pod_name']
        self.reduced_df = self.delay_sheet.drop_duplicates(key)[
            key].sort_values(key)

        self.reduced_df['pol_code'] = self.reduced_df.pol_name.map(
            self.port_id)
        self.reduced_df['pod_code'] = self.reduced_df.pod_name.map(
            self.port_id)

        self.reduced_df.dropna(inplace=True)

    def call_api(self):
        def get_schedules(pol_code: tuple, pod_code: tuple):
            pol_1, pol_2, pol_3 = pol_code
            pod_1, pod_2, pod_3 = pod_code
            pod_2 = pod_2.replace(',', '%2C')
            url = f'https://www.cma-cgm.com/ebusiness/schedules/routing-finder?POLDescription={pol_1}+%3B+{pol_2}+%3B+{pol_3}&PODDescription={pod_1}+%3B+{pod_2}+%3B+{pod_3}&g-recaptcha-response=undefined&actualPOLDescription={pol_1}+%3B+{pol_2}+%3B+{pol_3}&actualPODDescription={pod_1}+%3B+{pod_2}+%3B+{pod_3}'
            return self.session.get(url)

        self.response_jsons = []
        for row in tqdm(self.reduced_df.itertuples(), total=len(self.reduced_df)):
            response_filename = f'CMA {row.pol_name}-{row.pod_name}.html'
            if response_filename not in os.listdir():
                response = get_schedules(row.pol_code, row.pod_code)
                self.response_jsons.append(response.text)
                if len(response.text):
                    with open(response_filename, 'w') as f:
                        f.write(response.text)
                time.sleep(random.randint(*self.interval))
            else:
                with open(response_filename, 'r') as f:
                    self.response_jsons.append(f.read())

    def extract(self):
        """
        Extracts information from the HTML responses from the call_api method and assembles the final dataframe.

        Extracts data from all columns of a dataframe.
        Generates a dataframe for every table
        Loop over all tables -> dataframes
        Concatenates all these dataframes into one dataframe
        """
        self.response_intermediate = [pd.read_html(
            response) for response in self.response_jsons]
        reverse_port_id = {v[0].split(
            ", ")[0]: k for k, v in self.port_id.items() if v is not None}

        def get_updated_eta(row):
            for item in row:
                if len(item) > 10:
                    return pd.to_datetime(item)
            return None

        list_of_dataframes = []
        for i in self.response_intermediate:
            for table in i:
                pol = table.iloc[1].str.split("  ").str[0][1:].str.split(
                    ", ").str[0].apply(reverse_port_id.get)

                if table.shape[0] == 22:
                    pod_row, updated_eta_row = (21, 20)
                elif table.shape[0] == 18:
                    pod_row, updated_eta_row = (17, 16)
                elif table.shape[0] == 14:
                    pod_row, updated_eta_row = (13, 12)
                elif table.shape[0] == 10:
                    pod_row, updated_eta_row = (9, 8)
                else:
                    pod_row, updated_eta_row = (5, 4)

                pod = table.iloc[pod_row].str.split("  ")[1:].str[0].str.split(
                    ", ").str[0].apply(reverse_port_id.get)
                updated_etd = table.iloc[2][1:].str.split(
                    r"(\w{6,},)").apply(get_updated_eta)
                updated_eta = table.iloc[updated_eta_row][1:].apply(
                    pd.to_datetime)
                vessel = table.iloc[3][1:].str.split("  ").str[1]
                voyage = table.iloc[3][1:].str.split("  ").str[-1]

                list_of_dataframes.append(pd.DataFrame({'pol_name': pol, 'pod_name': pod, 'Vessel': vessel,
                                                        'Voyage': voyage, 'updated_etd': updated_etd,
                                                        'updated_eta': updated_eta}))

        merge_key = ['pol_name', 'pod_name', 'Vessel', 'Voyage']
        if len(list_of_dataframes):
            self.response_df = pd.concat(
                list_of_dataframes).drop_duplicates(merge_key)
            self.response_df.sort_values(['updated_eta'], inplace=True)
            self.response_df.reset_index(drop=True, inplace=True)

            self.delay_sheet = (self.delay_sheet.reset_index().
                                merge(self.response_df[merge_key + ['updated_eta', 'updated_etd']],
                                      on=merge_key, how='left')
                                .set_index('index')
                                .copy())
        else:
            self.response_df = pd.DataFrame({
                'pol_name': [], 'pod_name': [],
                'Vessel': [], 'Voyage': [],
                'updated_eta': [], 'updated_etd': []})


class ANLExtractor(BaseExtractor):
    def __init__(self, main_delay_sheet: pd.DataFrame, interval: tuple, carrier_mapping: dict):
        self.carrier_mapping = carrier_mapping
        self.interval = interval
        self.session = requests.Session()

        self.delay_sheet = (main_delay_sheet.query(f"`Fwd Agent` in {[k for k,v in self.carrier_mapping.items() if v == 'ANL']}")
                            .drop(['updated_etd', 'updated_eta', 'No. of days delayed ETD',
                                   'No. of days delayed ETA', 'Reason of Delay'], axis=1)
                            .copy())

        self.port_mapping = {v['Port Code']: v['Port Name'] for k, v in (pd.read_excel('../../data/Port Code Mapping - ANL.xlsx')
                                                                         .to_dict('index').items())}

        self.delay_sheet = self.delay_sheet.assign(pol_name=lambda x: x['Port of Loading'],
                                                   pod_name=lambda x: x['Port of discharge']).copy()

        self.delay_sheet.pod_name = self.delay_sheet.pod_name.replace(
            self.port_mapping)

    def get_location_id(self):
        port_id_file = 'ANL portID.json'
        if port_id_file not in os.listdir():
            def get_id(response):
                if len(response.json()):
                    return tuple(response.json()[0].get('ActualName').split(' ; '))
                return None

            def query_id(port: str):
                url = f"https://www.anl.com.au/api/PortsWithInlands/GetAll?id={port}&manageChineseRegions=true"
                headers = {
                    'X-Requested-With': 'XMLHttpRequest',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Referer': 'https://www.anl.com.au/ebusiness/schedules',
                    'Sec-Fetch-Site': 'same-origin',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Dest': 'empty',
                    'Accept': '*/*',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
                    'Accept-Language': 'en-GB,en;q=0.9',
                }
                return self.session.get(url, headers=headers)

            locations = (list(self.delay_sheet.pol_name.unique(
            )) + list(self.delay_sheet.pod_name.unique()))
            self.port_id = {location: get_id(
                query_id(location)) for location in tqdm(locations)}
            if len(self.port_id):
                write_json(self.port_id, port_id_file)

            # PODs with no pod_id
            exception_cases = [
                k for k, v in self.port_id.items() if v is None]
            if len(exception_cases):
                write_json(exception_cases, 'anl_exceptions.txt')
        else:
            read_config(self, 'port_id', port_id_file)

    def prepare(self):
        key = ['pol_name', 'pod_name']
        self.reduced_df = self.delay_sheet.drop_duplicates(key)[
            key].sort_values(key)

        self.reduced_df['pol_code'] = self.reduced_df.pol_name.map(
            self.port_id)
        self.reduced_df['pod_code'] = self.reduced_df.pod_name.map(
            self.port_id)

        self.reduced_df.dropna(inplace=True)

    def call_api(self):
        def get_schedules(pol_code: tuple, pod_code: tuple):
            pol_1, pol_2, pol_3 = pol_code
            pod_1, pod_2, pod_3 = pod_code
            pod_2 = pod_2.replace(',', '%2C')
            url = f'https://www.anl.com.au/ebusiness/schedules/routing-finder?POLDescription={pol_1}+%3B+{pol_2}+%3B+{pol_3}&PODDescription={pod_1}+%3B+{pod_2}+%3B+{pod_3}&g-recaptcha-response=undefined&actualPOLDescription={pol_1}+%3B+{pol_2}+%3B+{pol_3}&actualPODDescription={pod_1}+%3B+{pod_2}+%3B+{pod_3}'
            return self.session.get(url)

        self.response_jsons = []
        for row in tqdm(self.reduced_df.itertuples(), total=len(self.reduced_df)):
            response_filename = f'ANL {row.pol_name}-{row.pod_name}.html'
            if response_filename not in os.listdir():
                response = get_schedules(row.pol_code, row.pod_code)
                self.response_jsons.append(response.text)
                if len(response.text):
                    with open(response_filename, 'w') as f:
                        f.write(response.text)
                time.sleep(random.randint(*self.interval))
            else:
                with open(response_filename, 'r') as f:
                    self.response_jsons.append(f.read())

    def extract(self):
        self.response_intermediate = [pd.read_html(
            response) for response in self.response_jsons]
        reverse_port_id = {v[0].split(
            ", ")[0]: k for k, v in self.port_id.items() if v is not None}

        def get_updated_eta(row):
            for item in row:
                if len(item) > 10:
                    return pd.to_datetime(item)
            return None

        list_of_dataframes = []
        for i in self.response_intermediate:
            for table in i:
                pol = table.iloc[1].str.split("  ").str[0][1:].str.split(
                    ", ").str[0].apply(reverse_port_id.get)

                if table.shape[0] == 22:
                    pod_row, updated_eta_row = (21, 20)
                elif table.shape[0] == 18:
                    pod_row, updated_eta_row = (17, 16)
                elif table.shape[0] == 14:
                    pod_row, updated_eta_row = (13, 12)
                elif table.shape[0] == 10:
                    pod_row, updated_eta_row = (9, 8)
                else:
                    pod_row, updated_eta_row = (5, 4)

                pod = table.iloc[pod_row].str.split("  ")[1:].str[0].str.split(
                    ", ").str[0].apply(reverse_port_id.get)
                updated_etd = table.iloc[2][1:].str.split(
                    r"(\w{6,},)").apply(get_updated_eta)
                updated_eta = table.iloc[updated_eta_row][1:].apply(
                    pd.to_datetime)
                vessel = table.iloc[3][1:].str.split("  ").str[1]
                voyage = table.iloc[3][1:].str.split("  ").str[-1]

                list_of_dataframes.append(pd.DataFrame({'pol_name': pol, 'pod_name': pod, 'Vessel': vessel,
                                                        'Voyage': voyage, 'updated_etd': updated_etd,
                                                        'updated_eta': updated_eta}))

        merge_key = ['pol_name', 'pod_name', 'Vessel', 'Voyage']
        if len(list_of_dataframes):
            self.response_df = pd.concat(
                list_of_dataframes).drop_duplicates(merge_key)
            self.response_df.sort_values(['updated_eta'], inplace=True)
            self.response_df.reset_index(drop=True, inplace=True)

            self.delay_sheet = (self.delay_sheet.reset_index().
                                merge(self.response_df[merge_key + ['updated_eta', 'updated_etd']],
                                      on=merge_key, how='left')
                                .set_index('index')
                                .copy())
        else:
            self.response_df = pd.DataFrame({
                'pol_name': [], 'pod_name': [],
                'Vessel': [], 'Voyage': [],
                'updated_eta': [], 'updated_etd': []})


class HamburgExtractor(BaseExtractor):
    def __init__(self, main_delay_sheet: pd.DataFrame, interval: tuple, carrier_mapping: dict):
        self.carrier_mapping = carrier_mapping
        self.interval = interval
        self.session = requests.Session()

        self.delay_sheet = (main_delay_sheet.query(f"`Fwd Agent` in {[k for k,v in self.carrier_mapping.items() if v == 'HAMBURG']}")
                            .drop(['updated_etd', 'updated_eta', 'No. of days delayed ETD',
                                   'No. of days delayed ETA', 'Reason of Delay'], axis=1)
                            .copy())

        self.port_mapping = {v['Port Code']: v['Port Name'] for k, v in (pd.read_excel('../../data/Port Code Mapping - Hamburg.xlsx')
                                                                         .to_dict('index').items())}

        self.delay_sheet = self.delay_sheet.assign(pol_name=lambda x: x['Port of Loading'],
                                                   pod_name=lambda x: x['Port of discharge']).copy()

        self.delay_sheet.pod_name = self.delay_sheet.pod_name.replace(
            self.port_mapping)

    def prepare(self):
        key = ['pol_name', 'pod_name']
        self.reduced_df = self.delay_sheet.drop_duplicates(key)[
            key].sort_values(key)
        self.reduced_df.dropna(inplace=True)

    def call_api(self):
        def get_schedules(pol_name: str, pod_name: str, i):
            url = "https://api.hamburgsud-line.com/v1/schedules/point-to-point"
            headers = {'x-api-key': 'LJj1A6oZO6OjnqxQLogPaiSC2QrDtT2y'}
            parameters = {
                "searchDate": datetime.today().replace(day=i).strftime('%Y-%m-%d'),
                "from": pol_name,
                "to": pod_name
            }
            return self.session.get(url, headers=headers, params=parameters)

        self.response_jsons = []
        for row in tqdm(self.reduced_df.itertuples(), total=len(self.reduced_df)):
            for i in range(1, 30, 7):
                response_filename = f'Hamburg {row.pol_name}-{row.pod_name} {i:02}.json'
                if response_filename not in os.listdir():
                    response = get_schedules(row.pol_name, row.pod_name, i)
                    self.response_jsons.append(response.json())
                    if len(response.json()):
                        write_json(response.json(), response_filename)
                    time.sleep(random.randint(*self.interval))
                else:
                    with open(response_filename, 'r') as f:
                        self.response_jsons.append(json.load(f))

    def extract(self):
        def get_relevant_fields(response, i):
            def get_vv(data, i, total_legs):
                for j in range(total_legs):
                    if data[i]['leg'][j]['transportMode'] == 'Liner':
                        vessel = data[i]['leg'][j]['vessel']['name']
                        voyage = data[i]['leg'][j]['vessel']['voyage']
                        return voyage, vessel
                return None, None

            total_legs = len(response[i]['leg'])
            return {
                'pol_name': response[i]['leg'][0]['from']['unlocode'],
                'pod_name': response[i]['leg'][-1]['to']['unlocode'],
                'Voyage': get_vv(response, i, total_legs)[0],
                'Vessel': get_vv(response, i, total_legs)[1],
                'updated_etd': response[i]['leg'][0]['expectedDepartureLT'],
                'updated_eta': response[i]['leg'][-1]['expectedArrivalLT']
            }

        self.response_df = pd.DataFrame(([get_relevant_fields(response, i)
                                          for response in self.response_jsons
                                          if isinstance(response, list)
                                          for i in range(len(response))]))
        if len(self.response_df):
            self.response_df = self.response_df.sort_values('updated_eta').drop_duplicates(
                ['pol_name', 'pod_name', 'Voyage', 'Vessel'])

            merge_key = ['pol_name', 'pod_name', 'Vessel', 'Voyage']
            self.delay_sheet = (self.delay_sheet.reset_index().
                                merge(self.response_df[merge_key + ['updated_eta', 'updated_etd']],
                                      on=merge_key, how='left')
                                .set_index('index')
                                .copy())

            self.delay_sheet.updated_eta = pd.to_datetime(
                self.delay_sheet.updated_eta.str[:10])
            self.delay_sheet.updated_etd = pd.to_datetime(
                self.delay_sheet.updated_etd.str[:10])
        else:
            self.response_df = pd.DataFrame({
                'pol_name': [], 'pod_name': [],
                'Vessel': [], 'Voyage': [],
                'updated_eta': [], 'updated_etd': []})


class OOCLExtractor(BaseExtractor):
    def __init__(self, main_delay_sheet: pd.DataFrame, interval: tuple, carrier_mapping: dict):
        self.carrier_mapping = carrier_mapping
        self.interval = interval
        self.session = requests.Session()

        self.delay_sheet = (main_delay_sheet.query(f"`Fwd Agent` in {[k for k,v in self.carrier_mapping.items() if v == 'OOCL']}")
                            .drop(['updated_etd', 'updated_eta', 'No. of days delayed ETD',
                                   'No. of days delayed ETA', 'Reason of Delay'], axis=1)
                            .copy())

        self.port_mapping = {v['Port Code']: v['Port Name'] for k, v in (pd.read_excel('../../data/Port Code Mapping - OOCL.xlsx')
                                                                         .to_dict('index').items())}

        self.delay_sheet = self.delay_sheet.assign(pol=lambda x: x['Port of Loading'],
                                                   pod=lambda x: x['Port of discharge'],
                                                   pol_name=lambda x: x['Port of Loading'].apply(
                                                       lambda y: self.port_mapping.get(y)),
                                                   pod_name=lambda x: x['Port of discharge'].apply(lambda y: self.port_mapping.get(y))).copy()

    def get_location_id(self):
        if 'OOCL portID.json' not in os.listdir():
            def get_id(response):
                results = response.json().get('data').get('results')
                if results:
                    return results[0].get('LocationID')
                return None

            def query_id(port: str):
                url = f"https://www.oocl.com/_catalogs/masterpage/AutoCompleteSailingSchedule.aspx?type=sailingSchedule&Pars={port}"
                headers = {
                    'Sec-Fetch-User': '?1',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Dest': 'document',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36',
                    'Accept-Language': 'en-GB,en;q=0.9',
                    'Upgrade-Insecure-Requests': "1",
                    'Cache-Control': 'max-age=0',
                }
                return self.session.get(url, headers=headers)

            locations = (list(self.delay_sheet.pol_name.unique(
            )) + list(self.delay_sheet.pod_name.unique()))
            self.port_id = {location: get_id(
                query_id(location)) for location in tqdm(locations)}
            if len(self.port_id):
                write_json(self.port_id, 'OOCL portID.json')

            exception_cases = [
                k for k, v in self.port_id.items() if v is None]
            if len(exception_cases):
                write_json(exception_cases, 'oocl_exceptions.txt')
        else:
            read_config(self, 'port_id', 'OOCL portID.json')

    def prepare(self):
        key = ['pol', 'pod', 'pol_name', 'pod_name']
        self.reduced_df = self.delay_sheet.drop_duplicates(key)[
            key].sort_values(key)

        self.reduced_df['pol_code'] = self.reduced_df.pol_name.map(
            self.port_id)
        self.reduced_df['pod_code'] = self.reduced_df.pod_name.map(
            self.port_id)

        self.reduced_df.dropna(inplace=True)

    def call_api(self):
        def get_schedules(pol_locationID: str, pod_locationID: str, pol_name: str, pod_name: str):
            url = f"http://moc.oocl.com/nj_prs_wss/mocss/secured/supportData/nsso/searchHubToHubRoute"
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
                'Origin': 'http://moc.oocl.com',
                'Referer': 'http://moc.oocl.com/nj_prs_wss/',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'en-GB,en;q=0.9',
                'Cookie': 'userSearchHistory=%5B%7B%22origin%22%3A%22Brisbane%2C%20Queensland%2C%20Australia%22%2C%22destination%22%3A%22Bangkok%2C%20Thailand%22%2C%22originId%22%3A%22461802935875046%22%2C%22destinationId%22%3A%22461802935876800%22%2C%22originCountryCode%22%3A%22%22%2C%22destinationCountryCode%22%3A%22%22%2C%22transhipment_PortId%22%3Anull%2C%22transhipment_Port%22%3Anull%2C%22service%22%3Anull%2C%22port_of_LoadId%22%3Anull%2C%22port_of_Load%22%3Anull%2C%22port_of_DischargeId%22%3Anull%2C%22port_of_Discharge%22%3Anull%2C%22origin_Haulage%22%3A%22cy%22%2C%22destination_Haulage%22%3A%22cy%22%2C%22cargo_Nature%22%3A%22dry%22%2C%22sailing%22%3A%22sailing.from%22%2C%22weeks%22%3A%222%22%7D%5D; AcceptCookie=yes; BIGipServeriris4-wss=1597103762.61451.0000; BIGipServerpool_ir4moc=590470802.20480.0000; BIGipServerpool_moc_8011=2022663115.19231.0000'
            }
            first_day = datetime.today().replace(day=1).strftime('%Y-%m-%d')
            payload = {
                "date": f"{first_day}",
                "displayDate": f"{first_day}",
                "transhipment_Port": None,
                "port_of_Load": None,
                "port_of_Discharge": None,
                "sailing": "sailing.from",
                "weeks": "6",
                "transhipment_PortId": None,
                "service": None,
                "port_of_LoadId": None,
                "port_of_DischargeId": None,
                "origin_Haulage": "cy",
                "destination_Haulage": "cy",
                "cargo_Nature": "dry",
                "originId": f"{pol_locationID}",
                "originCountryCode": "",
                "destinationCountryCode": "",
                "destinationId": f"{pod_locationID}",
                "origin": f"{pol_name}",
                "destination": f"{pod_name}",
                "weeksSymbol": "+"
            }

            return self.session.post(url, headers=headers, data=payload)

        self.response_jsons = []
        self.responses = []
        for row in tqdm(self.reduced_df.itertuples(), total=len(self.reduced_df)):
            response_filename = f'OOCL {row.pol}-{row.pod}.json'
            if response_filename not in os.listdir():
                response = get_schedules(int(row.pol_code), int(
                    row.pod_code), row.pol_name, row.pod_name)
                self.response_jsons.append(response.json())
                if len(response.json()):
                    write_json(response.json(), response_filename)
                time.sleep(random.randint(*self.interval))
            else:
                with open(response_filename, 'r') as f:
                    self.response_jsons.append(json.load(f))

    def extract(self):
        def get_relevant_fields(response, i):
            def get_vv_etd(response, i):
                for j in range(len(response['data']['standardRoutes'][i]['Legs'])):
                    if response['data']['standardRoutes'][i]['Legs'][j]['Type'] == "Voyage":
                        voyage = response['data']['standardRoutes'][i]['Legs'][j]['ExternalVoyageReference']
                        vessel = response['data']['standardRoutes'][i]['Legs'][j]['VesselName']
                        etd = response['data']['standardRoutes'][i]['Legs'][j]['FromETDLocalDateTime']['dateStr']
                        return voyage, vessel, etd
                return ""

            def get_eta(response, i):
                for j in range(len(list(reversed(response['data']['standardRoutes'][i]['Legs'])))):
                    if response['data']['standardRoutes'][i]['Legs'][j]['Type'] == "Voyage":
                        eta = response['data']['standardRoutes'][i]['Legs'][j]['ToETALocalDateTime']['dateStr']
                        return eta
                return ""

            return {
                'pol_code': response['data']['standardRoutes'][i]['Legs'][0]['City']['ID'],
                'pod_code': response['data']['standardRoutes'][i]['Legs'][-1]['City']['ID'],
                'Voyage': get_vv_etd(response, i)[0],
                'Vessel': get_vv_etd(response, i)[1],
                'updated_etd': get_vv_etd(response, i)[2],
                'updated_eta': get_eta(response, i)
            }

        self.response_df = pd.DataFrame(([get_relevant_fields(response, i)
                                          for response in self.response_jsons
                                          if len(response)
                                          for i in range(len(response['data']['standardRoutes']))]))

        # Create reverse mapping from port_code to name
        port_id_reversed = {v: k for k, v in self.port_id.items()}

        if len(self.response_df):
            self.response_df['pol_name'] = self.response_df.pol_code.map(
                port_id_reversed)
            self.response_df['pod_name'] = self.response_df.pod_code.map(
                port_id_reversed)

            self.response_df = self.response_df.sort_values('updated_eta').drop_duplicates(
                ['pol_code', 'pod_code', 'Voyage', 'Vessel'])

            merge_key = ['pol_name', 'pod_name', 'Vessel', 'Voyage']
            self.delay_sheet = (self.delay_sheet.reset_index().
                                merge(self.response_df[merge_key + ['updated_eta', 'updated_etd']],
                                      on=merge_key, how='left')
                                .set_index('index')
                                .copy())

            self.delay_sheet.updated_eta = pd.to_datetime(
                self.delay_sheet.updated_eta.str[:8], format='%Y%m%d')
            self.delay_sheet.updated_etd = pd.to_datetime(
                self.delay_sheet.updated_etd.str[:8], format='%Y%m%d')
        else:
            self.response_df = pd.DataFrame({
                'pol_name': [], 'pod_name': [],
                'Vessel': [], 'Voyage': [],
                'updated_eta': [], 'updated_etd': []})


class MSCExtractor(BaseExtractor):
    def __init__(self, main_delay_sheet: pd.DataFrame, interval: tuple, carrier_mapping: dict):
        self.interval = interval
        self.session = requests.Session()

        self.delay_sheet = (main_delay_sheet.loc[main_delay_sheet['Fwd Agent'] == 'MSC']
                            .drop(['updated_etd', 'updated_eta', 'No. of days delayed ETD',
                                   'No. of days delayed ETA', 'Reason of Delay'], axis=1)
                            .copy())

        self.port_mapping = {v['Port Code']: v['Port Name'] for k, v in (pd.read_excel('../../data/Port Code Mapping - MSC.xlsx')
                                                                         .to_dict('index').items())}

        self.delay_sheet = self.delay_sheet.assign(pol=lambda x: x['Port of Loading'],
                                                   pod=lambda x: x['Port of discharge'],
                                                   pol_name=lambda x: x['Port of Loading'].apply(
                                                       lambda y: self.port_mapping.get(y)),
                                                   pod_name=lambda x: x['Port of discharge'].apply(lambda y: self.port_mapping.get(y))).copy()

    def get_location_id(self):
        if 'MSC locationID.json' not in os.listdir():
            def query_id(port: str):
                url = f"https://www.msc.com/api/schedules/autocomplete?q={port}"
                return self.session.get(url)

            def get_id(response):
                if len(response.json()):
                    return response.json()[0].get('id')

            locations = (list(self.delay_sheet.pol_name.unique())
                         + list(self.delay_sheet.pod_name.unique()))
            location_code_responses = {location: query_id(
                location) for location in tqdm(locations)}
            self.port_id = {k: get_id(v)
                            for k, v in location_code_responses.items()}
            if len(self.port_id):
                write_json(self.port_id, 'MSC locationID.json')

            # PODs with no pod_id
            exception_cases = [
                k for k, v in self.port_id.items() if v is None]
            if len(exception_cases):
                write_json(exception_cases, 'MSC exceptions.txt')
        else:
            read_config(self, 'port_id', 'MSC locationID.json')

    def prepare(self):
        key = ['pol', 'pod', 'pol_name', 'pod_name']
        self.reduced_df = self.delay_sheet.drop_duplicates(key)[
            key].sort_values(key)

        self.reduced_df['pol_code'] = self.reduced_df.pol_name.map(
            self.port_id)
        self.reduced_df['pod_code'] = self.reduced_df.pod_name.map(
            self.port_id)

        self.reduced_df.dropna(inplace=True)

    def call_api(self):
        def get_schedules(pol_code: str, pod_code: str):
            etd = datetime.today().strftime('%Y-%m-%d')
            url = f"https://www.msc.com/api/schedules/search?WeeksOut=8&DirectRoutes=false&Date={etd}&From={pol_code}&To={pod_code}"
            headers = {
                'Accept': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36',
                'Content-Type': 'application/json',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Dest': 'empty',
                'Referer': 'https://www.msc.com/search-schedules',
                'Accept-Language': 'en-GB,en;q=0.9',
                'Cookie': 'CMSPreferredCulture=en-GB; ASP.NET_SessionId=tht5lkut0asln2goiskoagfe; UrlReferrer=https://www.google.com/; CurrentContact=8b0b2fea-705b-4a4f-b8bf-bb1cd6c982bc; MSCAgencyId=115867; BIGipServerkentico.app~kentico_pool=439883018.20480.0000; _ga=GA1.2.1736073830.1597290148; _gid=GA1.2.1289141279.1597290148; _gcl_au=1.1.345060449.1597290148; __hstc=100935006.13bb76c8a78a8d0a203a993ffef3a3f6.1597290148282.1597290148282.1597290148282.1; hubspotutk=13bb76c8a78a8d0a203a993ffef3a3f6; __hssrc=1; _ym_uid=15972901491036911544; _ym_d=1597290149; _ym_isad=1; newsletter-signup-cookie=temp-hidden; _hjid=3e183004-f562-4048-8b60-daccdf9c187c; _hjUserAttributesHash=2c3b62a0e1cd48bdfd4d01b922060e19; _hjCachedUserAttributes={"attributes":{"mscAgencyId":"115867"},"userId":null}; OptanonAlertBoxClosed=2020-08-13T03:42:45.080Z; CMSCookieLevel=200; VisitorStatus=11062214903; TS0142aef9=0192b4b6225179b1baa3b4d270b71a4eee782a0192338173beabaa471f306c2a13fe854bf6a7ac08ac21924991864aa7728c54559023beabd273d82285d5f943202adb58da417d61813232e89b240828c090f890c6a74dc4adfec38513d13447be4b5b4404d69f964987b7917f731b858f0c9880a139994b98397c4aeb5bd60b0d0e38ec9e5f3c97b13fb184b4e068506e6086954f8a515f2b7239d2e5c1b9c70f61ca74f736355c58648a6036e9b5d06412389ac41221c5cb740df99c84dc2bfef4a530dbc5e2577c189212eebac723d9ee9f98030f4bc6ca7d824ab313ae5fdd1eaa9886; OptanonConsent=isIABGlobal=false&datestamp=Thu+Aug+13+2020+11%3A43%3A36+GMT%2B0800+(Singapore+Standard+Time)&version=5.9.0&landingPath=NotLandingPage&groups=1%3A1%2C2%3A1%2C3%3A1%2C4%3A1%2C0_53017%3A1%2C0_53020%3A1%2C0_53018%3A1%2C0_53019%3A1%2C101%3A1&AwaitingReconsent=false'
            }
            return self.session.get(url, headers=headers)

        self.response_jsons = []

        for row in tqdm(self.reduced_df.itertuples(), total=len(self.reduced_df)):
            response_filename = f'MSC {row.pol}-{row.pod}.json'
            if response_filename not in os.listdir():
                response = get_schedules(int(
                    row.pol_code), int(row.pod_code))
                self.response_jsons.append(response.json())
                if len(response.json()):
                    write_json(response.json(), response_filename)
                time.sleep(random.randint(*self.interval))
            else:
                with open(response_filename, 'r') as f:
                    self.response_jsons.append(json.load(f))

    def extract(self):
        def get_relevant_fields(response, i):
            return {
                'pol_code': response[0]['Sailings'][i]['PortOfLoadId'],
                'pod_code': response[0]['Sailings'][i]['PortOfDischargeId'],
                'Voyage': response[0]['Sailings'][i]['VoyageNum'],
                'Vessel': response[0]['Sailings'][i]['VesselName'],
                'updated_etd': response[0]['Sailings'][i]['NextETD'],
                'updated_eta': response[0]['Sailings'][i]['ArrivalDate']
            }

        self.response_df = pd.DataFrame(([get_relevant_fields(response, i)
                                          for response in self.response_jsons
                                          if len(response)
                                          for i in range(len(response[0]['Sailings']))]))

        # Create reverse mapping from port_code to name
        port_id_reversed = {v: k for k, v in self.port_id.items()}

        if len(self.response_df):
            self.response_df['pol_name'] = self.response_df.pol_code.map(
                port_id_reversed)
            self.response_df['pod_name'] = self.response_df.pod_code.map(
                port_id_reversed)

            merge_key = ['pol_name', 'pod_name', 'Vessel', 'Voyage']
            self.delay_sheet = (self.delay_sheet.reset_index().
                                merge(self.response_df[merge_key + ['updated_eta', 'updated_etd']],
                                      on=merge_key, how='left')
                                .set_index('index')
                                .copy())
            self.delay_sheet.updated_eta = pd.to_datetime(
                self.delay_sheet.updated_eta.str[:10])
            self.delay_sheet.updated_etd = pd.to_datetime(
                self.delay_sheet.updated_etd.str[:10])
        else:
            self.response_df = pd.DataFrame({
                'pol_name': [], 'pod_name': [],
                'Vessel': [], 'Voyage': [],
                'updated_eta': [], 'updated_etd': []})


class G2Extractor:
    """
    Extracts information from the G2 Schedule Excel file by using pd.apply to the delay_sheet.

    Methods
    -------
    extract:
        Extracts data from the Excel dataframe, using two helper methods get_updated_eta and get_updated_etd.

    get_updated_eta, get_updated_etd:
        Helper methods to extract the updated_eta and updated_etd given a row in the delay_sheet.
    """

    def __init__(self, g2_file: str, main_delay_sheet: pd.DataFrame, config: dict):
        self.schedule = pd.read_excel(
            Path('../../' + g2_file), skiprows=config.get('g2_whitespace_rows', 9), index_col='Unnamed: 0')
        self.delay_sheet = main_delay_sheet.query(
            f"`Fwd Agent` in {['G2OCEAN']}").copy()
        self.port_mapping = {v['Port Code']: v['Port Name'] for k, v in (pd.read_excel('../../data/Port Code Mapping - G2.xlsx')
                                                                         .to_dict('index').items())}

    def get_updated_etd(self, row):
        try:
            # column_index_etd is the column number that points to the ETD
            column_index_etd = np.argwhere(
                self.schedule.columns.str.contains(row['Vessel']))[0][0] + 1
        except IndexError:
            return np.nan
        etd = self.schedule.loc[self.schedule.index == self.port_mapping.get(
            row['Port of Loading'])].iloc[:, column_index_etd][0]
        if etd is '-':
            return np.nan
        return etd

    def get_updated_eta(self, row):
        try:
            # column_index_eta is the column number that points to the ETA
            column_index_eta = np.argwhere(
                self.schedule.columns.str.contains(row['Vessel']))[0][0]
        except IndexError:
            return np.nan
        eta = self.schedule.loc[self.schedule.index == self.port_mapping.get(
            row['Port of discharge'])].iloc[:, column_index_eta][0]
        if eta is '-':
            return np.nan
        return eta

    def extract(self):
        """
        Extracts data from the Excel dataframe, using two helper methods get_updated_eta and get_updated_etd.
        """
        self.delay_sheet['updated_etd'] = self.delay_sheet.apply(
            self.get_updated_etd, axis=1)
        self.delay_sheet['updated_eta'] = self.delay_sheet.apply(
            self.get_updated_eta, axis=1)


class DelayReport:
    """
    Main delay report class that loads configurations that are shared across Extractors and runs the Extractors.

    Methods
    -------
    run:
        Runs the corresponding extraction by instantiating a relevant Extractor class.

    calculate_deltas:
        Calculates the deltas from the updated delay_sheet.

    output:
        Write the final delay report Excel file to disk.
    """

    def __init__(self):
        self.config = {}
        self.carrier_mapping = {}
        self.one_extractor = None
        self.cosco_extractor = None
        self.cma_extractor = None
        self.anl_extractor = None
        self.hamburg_extractor = None
        self.oocl_extractor = None
        self.msc_extractor = None
        self.g2_extractor = None
        self.saved_file = ''

        # Read configurations
        self.config = {v['Field']: v['Value'] for k, v in pd.read_excel(
            'data/Configurations.xlsx').to_dict('index').items()}

        # Used to map Fwd Agent column to the respective carrier portals
        self.carrier_mapping = {v['Fwd Agent']: v['Carrier'] for k, v in pd.read_excel(
            'data/Carrier Mapping.xlsx').to_dict('index').items()}

        # Random interval in seconds
        self.interval = (self.config.get('randomiser_lower_interval'),
                         self.config.get('randomiser_upper_interval'))

        # Read the vessel delay tracking file
        self.xl = pd.ExcelFile(self.config['delay_filename'])
        # today_date = datetime.now().strftime('%d.%m.%Y')
        # if today_date not in self.xl.sheet_names:
        #     raise Exception(
        # f"The script cannot find today's date ({today_date}) in the Vessel Delay Tracking.xlsx file provided. Please check that the sheets are correctly named - the script will only operate on a sheet with today's date.")

        # Assemble the final dataframe to update
        self.main_delay_sheet = self.xl.parse(pd.to_datetime(self.xl.sheet_names,
                                                             errors='coerce',
                                                             format='%d.%m.%Y').max().date().strftime('%d.%m.%Y'),
                                              parse_dates=True).copy()

        # If our current Excel file already has updated_eta or updated_etd columns, we drop them
        new_columns = ['updated_etd', 'updated_eta', 'No. of days delayed ETD',
                       'No. of days delayed ETA', 'Reason of Delay']
        for updated_column in new_columns:
            if updated_column in self.main_delay_sheet.columns:
                self.main_delay_sheet.drop(
                    updated_column, axis=1, inplace=True)

        # Add new columns to the right side of the dataframe
        self.main_delay_sheet[new_columns] = pd.DataFrame(
            [[pd.NaT for i in range(4)] + [np.nan]])
        self.clean_delay_sheet = self.main_delay_sheet.copy()

        # Today's directory
        today_path = Path('responses/' + datetime.now().strftime('%Y-%m-%d'))
        try:
            os.makedirs(today_path)
        except FileExistsError:
            pass
        os.chdir(today_path)

    def run_g2(self):
        if self.config.get('run_g2'):
            print('Extracting G2Schedules from file...')
            self.g2_extractor = G2Extractor(self.config.get(
                'g2_filename'), self.clean_delay_sheet, self.config)
            self.g2_extractor.extract()
            self.main_delay_sheet.update(self.g2_extractor.delay_sheet)

    def run(self, carrier_name: str, extractor_name: str, extractor_class: BaseExtractor):
        if self.config.get(f'run_{carrier_name.lower()}'):
            setattr(self, extractor_name, extractor_class(
                self.clean_delay_sheet, self.interval, self.carrier_mapping))
            getattr(self, extractor_name).get_location_id()
            getattr(self, extractor_name).prepare()
            print(
                f'Extracting {carrier_name} information...')
            getattr(self, extractor_name).call_api()
            getattr(self, extractor_name).extract()
            self.main_delay_sheet.update(
                getattr(self, extractor_name).delay_sheet)

    def calculate_deltas(self):
        date_columns = ['ETD Date', 'Disport ETA', 'BOL Date',
                        'updated_etd', 'updated_eta']

        # Convert strings (by default SAP download is parsed as string) to dates
        for column in date_columns[:3]:
            self.main_delay_sheet[column] = pd.to_datetime(
                self.main_delay_sheet[column], format='%d/%m/%Y')

        # Calculate the deltas
        self.main_delay_sheet['No. of days delayed ETD'] = (self.main_delay_sheet.updated_etd
                                                            - self.main_delay_sheet['ETD Date']).dt.days
        self.main_delay_sheet['No. of days delayed ETA'] = (self.main_delay_sheet.updated_eta
                                                            - self.main_delay_sheet['Disport ETA']).dt.days

        # Format the dates correctly
        for column in date_columns:
            self.main_delay_sheet[column] = self.main_delay_sheet[column].dt.strftime(
                '%d/%m/%Y')

    def mask_bol(self):
        # Masks those lines with existing BOL dates; since we no longer track these ships which have left site
        if self.config.get('mask_date_if_bol_present'):
            self.main_delay_sheet.loc[~self.main_delay_sheet['BOL Date'].isnull(
            ), 'updated_etd'] = self.main_delay_sheet['ETD Date']
            self.main_delay_sheet.loc[~self.main_delay_sheet['BOL Date'].isnull(
            ), 'updated_eta'] = self.main_delay_sheet['Disport ETA']
            self.main_delay_sheet.loc[~self.main_delay_sheet['BOL Date'].isnull(
            ), 'No. of days delayed ETD'] = 0
            self.main_delay_sheet.loc[~self.main_delay_sheet['BOL Date'].isnull(
            ), 'No. of days delayed ETA'] = 0

    def output(self):
        self.saved_file = f"Vessel Delay Tracking - {datetime.today().strftime('%d.%m.%Y')}.xlsx"
        self.main_delay_sheet.to_excel(
            Path('../../' + self.saved_file), index=False)
        os.startfile(Path('../../' + self.saved_file))


# Utility functions
def write_json(response: dict, output_file: str):
    with open(output_file, 'w') as w:
        json.dump(response, w, indent=2)


def read_config(instance: object, attr_name: str, path_to_config: str):
    with open(path_to_config, "r") as f:
        setattr(instance, attr_name, json.load(f))


if __name__ == "__main__":
    delay_report = DelayReport()
    delay_report.run('ONE', 'one_extractor', ONEExtractor)
    delay_report.run('COSCO', 'cosco_extractor', COSCOExtractor)
    delay_report.run('CMA', 'cma_extractor', CMAExtractor)
    delay_report.run('ANL', 'anl_extractor', ANLExtractor)
    delay_report.run('Hamburg', 'hamburg_extractor', HamburgExtractor)
    delay_report.run('OOCL', 'oocl_extractor', OOCLExtractor)
    delay_report.run('MSC', 'msc_extractor', MSCExtractor)
    delay_report.run_g2()
    delay_report.calculate_deltas()
    delay_report.mask_bol()
    delay_report.output()

    print(f'{delay_report.saved_file} has been generated in the current directory. You may close this window.')
    input()
