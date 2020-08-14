# Imports
import pkg_resources.py2_warn
import pandas as pd
import numpy as np
import random
import logging
import os
import json
import requests
import time

from tqdm.auto import tqdm
from pathlib import Path
from datetime import datetime


class MSCExtractor:
    def __init__(self, xl):
        # Get the MSC delay sheet
        self.delay_sheet = (xl.parse(pd.to_datetime(xl.sheet_names,
                                                    errors='coerce',
                                                    format='%d.%m.%Y').max().date().strftime('%d.%m.%Y'),
                                     parse_dates=True)
                            .query(f"`Fwd Agent` in {['MSC']}"))

        # Get the MSC-specific port names from the UNLOCODEs
        self.port_mapping = {v['Port Code']: v['MSC Port Name'] for k, v in (pd.read_excel('data/MSC Port Code Mapping.xlsx')
                                                                             .to_dict('index').items())}

        # Get port name
        self.delay_sheet = self.delay_sheet.assign(pol_name=lambda x: x['Port of Loading'].apply(lambda y: self.port_mapping.get(y)),
                                                   pod_name=lambda x: x['Port of discharge'].apply(lambda y: self.port_mapping.get(y)))

        # If our current Excel file already has an updated_eta or updated_etd columns, we drop them
        for updated_column in ['updated_eta', 'updated_etd']:
            if updated_column in self.delay_sheet.columns:
                self.delay_sheet.drop(updated_column, axis=1, inplace=True)

        self.session = requests.Session()

    def get_countryID(self):
        def query_id(port):
            url = f"https://www.msc.com/api/schedules/autocomplete?q={port}"
            return self.session.request("GET", url)

        def get_id(response):
            if len(response.json()):
                return response.json()[0].get('id')

        msc_locations = list(self.delay_sheet.pol_name.unique()) + \
            list(self.delay_sheet.pod_name.unique())
        location_code_responses = {location: query_id(
            location) for location in tqdm(msc_locations)}
        self.msc_port_id = {k: get_id(v)
                            for k, v in location_code_responses.items()}

        # PODs with no pod_id
        exception_cases = [k for k, v in self.msc_port_id.items() if v is None]
        with open('msc_exceptions.txt', 'w') as msc_exceptions:
            json.dump(exception_cases, msc_exceptions)

    def prepare(self):
        # Further filter by POL-Vessel-Voyage to get ETD, POD-Vessel-Voyage to get ETA
        key = ['pol_name', 'pod_name']
        reduced_df = self.delay_sheet.drop_duplicates(
            key)[key].sort_values(key)

        reduced_df['pol_code'] = reduced_df.pol_name.map(self.msc_port_id)
        reduced_df['pod_code'] = reduced_df.pod_name.map(self.msc_port_id)

        # Unable to handle those with no pod_id in MSC; dropping these lines
        reduced_df.dropna(inplace=True)
        self.reduced_df = reduced_df.copy()

    def call_api(self):
        def get_schedules(etd, pol, pod):
            url = f"https://www.msc.com/api/schedules/search?WeeksOut=8&DirectRoutes=false&Date={etd}&From={pol}&To={pod}"
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
            response = self.session.get(url, headers=headers)
            return response

        self.responses = []
        reduced_df = self.reduced_df
        for row in tqdm(reduced_df.itertuples(), total=len(reduced_df)):
            first_day = datetime.today().replace(day=1).strftime('%Y-%m-%d')
            response = get_schedules(first_day, int(
                row.pol_code), int(row.pod_code))
            self.responses.append(response)
            time.sleep(random.randint(2, 5))

    def assemble_data(self):
        def get_relevant_fields(response, i):
            return {
                'pol_code': response.json()[0]['Sailings'][i]['PortOfLoadId'],
                'pod_code': response.json()[0]['Sailings'][i]['PortOfDischargeId'],
                'Voyage': response.json()[0]['Sailings'][i]['VoyageNum'],
                'Vessel': response.json()[0]['Sailings'][i]['VesselName'],
                'updated_etd': response.json()[0]['Sailings'][i]['NextETD'],
                'updated_eta': response.json()[0]['Sailings'][i]['ArrivalDate']
            }

        response_df = pd.DataFrame(([get_relevant_fields(response, i)
                                     for response in self.responses
                                     for i in range(len(response.json()[0]['Sailings']))
                                     if len(response.json())
                                     ]))

        # Create reverse mapping from port_code to name
        msc_port_id_reversed = {v: k for k, v in self.msc_port_id.items()}

        # Add additional columns to response_df
        response_df['pol_name'] = response_df.pol_code.map(
            msc_port_id_reversed)
        response_df['pod_name'] = response_df.pod_code.map(
            msc_port_id_reversed)

        # Merge results back to original dataframe
        merge_key = ['pol_name', 'pod_name', 'Vessel', 'Voyage']
        self.delay_sheet = self.delay_sheet.merge(response_df[merge_key + ['updated_eta', 'updated_etd']],
                                                  on=merge_key, how='left')

        self.delay_sheet.updated_eta = pd.to_datetime(
            self.delay_sheet.updated_eta.str[:10])
        self.delay_sheet.updated_etd = pd.to_datetime(
            self.delay_sheet.updated_etd.str[:10])


class G2Extractor:
    def __init__(self, g2_file, xl):
        self.schedule = pd.read_excel(
            g2_file, skiprows=9, index_col='Unnamed: 0')
        self.delay_sheet = (xl.parse(pd.to_datetime(xl.sheet_names,
                                                    errors='coerce',
                                                    format='%d.%m.%Y').max().date().strftime('%d.%m.%Y'),
                                     parse_dates=True)
                            .query(f"`Fwd Agent` in {['G2OCEAN']}"))
        self.g2_port_map = {
            'AUPTJ': 'Portland',
            'AUNTL': 'Newcastle',
            'AUGLT': 'Gladstone',
            'NZTWI': 'Bluff',
            'TWKHH': 'Kaohsiung',
            'KRINC': 'Inchon',
            'KRPUS': 'Busan',
            'JPYOK': 'Yokohama',
            'JPNGO': 'Nagoya',
            'JPOSA': 'Osaka',
            'JPTOY': 'Toyama',
            'JPIHA': 'Niihama',
            'HKHKG': 'Hong Kong',
            'CNSHA': 'Shanghai'
        }

    def get_updated_etd(self, row):
        try:
            # column_index_etd is the column number that points to the ETD
            column_index_etd = np.argwhere(
                self.schedule.columns.str.contains(row['Vessel']))[0][0] + 1
        except IndexError:
            return np.nan
        return self.schedule.loc[self.schedule.index == self.g2_port_map.get(row['Port of Loading'])].iloc[:, column_index_etd][0]

    def get_updated_eta(self, row):
        try:
            # column_index_eta is the column number that points to the ETA
            column_index_eta = np.argwhere(
                self.schedule.columns.str.contains(row['Vessel']))[0][0]
        except IndexError:
            return np.nan
        return self.schedule.loc[self.schedule.index == self.g2_port_map.get(row['Port of discharge'])].iloc[:, column_index_eta][0]

    def extract(self):
        self.delay_sheet['updated_etd'] = self.delay_sheet.apply(
            self.get_updated_etd, axis=1)
        self.delay_sheet['updated_eta'] = self.delay_sheet.apply(
            self.get_updated_eta, axis=1)


class DelayReport:
    def __init__(self):
        # Read configuration file
        with open("data/config.json", "r") as f:
            self.config = json.load(f)

        # Used to map carrier names to the ones BigSchedules uses and supports
        with open("data/carrier_mapping.json", "r") as f:
            self.carrier_mapping = json.load(f)

        # BigSchedules login
        with open("data/bigschedules_login.json", "r") as f:
            self.bs_login = json.load(f)

        # Prepare base information
        # UNLOCODE to port name mapping
        self.port_mapping = (
            pd.concat([pd.read_csv(p, usecols=[1, 2, 4, 5], engine='python', names=[
                      'country', 'port', 'name', 'subdiv']) for p in Path('data').glob("*UNLOCODE CodeListPart*")])
            .query('port == port')
            .assign(uncode=lambda x: x.country.str.cat(x.port),
                    full_name=lambda x: np.where(x.subdiv.notnull(), x.name.str.cat(x.subdiv, sep=", "), x.name))
            .drop_duplicates('uncode')
            .set_index('uncode')
            .to_dict('index'))

        # Read the vessel delay tracking file
        self.xl = pd.ExcelFile(self.config['delay_filename'])
        # today_date = datetime.now().strftime('%d.%m.%Y')
        # if today_date not in self.xl.sheet_names:
        #     raise Exception(
        #         f"The script cannot find today's date ({today_date}) in the Vessel Delay Tracking.xlsx file provided. Please check that the sheets are correctly named - the script will only operate on a sheet with today's date.")

    def run_bs(self):
        if self.config.get('run_bs'):
            bs_extractor = BSExtractor()
            bs_extractor.extract()

    def run_msc(self):
        if self.config.get('run_msc'):
            self.msc_extractor = MSCExtractor(self.xl)
            self.msc_extractor.get_countryID()
            self.msc_extractor.prepare()
            self.msc_extractor.call_api()
            self.msc_extractor.assemble_data()

    def run_g2(self):
        if self.config.get('run_g2'):
            self.g2_extractor = G2Extractor(self.config.get(
                'g2_filename'), self.xl)
            self.g2_extractor.extract()

    def assemble(self):
        # Assemble the final dataframe to update
        main_delay_sheet = self.xl.parse()

        # Add new columns to the right side of the dataframe
        new_columns = ['updated_etd', 'updated_eta', 'No. of days delayed ETD',
                       'No. of days delayed ETA', 'Reason of Delay']
        main_delay_sheet[new_columns] = pd.DataFrame(
            [[pd.NaT for i in range(4)] + [np.nan]])

        if self.config.get('run_bs'):
            main_delay_sheet.update(self.bs_extractor.delay_sheet)

        if self.config.get('run_msc'):
            main_delay_sheet.update(self.msc_extractor.delay_sheet)

        if self.config.get('run_g2'):
            main_delay_sheet.update(self.g2_extractor.delay_sheet)

        # Calculate the deltas
        main_delay_sheet['No. of days delayed ETD'] = (main_delay_sheet.updated_etd
                                                       - pd.to_datetime(main_delay_sheet['ETD Date'])).dt.days
        main_delay_sheet['No. of days delayed ETA'] = (main_delay_sheet.updated_eta
                                                       - pd.to_datetime(main_delay_sheet['Disport ETA'])).dt.days

        # Format the dates correctly via strftime
        date_columns = ['ETD Date', 'Disport ETA',
                        'updated_etd', 'updated_eta']
        for column in date_columns:
            main_delay_sheet[column] = main_delay_sheet[column].dt.strftime(
                '%d/%m/%Y')
        self.main_delay_sheet = main_delay_sheet.copy()

    def output(self):
        # Output the excel file
        self.saved_file = f"Vessel Delay Tracking - {datetime.today().strftime('%d.%m.%Y')}.xlsx"
        self.main_delay_sheet.to_excel(self.saved_file, index=False)
        os.startfile(self.saved_file)


# Delay report skeleton
delay_report = DelayReport()
delay_report.run_bs()
delay_report.run_msc()
delay_report.run_g2()
delay_report.assemble()
delay_report.output()

print(f'{delay_report.saved_file} has been generated in the current directory. You may close this window.')
input()
