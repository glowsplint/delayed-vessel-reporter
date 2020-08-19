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
from datetime import datetime


class MSCExtractor:
    """
    Extracts information from the MSC Portal.

    Methods
    -------
    get_countryID:
        Gets the countryID mappings via the CountryID API in order to use the Search Schedules API.

    prepare:
        A single query to the Search Schedules API can provide information to multiple lines on the delay_sheet.
        Further filters self.delay_sheet to a smaller list of searches needed to fulfill all the lines on the
            delay_sheet. This reduces the total number of calls made to the Search Schedules API and prevents
            duplication of API calls.

    call_api:
        Makes calls to the Search Schedules API, using information from the prepare method as parameters in the
        API request. Also saves the API responses into a subdirectory "responses/<today_date>".

    extract:
        Extracts information from the JSON responses from the call_api method and assembles the final dataframe.
    """

    def __init__(self, main_delay_sheet: pd.DataFrame, interval: tuple):
        # Get the MSC delay sheet
        self.delay_sheet = (main_delay_sheet.loc[main_delay_sheet['Fwd Agent'] == 'MSC']
                            .drop(['updated_etd', 'updated_eta', 'No. of days delayed ETD',
                                   'No. of days delayed ETA', 'Reason of Delay'], axis=1)
                            .copy())

        # Get the MSC-specific port names from the UNLOCODEs
        self.port_mapping = {v['Port Code']: v['MSC Port Name'] for k, v in (pd.read_excel('../../data/MSC Port Code Mapping.xlsx')
                                                                             .to_dict('index').items())}

        # Get port name
        self.delay_sheet = self.delay_sheet.assign(pol_name=lambda x: x['Port of Loading'].apply(lambda y: self.port_mapping.get(y)),
                                                   pod_name=lambda x: x['Port of discharge'].apply(lambda y: self.port_mapping.get(y))).copy()

        self.interval = interval
        self.session = requests.Session()

    def get_countryID(self):
        """
        Checks if the query for countryID has been done today.
        If it has been done, skips it and uses the existing countryID JSON file.
        Otherwise, queries the countryID API.

        This API call does not require a cookie.
        """
        if 'countryID.json' not in os.listdir():
            def query_id(port: str):
                url = f"https://www.msc.com/api/schedules/autocomplete?q={port}"
                return self.session.get(url)

            def get_id(response):
                if len(response.json()):
                    return response.json()[0].get('id')

            msc_locations = list(self.delay_sheet.pol_name.unique(
            )) + list(self.delay_sheet.pod_name.unique())
            location_code_responses = {location: query_id(
                location) for location in tqdm(msc_locations)}
            self.msc_port_id = {k: get_id(v)
                                for k, v in location_code_responses.items()}
            write_json(self.msc_port_id, 'countryID.json')

            # PODs with no pod_id
            exception_cases = [
                k for k, v in self.msc_port_id.items() if v is None]
            write_json(exception_cases, 'msc_exceptions.txt')
        else:
            read_config(self, 'msc_port_id', 'countryID.json')

    def prepare(self):
        """
        Further filters self.delay_sheet to a smaller list of searches needed to fulfill all the lines on the
            delay_sheet. Also maps the UNLOCODE to their respective countryID.
        """
        # Further filter by POL-Vessel-Voyage to get ETD, POD-Vessel-Voyage to get ETA
        key = ['pol_name', 'pod_name']
        self.reduced_df = self.delay_sheet.drop_duplicates(key)[
            key].sort_values(key)

        self.reduced_df['pol_code'] = self.reduced_df.pol_name.map(
            self.msc_port_id)
        self.reduced_df['pod_code'] = self.reduced_df.pod_name.map(
            self.msc_port_id)

        # Unable to handle those with no pod_id in MSC; dropping these lines
        self.reduced_df.dropna(inplace=True)

    def call_api(self):
        """
        Makes calls to the Search Schedules API, using information from the prepare method as parameters in the
        API request. Also saves the API responses into a subdirectory "responses/<today_date>".
        """
        def get_schedules(etd: str, pol: str, pod: str):
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

        self.response_jsons = []
        first_day = datetime.today().replace(day=1).strftime('%Y-%m-%d')

        for row in tqdm(self.reduced_df.itertuples(), total=len(self.reduced_df)):
            response_filename = f'MSC {int(row.pol_code)}-{int(row.pod_code)}.json'
            if response_filename not in os.listdir():
                response = get_schedules(first_day, int(
                    row.pol_code), int(row.pod_code))
                self.response_jsons.append(response.json())
                write_json(response.json(), response_filename)
                time.sleep(random.randint(*self.interval))
            else:
                with open(response_filename, 'r') as f:
                    self.response_jsons.append(json.load(f))

    def extract(self):
        """
        Extracts information from the JSON responses from the call_api method and assembles the final dataframe.
        """
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
                                          for i in range(len(response[0]['Sailings']))
                                          if len(response)
                                          ]))

        # Create reverse mapping from port_code to name
        msc_port_id_reversed = {v: k for k, v in self.msc_port_id.items()}

        # Add additional columns to response_df
        self.response_df['pol_name'] = self.response_df.pol_code.map(
            msc_port_id_reversed)
        self.response_df['pod_name'] = self.response_df.pod_code.map(
            msc_port_id_reversed)

        # Merge results back to original dataframe
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

    def __init__(self, g2_file: str, main_delay_sheet: pd.DataFrame):
        self.schedule = pd.read_excel(
            Path('../../' + g2_file), skiprows=9, index_col='Unnamed: 0')
        self.delay_sheet = main_delay_sheet.query(
            f"`Fwd Agent` in {['G2OCEAN']}").copy()
        self.port_mapping = {v['Port Code']: v['G2 Port Name'] for k, v in (pd.read_excel('../../data/G2 Port Code Mapping.xlsx')
                                                                            .to_dict('index').items())}

    def get_updated_etd(self, row):
        try:
            # column_index_etd is the column number that points to the ETD
            column_index_etd = np.argwhere(
                self.schedule.columns.str.contains(row['Vessel']))[0][0] + 1
        except IndexError:
            return np.nan
        return self.schedule.loc[self.schedule.index == self.port_mapping.get(row['Port of Loading'])].iloc[:, column_index_etd][0]

    def get_updated_eta(self, row):
        try:
            # column_index_eta is the column number that points to the ETA
            column_index_eta = np.argwhere(
                self.schedule.columns.str.contains(row['Vessel']))[0][0]
        except IndexError:
            return np.nan
        return self.schedule.loc[self.schedule.index == self.port_mapping.get(row['Port of discharge'])].iloc[:, column_index_eta][0]

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
    run_bs, run_msc, run_g2:
        Runs the corresponding extraction by instantiating a relevant Extractor class.

    calculate_deltas:
        Calculates the deltas from the updated delay_sheet.

    output:
        Write the final delay report Excel file to disk.
    """

    def __init__(self):
        # Read configurations
        read_config(self, 'config', 'data/config.json')

        # Used to map carrier names to the ones BigSchedules uses and supports
        read_config(self, 'carrier_mapping', 'data/carrier_mapping.json')

        # Random interval in seconds
        self.interval = (self.config.get('randomiser_lower_interval'),
                         self.config.get('randomiser_upper_interval'))

        # Prepare UNLOCODE to port name mapping
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

        # Assemble the final dataframe to update
        self.main_delay_sheet = self.xl.parse(pd.to_datetime(self.xl.sheet_names,
                                                             errors='coerce',
                                                             format='%d.%m.%Y').max().date().strftime('%d.%m.%Y'),
                                              parse_dates=True).copy()

        # If our current Excel file already has an updated_eta or updated_etd columns, we drop them
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

    def run_bs(self):
        if self.config.get('run_bs'):
            bs_extractor = BSExtractor(self.clean_delay_sheet, self.interval)
            bs_extractor.extract()
            self.main_delay_sheet.update(self.bs_extractor.delay_sheet)

    def run_msc(self):
        if self.config.get('run_msc'):
            self.msc_extractor = MSCExtractor(
                self.clean_delay_sheet, self.interval)
            self.msc_extractor.get_countryID()
            self.msc_extractor.prepare()
            self.msc_extractor.call_api()
            self.msc_extractor.extract()
            self.main_delay_sheet.update(self.msc_extractor.delay_sheet)

    def run_g2(self):
        if self.config.get('run_g2'):
            self.g2_extractor = G2Extractor(self.config.get(
                'g2_filename'), self.clean_delay_sheet)
            self.g2_extractor.extract()
            self.main_delay_sheet.update(self.g2_extractor.delay_sheet)

    def calculate_deltas(self):
        # Calculate the deltas
        self.main_delay_sheet['No. of days delayed ETD'] = (self.main_delay_sheet.updated_etd
                                                            - pd.to_datetime(self.main_delay_sheet['ETD Date'])).dt.days
        self.main_delay_sheet['No. of days delayed ETA'] = (self.main_delay_sheet.updated_eta
                                                            - pd.to_datetime(self.main_delay_sheet['Disport ETA'])).dt.days

        # Format the dates correctly via strftime
        date_columns = ['ETD Date', 'Disport ETA',
                        'updated_etd', 'updated_eta']
        for column in date_columns:
            self.main_delay_sheet[column] = self.main_delay_sheet[column].dt.strftime(
                '%d/%m/%Y')

    def output(self):
        # Output the excel file
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
    # Delay report skeleton
    delay_report = DelayReport()
    delay_report.run_bs()
    delay_report.run_msc()
    delay_report.run_g2()
    delay_report.calculate_deltas()
    delay_report.output()

    print(f'{delay_report.saved_file} has been generated in the current directory. You may close this window.')
    input()
