# Imports
import pkg_resources.py2_warn
import pandas as pd
import numpy as np
import random
import logging
import os
import json
import requests

from pathlib import Path
from datetime import datetime


class G2Extractor:
    def __init__(self, g2_file, xl, carrier_mapping):
        self.schedule = pd.read_excel(
            g2_file, skiprows=9, index_col='Unnamed: 0')
        self.delay_sheet = (xl.parse(pd.to_datetime(xl.sheet_names,
                                                    errors='coerce',
                                                    format='%d.%m.%Y').max().date().strftime('%d.%m.%Y'),
                                     parse_dates=True)
                            .query(f"`Fwd Agent` in {['G2OCEAN']}")
                            .replace({'Fwd Agent': carrier_mapping}))
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
        self.xl = pd.ExcelFile('Vessel Delay Tracking.xlsx')
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
            self.msc_extractor = MSCExtractor(self.xl, self.carrier_mapping)
            self.msc_extractor.extract()

    def run_g2(self):
        if self.config.get('run_g2'):
            self.g2_extractor = G2Extractor(self.config.get(
                'g2_filename'), self.xl, self.carrier_mapping)
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
        os.startfile(saved_file)


# Delay report skeleton
delay_report = DelayReport()
delay_report.run_bs()
delay_report.run_msc()
delay_report.run_g2()
delay_report.assemble()
delay_report.output()

print(f'{delay_report.saved_file} has been generated in the current directory. You may close this window.')
input()
