

import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import List

import dpkt
import geopandas as gpd
import pandas
from matplotlib import pyplot as plt

sys.path.append('../')  # FIXME

PATH_TO_CHROME = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
TSHARK = '/Applications/Wireshark.app/Contents/MacOS/tshark'
TMP_CHROME_PROFILE = 'src/DetermineTEDSpecialists/__TMP_CHROME_PROFILE'
TMP_CHROME_SSL_LOG = 'src/DetermineTEDSpecialists/__TMP_SSL_KEY.log'



class Colors:  # TODO: use actual package
    OK = '\033[94m'
    YELLOW = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    UNDERLINE = '\033[4m'


class Timeout(Exception):
    MESSAGE = f"""\n{Colors.FAIL}Timed out.{Colors.ENDC} Retry last cycle {Colors.YELLOW}(y){Colors.ENDC} \
or continue with empty data {Colors.YELLOW}(n){Colors.ENDC}?
Latter option assumes current zipcode (`no_data_radius <= 0`) or surrounding \
`no_data_radius` miles {Colors.YELLOW}has zero doctors{Colors.ENDC}. """


class TepezzaInterface:
    URL = 'https://www.tepezza.com/ted-specialist-finder'
    CRS = "+proj=aeqd +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m no_defs"  # ESRI:102016
    TSHARK_CMD = f'{TSHARK} -i en0 -o ssl.keylog_file:{TMP_CHROME_SSL_LOG} -Y "tcp.port==443"'

    def __init__(self):

        print("Reading zipcode shapefile...")  # TODO: use logging

        # shapefile from
        # https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2019&layergroup=ZIP+Code+Tabulation+Areas
        self._shp = gpd.read_file(
            # 1000 random zipcodes for testing
            'src/DetermineTEDSpecialists/shapefiles/dummy_zipcodes',
        )
        self._shp.set_index('ZCTA5CE10', inplace=True)
        self._shp.to_crs(crs=self.CRS, inplace=True)

        print("Reading dissolved shapefile...")
        self._incomplete = gpd.read_file(
            'src/DetermineTEDSpecialists/shapefiles/dissolved')
        self._incomplete.to_crs(crs=self.CRS, inplace=True)

        self.__exit__()
        Path(TMP_CHROME_SSL_LOG).touch(exist_ok=True)
        os.environ['SSLKEYLOGFILE'] = TMP_CHROME_SSL_LOG

        self.chrome_subp = subprocess.Popen(
            [PATH_TO_CHROME,
             # https://ivanderevianko.com/2020/04/disable-logging-in-selenium-chromedriver
             '--output=/dev/null',
             '--log-level=3',
             '--disable-logging',
             f"--user-data-dir={TMP_CHROME_PROFILE}"],
            stdout=subprocess.DEVNULL)
        print(
            f"New Chrome profile launched; {Colors.YELLOW}proceed to Tepezza{Colors.ENDC}.")

        print(f"{Colors.OK}Initialization complete.\n{Colors.ENDC}")

        while True:
            in_ = input(
                f"Is Chrome loaded? Ready to begin? Press {Colors.YELLOW}\"y\"{Colors.ENDC} to continue. ")
            if in_ == "y":
                break

    def __enter__(self):
        pass

    def __exit__(self, *_):
        if os.path.exists(TMP_CHROME_PROFILE):
            shutil.rmtree(TMP_CHROME_PROFILE)
        if os.path.exists(TMP_CHROME_SSL_LOG):
            pass
            #os.remove(TMP_CHROME_SSL_LOG)
        try:
            self.chrome_subp.kill()
        except AttributeError:
            pass
        print(f"{Colors.OK}Cleanup complete.{Colors.ENDC}")

    def _watch_network(self) -> 'JSON':
        """
        Get network data after zipcode data request is made.
        """

    def get_data(self, starting_zip: str, thresh_frac: float, filepath: str, timeout: int = 60, no_data_radius: int = 0) -> pandas.DataFrame:
        df = pandas.DataFrame(
            columns=['Distance', 'VEEVA_ID', 'FIRST_NAME', 'LAST_NAME', 'MIDDLE_NAME', 'ADDRESS_LINE1',
                     'ADDRESS_LINE2', 'CITY', 'STATE', 'ZIP', 'PRIMARY_DEGREE', 'AMA_SPECIALITY', 'PHONE',
                     'MOBILE', 'EMAIL', 'Latitude', 'Longitude', 'PhysicianAttributes'
                     ]
        )

        target_zip_name = starting_zip
        target_zip_obj = self._shp.at[starting_zip, 'geometry']
        initial_area = self._incomplete.area.iloc[0]

        while not self._incomplete.is_empty.all():
            print(
                f"Look up zipcode {Colors.YELLOW}{Colors.UNDERLINE}{target_zip_name}{Colors.ENDC}.")

            def handler(sig, frame): raise Timeout()
            signal.signal(signal.SIGALRM, handler)
            signal.alarm(timeout)
            try:
                data = self._watch_network()
            except Timeout:
                data = []
                retry = None
                while True:
                    in_ = input(Timeout.MESSAGE)
                    print(in_)
                    if in_ == 'y':
                        retry = True
                        break
                    elif in_ == 'n':
                        retry = False
                        break
                print('\n')
                if retry:
                    # back to start of loop like nothing ever happened
                    continue

            df = df.append(data)
            df.to_csv(filepath)
            print(f"Data received and saved.")

            if data != [] or no_data_radius > 0:
                if data == []:
                    radius_mi = no_data_radius
                else:
                    radius_mi = max((i['Distance']
                                    for i in data)) * thresh_frac

                radius_meters = radius_mi * 5280 * 12 * 2.54 / 100
                complete = target_zip_obj.representative_point().buffer(radius_meters)
            else:
                complete = target_zip_obj
            complete_gdf = gpd.GeoDataFrame(
                {'geometry': complete}, index=[0], crs=self.CRS)

            print("Completed GeoDataFrame found.")

            self._incomplete = gpd.overlay(
                self._incomplete, complete_gdf, how='difference')

            print("Overlay operation complete.")

            pct_done = round(
                (1 - self._incomplete.area.iloc[0] / initial_area) * 100, 4)
            print(f"{Colors.YELLOW}{pct_done}{Colors.ENDC}% done by area.\n")

            target_point = self._incomplete.representative_point().iloc[0]
            target_zip_obj = self._shp.cx[target_point.x, target_point.y]
            idx = self._shp.sindex.query(target_point)
            target_zip_name = self._shp.index[idx]

            print(
                f"New points generated; {Colors.OK}cycle complete{Colors.ENDC}.\n")

            break

        return df


if __name__ == '__main__':
    with TepezzaInterface() as ti:
        pass
    # ti._watch_network()
    #ti.get_data('88255', 0.8, "test_data.csv", 60)
