import logging
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
from math import sqrt
from pathlib import Path
from random import randint
from statistics import median
from typing import Callable, Iterable, List, Optional

import clipboard
import dpkt
import geopandas as gpd
import pandas
from matplotlib import pyplot as plt
from shapely.ops import nearest_points


class Colors:  # TODO: use actual package or logging
    OK = '\033[94m'
    YELLOW = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    UNDERLINE = '\033[4m'


class TepezzaApi:
    """Partially automate data return from tepezza.com.
    This interface allows a user to efficiently scrape data,
    even though the website uses reCaptcha. It does this by
    monitoring network traffic through tshark while the user
    looks up zipcodes that an iterative algorithm generates (so
    as to cover the United States efficiently).

    :return: A `TepezzaApi` instance. Call the `logger` method for functionality.
    :rtype: TepezzaApi
    """
    TSHARK = '/Applications/Wireshark.app/Contents/MacOS/tshark'
    PATH_TO_CHROME = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
    TMP_CHROME_PROFILE = '/Users/eab06/Desktop/WJB/PythonProjects/HT_Data/src/__TMP_CHROME_PROFILE'
    TMP_CHROME_SSL_LOG = '/Users/eab06/Desktop/WJB/PythonProjects/HT_Data/src/__TMP_SSL_KEY.log'
    TMP_GATHERED_DATA_LOG = '/Users/eab06/Desktop/WJB/PythonProjects/HT_Data/__TMP_DATA_GATHERED.json'
    SHAPEFILES = '/Users/eab06/Desktop/WJB/PythonProjects/HT_Data/data/tepezza_shapefiles'

    CRS = "+proj=aeqd +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m no_defs"  # ESRI:102016
    TSHARK_CMD = f'{TSHARK} -l -x -i en0 -o ssl.keylog_file:{TMP_CHROME_SSL_LOG} -Y json -T ek'
    RE_JSON_RAW = re.compile('"json_raw": "([a-f0-9]+)"')

    def __init__(self):
        self.__exit__()

    def startup(self) -> None:
        """Load the relevant shapefiles and start the Chrome and tshark subprocesses.
        """
        self.startup_logger = logging.getLogger("startup")
        self._load_shps(self.startup_logger)
        self._load_chrome(self.startup_logger)
        
        while True:
            in_ = input(
                f"Is Chrome loaded? Ready to begin? Press {Colors.YELLOW}(y){Colors.ENDC}. ")
            if in_ == "y":
                break

        self.network_subp = subprocess.Popen(
            shlex.split(self.TSHARK_CMD), stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1, universal_newlines=True)
        os.set_blocking(self.network_subp.stdout.fileno(), False)
        os.set_blocking(self.network_subp.stderr.fileno(), False)

        
        # proceeding too quickly will break things. Added a busy wait from self.network_subp.stderr to fix this
        while self.network_subp.stderr.readline() == '':
            pass
        
        self.startup_logger.info(f"{Colors.OK}Initialization complete; proceed to Tepezza.{Colors.ENDC}")
        

    def _load_shps(self, logger: logging.Logger) -> None:
        logger.info("Reading zipcode shapefile...")  # TODO: use logging

        # shapefile from
        # https://www2.census.gov/geo/tiger/TIGER2019/ZCTA5/tl_2019_us_zcta510.zip,
        # converted to CRS
        self._shp = gpd.read_file(os.path.join(self.SHAPEFILES, 'zipcodes'))
        self._shp.set_index('ZCTA5CE10', inplace=True)

        logger.info("Reading dissolved shapefile...")
        self._incomplete = gpd.read_file(
            os.path.join(self.SHAPEFILES, 'dissolved/'))
        self._incomplete.to_crs(crs=self.CRS, inplace=True)  # FIXME

        logger.info("Reading representative multipoint shapefile...")
        self._repr_multipoint = gpd.read_file(
            os.path.join(self.SHAPEFILES, 'representative_pts_multipoint/'))\
            .geometry[0]

    def _load_chrome(self, logger: logging.Logger) -> None:
        """Open new Chrome window in a subprocess,
        so as to have decryption keys go into
        `TMP_CHROME_SSL_LOG`.
        """
        Path(self.TMP_CHROME_SSL_LOG).touch(exist_ok=True)
        os.environ['SSLKEYLOGFILE'] = self.TMP_CHROME_SSL_LOG

        self.chrome_subp = subprocess.Popen(
            [self.PATH_TO_CHROME,
             # https://ivanderevianko.com/2020/04/disable-logging-in-selenium-chromedriver
             '--output=/dev/null',
             '--log-level=3',
             '--disable-logging',
             f"--user-data-dir={self.TMP_CHROME_PROFILE}"],
            stdout=subprocess.DEVNULL)
        logger.info(
            f"New Chrome profile launched; {Colors.FAIL}Do not proceed to Tepezza{Colors.ENDC}.")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.exit_logger = logging.getLogger("exit cleanup")
        try:
            self.chrome_subp.kill()
        except AttributeError:
            self.exit_logger.debug("No chrome subprocess found")
        else:
            self.exit_logger.debug("Chrome subprocess killed")

        try:
            self.network_subp.kill()
        except AttributeError:
            self.exit_logger.debug("No network subprocess found")
        else:
            self.exit_logger.debug("Network subprocess killed")

        for path in [
            self.TMP_CHROME_PROFILE,
            self.TMP_GATHERED_DATA_LOG,
            self.TMP_CHROME_SSL_LOG
        ]:
            if os.path.exists(path):
                try:
                    if os.path.isdir(path):
                        shutil.rmtree(path)
                    elif os.path.isfile(path):
                        os.remove(path)
                except PermissionError as e:
                    self.exit_logger.warning(e, exc_info=True)

        self.exit_logger.info(f"{Colors.OK}Cleanup complete.{Colors.ENDC}")

    def _watch_network(self, logger: logging.Logger) -> 'JSON':
        while True:
            if input(
                f"Once zipcode has been searched, press {Colors.YELLOW}(y){Colors.ENDC} to continue. "
            ) == 'y':
                break
        counter = 0
        s = b''  # build up `s` from packets that have missing/cutoff JSON
        while True:
            try:
                in_ = bytes(self.network_subp.stdout.readline(), 'utf-8')
                if in_ == b'':
                    continue
                counter += 1
                pkt = json.loads(in_)
                s = b''

            except (json.JSONDecodeError, json.decoder.JSONDecodeError) as e:  # overflowed
                logger.info(e, exc_info=True)
                s += in_
                try:
                    pkt = json.loads(s)
                except (json.JSONDecodeError, json.decoder.JSONDecodeError):
                    continue
            s = b''
            try:
                match_obj = self.RE_JSON_RAW.search(json.dumps(pkt))
                assert match_obj, "match_obj"
                json_raw = match_obj.group(1)
                assert json_raw, "json_raw"

                d = json.loads(bytearray.fromhex(json_raw).decode('utf-8'))
                assert d['success'], 'success'

                for i in d['data']:
                    i.pop('GeoLocation')
                return d['data']
            except (KeyError, IndexError, AssertionError, TypeError) as e:
                #with open(f'error_{counter}.json', 'wb+') as f:
                    #f.write(in_)
                logger.info(e, exc_info=True)

            # i in d['data'] will be str if no data, so pop raises
            except AttributeError:
                logger.info("returning empty data")
                return []

    def get_data(self,
                 starting_zip: str,
                 radius_func: Callable[[Iterable[float]], float],
                 filepath: str,
                 no_data_radius: int = 0) -> pandas.DataFrame:
        """Begin zipcode data loop.

        :param starting_zip: Initial zip code to scan
        :type starting_zip: str
        :param radius_func: transform list of radii to some aggregate (median, greatest, etc.)
        :type radius_func: Callable[[Iterable[float]], float]
        :param filepath: where output DataFrame is saved
        :type filepath: str
        :param no_data_radius:
            If no data are returned, assume there are no doctors within this mile radius.
            If <= 0, only assume no doctors within the scanned zipcode, defaults to 0.
        :type no_data_radius: int, optional
        :return: output DataFrame, also saved to `filepath`.
        :rtype: pandas.DataFrame
        """

        df = pandas.DataFrame()

        target_zip_name = starting_zip
        initial_area = self._incomplete.area.iloc[0]

        self.logger = logging.getLogger("get data")
        self.watch_network_logger = logging.getLogger("watch network")
        while True:
            self.logger.info(
                f"Look up zipcode {Colors.YELLOW}{Colors.UNDERLINE}{target_zip_name}{Colors.ENDC}.")
            clipboard.copy(target_zip_name)

            while True:
                data = self._watch_network(self.watch_network_logger)
                with open(self.TMP_GATHERED_DATA_LOG, 'w+') as f:
                    json.dump(data, f, indent=2)
                inp = input(
                    f"Are data accurate (correct zipcode entered)? If not sure, check "
                    f"{Colors.YELLOW}{self.TMP_GATHERED_DATA_LOG}{Colors.ENDC} for most recent observation. "
                    f"Otherwise, enter {Colors.YELLOW}(y){Colors.ENDC} to proceed. "
                )
                if inp == 'y':
                    break

            df = df.append(data)
            df.to_csv(filepath)
            

            target_zip_obj = self._shp.at[target_zip_name, 'geometry']

            if data:
                radius_mi = radius_func((i['Distance'] for i in data))
                radius_meters = radius_mi * 5280 * 12 * 2.54 / 100
                complete = target_zip_obj.representative_point().buffer(radius_meters)
            elif no_data_radius:
                radius_mi = no_data_radius
                radius_meters = radius_mi * 5280 * 12 * 2.54 / 100
                complete = target_zip_obj.representative_point().buffer(radius_meters)
            else:
                complete = target_zip_obj

            complete_gdf = gpd.GeoDataFrame(
                {'geometry': complete}, index=[0], crs=self.CRS)

            self.logger.info("Completed GeoDataFrame found.")

            self._incomplete = gpd.overlay(
                self._incomplete, complete_gdf, how='difference')

            #self._incomplete.plot()
            #plt.show()

            self.logger.info("Overlay operation complete.")

            if self._incomplete.is_empty.all():
                self.logger.info("Loop done.")
                break

            pct_done = round(
                (1 - self._incomplete.area.iloc[0] / initial_area) * 100, 8)
            self.logger.info(f"{Colors.YELLOW}{pct_done}{Colors.ENDC}% done by area.\n")

            target_point = self._incomplete.representative_point().iloc[0]
            nearest_repr_pt = nearest_points(
                target_point,
                self._repr_multipoint
            )[1]
            zipcode_index_idx = self._shp.sindex.query(nearest_repr_pt)[0]
            target_zip_name = self._shp.index[zipcode_index_idx]

            self.logger.info(
                f"New points generated; {Colors.OK}cycle complete{Colors.ENDC}.\n")

        return df


if __name__ == '__main__':
    def rfunc(radii: Iterable[float]) -> float:
        return median(radii)

    logging.basicConfig(level=logging.INFO)

    try:
        with TepezzaApi() as api:
            api.startup()
            api.get_data('60609', rfunc, 'data/_tepezza_raw.csv', 50)

    except KeyboardInterrupt:
        pass
