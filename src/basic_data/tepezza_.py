from tepezza import TepezzaApi
import logging

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    try:
        with TepezzaApi() as api:
            api.startup()
            api.get_data(5, 'data/_tepezza_raw.csv', True, '05853')

    except KeyboardInterrupt:
        pass