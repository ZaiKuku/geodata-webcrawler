import os
from configparser import ConfigParser
import urllib.parse
from dotenv import load_dotenv


def load_config():
    # Set the current environment
    load_dotenv('.env')
    config = ConfigParser()
    config_file_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    config.read(config_file_path)
    env_name = os.environ.get('ENV_NAME')
    # env_name = 'dev_shangqing'

    # read the configuration file
    config.read(config_file_path)

    # check if the environment exists in the configuration file
    try:
        env_config = config[env_name]
        
        env_config['DB_PW'] = urllib.parse.quote_plus(env_config['DB_PW'])
        env_config['DB_CONN_STR'] = (
            "mysql+pymysql://"
            + env_config['DB_US']
            + ":"
            + env_config['DB_PW']
            + "@"
            + env_config['DB_HT']
            + ":"
            + env_config['DB_PORT']
            + "/"
            + env_config['DB_NAME']
        )

        env_config['API_SATELLITE_URL'] = (
            "http://" + env_config['API_SATELLITE_URL'] + ":" +
            env_config['API_SATELLITE_PORT']
        )
    except:
        raise ValueError(f"No configuration found for env: {env_name}")

    return env_config


config = load_config()

DB_US = config['DB_US']
DB_PW = config['DB_PW']
DB_HT = config['DB_HT']
DB_PORT = config['DB_PORT']
DB_NAME = config['DB_NAME']
DB_CONN_STR = config['DB_CONN_STR']
API_SATELLITE_ENDPOINT = config['API_SATELLITE_URL']
RESUME_DATA_API_ENDPOINT = config['RESUME_DATA_API_ENDPOINT']
RESUME_DATA_REQ_MAXIMUM = int(config['RESUME_DATA_REQ_MAXIMUM'])
OPERATION_DETAIL_API_ENDPOINT = config['OPERATION_DETAIL_API_ENDPOINT']
