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
    # env_name = os.environ.get('ENV_NAME')
    env_name = 'dev_shangqing'

    # read the configuration file
    config.read(config_file_path)

    # check if the environment exists in the configuration file
    try:
        env_config = dict(config[env_name])
        env_config['db_pw'] = urllib.parse.quote_plus(env_config['db_pw'])
        env_config['db_conn_str'] = (
            "mysql+pymysql://"
            + env_config['db_us']
            + ":"
            + env_config['db_pw']
            + "@"
            + env_config['db_ht']
            + ":"
            + env_config['db_port']
            + "/"
            + env_config['db_name']
        )

        env_config['api_satellite_endpoint'] = (
            "http://" + env_config['api_satellite_url'] + ":" +
            env_config['api_satellite_port']
        )
    except:
        raise ValueError(f"No configuration found for env: {env_name}")

    return env_config


config = load_config()

DB_US = config['db_us']
DB_PW = config['db_pw']
DB_HT = config['db_ht']
DB_PORT = config['db_port']
DB_NAME = config['db_name']
DB_CONN_STR = config['db_conn_str']
API_SATELLITE_ENDPOINT = config['api_satellite_endpoint']
