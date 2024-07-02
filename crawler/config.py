import os
import configparser
import urllib.parse
from dotenv import load_dotenv


def load_config():
    # Set the current environment
    load_dotenv('.env')
    config = configparser.RawConfigParser()
    # env_name = os.environ.get('ENV_NAME')
    env_name = 'dev_shangqing'

    # read the configuration file
    config.read('config.ini')
    
    # check if the environment exists in the configuration file
    if env_name in config:
        env_config = dict(config[env_name])
        env_config['db_pw'] = urllib.parse.quote_plus(env_config['db_pw'])
        env_config['db_conn_str'] = f"mysql+pymysql://{env_config['db_us']}:{env_config['db_pw']}@{env_config['db_ht']}:{env_config['db_port']}/{env_config['db_name']}"
    else:
        raise ValueError(f"No configuration found for env: {env_name}")

    return env_config




