import os
from Modules.configure_api import set_envrionment_vars
from binance.client import Client
import datetime
set_envrionment_vars()

api_key = os.environ.get('binance_api')
api_secret = os.environ.get('binance_secret')

client = Client(api_key, api_secret)

