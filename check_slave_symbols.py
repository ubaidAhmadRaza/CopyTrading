# check_slave_symbols.py
import MetaTrader5 as mt5
from config.loader import load_config

cfg = load_config('config/slave.yaml')
slave = [s for s in cfg.slaves if s.enabled][0]

mt5.initialize(path=slave.terminal_path, login=slave.login, password=slave.password, server=slave.server)

symbols = mt5.symbols_get()
btc = [s for s in symbols if 'BTC' in s.name]
print("BTC symbols on slave:", [s.name for s in btc])

mt5.shutdown()