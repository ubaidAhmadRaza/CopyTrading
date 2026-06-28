"""
Check available symbols on master account
"""
import sys
sys.path.insert(0, '.')

import MetaTrader5 as mt5
from config.loader import load_config

# Load master config
cfg = load_config('config/master.yaml')

print(f"Connecting to master account {cfg.master.login}...")

if not mt5.initialize(
    path=cfg.master.terminal_path,
    login=cfg.master.login,
    password=cfg.master.password,
    server=cfg.master.server
):
    print(f"❌ MT5 initialize failed: {mt5.last_error()}")
    sys.exit(1)

print("✓ Connected to master MT5")

# Get account info
info = mt5.account_info()
if info:
    print(f"\n📊 Account Info:")
    print(f"   Login: {info.login}")
    print(f"   Balance: {info.balance}")
    print(f"   Equity: {info.equity}")
    print(f"   Server: {info.server}")

# Get all symbols
symbols = mt5.symbols_get()
print(f"\n📈 Total symbols available: {len(symbols)}")

# Show important groups
print("\n=== FOREX SYMBOLS (first 20) ===")
forex = [s for s in symbols if not any(x in s.name for x in ['BTC', 'ETH', 'XAU', 'XAG', 'US30', 'NAS', 'SPX'])]
for s in forex[:20]:
    print(f"  {s.name}")

print("\n=== CRYPTO SYMBOLS ===")
crypto = [s for s in symbols if any(x in s.name for x in ['BTC', 'ETH', 'XRP', 'LTC'])]
for s in crypto:
    print(f"  {s.name}")

print("\n=== METALS ===")
metals = [s for s in symbols if any(x in s.name for x in ['XAU', 'XAG', 'XPT'])]
for s in metals:
    print(f"  {s.name}")

print("\n=== INDICES ===")
indices = [s for s in symbols if any(x in s.name for x in ['US30', 'NAS', 'SPX', 'DAX', 'UK100'])]
for s in indices:
    print(f"  {s.name}")

# Check specific symbols you might trade
print("\n=== SYMBOL DETAILS ===")
symbols_to_check = ['BTCUSDm', 'BTCUSD', 'EURUSD', 'EURUSD.a', 'XAUUSD', 'XAUUSDm']
for sym_name in symbols_to_check:
    sym = mt5.symbol_info(sym_name)
    if sym:
        print(f"\n✓ {sym_name}:")
        print(f"   Spread: {sym.spread}")
        print(f"   Digits: {sym.digits}")
        print(f"   Volume Min/Max: {sym.volume_min}/{sym.volume_max}")
        print(f"   Trade Mode: {sym.trade_mode}")
        print(f"   Trade Stops Level: {sym.trade_stops_level}")
    else:
        print(f"\n✗ {sym_name}: NOT AVAILABLE")

# Current positions
positions = mt5.positions_get()
print(f"\n=== CURRENT POSITIONS ({len(positions)}) ===")
for p in positions:
    print(f"  Ticket: {p.ticket} | {p.symbol} | {p.type} | Vol: {p.volume} | Profit: {p.profit}")

# Current orders
orders = mt5.orders_get()
print(f"\n=== PENDING ORDERS ({len(orders)}) ===")
for o in orders:
    print(f"  Ticket: {o.ticket} | {o.symbol} | {o.type} | Vol: {o.volume_current} | Price: {o.price_open}")

mt5.shutdown()
print("\n✓ Disconnected from master")