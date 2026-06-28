# test_filling_mode.py (fixed)
import MetaTrader5 as mt5
from config.loader import load_config

cfg = load_config('config/slave.yaml')
slave = [s for s in cfg.slaves if s.enabled][0]

mt5.initialize(
    path=slave.terminal_path,
    login=slave.login,
    password=slave.password,
    server=slave.server
)

# Check BTCUSDm filling modes
symbol_info = mt5.symbol_info("BTCUSDm")
if symbol_info:
    print(f"Symbol: BTCUSDm")
    print(f"Filling mode flags: {symbol_info.filling_mode}")
    print(f"Trade mode: {symbol_info.trade_mode}")
    print(f"Trade execution: {symbol_info.trade_exemode}")  # Correct attribute
    
    # Test FOK
    tick = mt5.symbol_info_tick("BTCUSDm")
    if tick:
        request = {
            "action": 1,  # TRADE_ACTION_DEAL
            "symbol": "BTCUSDm",
            "volume": 0.01,
            "type": 0,  # BUY
            "price": tick.ask,
            "deviation": 20,
            "magic": 123456,
            "comment": "test",
            "type_time": 1,  # GTC
            "type_filling": 1,  # FOK
        }
        
        print(f"\n--- Test FOK order ---")
        result = mt5.order_check(request)
        if result:
            print(f"  Retcode: {result.retcode}")
            print(f"  Comment: {result.comment}")
            if result.retcode == 0:
                print("  ✓ FOK SUCCESS")
        else:
            print(f"  Error: {mt5.last_error()}")
        
        # Test IOC  
        request["type_filling"] = 0  # Try 0 (let broker decide)
        print(f"\n--- Test AUTO filling ---")
        result = mt5.order_check(request)
        if result:
            print(f"  Retcode: {result.retcode}")
            print(f"  Comment: {result.comment}")
            if result.retcode == 0:
                print("  ✓ AUTO SUCCESS")
        
        # Try without filling specification
        request.pop("type_filling", None)
        print(f"\n--- Test NO filling specified ---")
        result = mt5.order_check(request)
        if result:
            print(f"  Retcode: {result.retcode}")
            print(f"  Comment: {result.comment}")
            if result.retcode == 0:
                print("  ✓ NO FILL SUCCESS")

mt5.shutdown()