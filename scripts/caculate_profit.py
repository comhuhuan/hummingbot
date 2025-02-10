import asyncio
import time

import ccxt.pro as ccxtpro
from colorama import Fore, Style

high_spread_queue = asyncio.Queue()


async def load_markets(exchanges):
    """加载所有交易所的市场数据"""
    for exchange in exchanges.values():
        await exchange.load_markets()


def get_common_pairs(exchange_1, exchange_2):
    """获取两个交易所的共同交易对（仅限合约交易）"""
    exchange_1_pairs = set(
        symbol for symbol in exchange_1.markets if exchange_1.markets[symbol].get('swap', False) and exchange_1.markets[symbol].get('linear', False)
    )
    exchange_2_pairs = set(
        symbol for symbol in exchange_2.markets if exchange_2.markets[symbol].get('swap', False) and exchange_2.markets[symbol].get('linear', False)
    )
    return list(exchange_1_pairs.intersection(exchange_2_pairs))


async def subscribe_tickers(exchanges, price_cache):
    """
    订阅所有交易所支持的 linear 类型交易对的 ticker 数据，并缓存价格
    """
    tasks = {}
    for exchange_name, exchange in exchanges.items():
        symbols = [symbol for symbol in exchange.markets if exchange.markets[symbol].get('swap', False) and exchange.markets[symbol].get('linear', False)]
        if symbols:
            tasks[exchange_name] = exchange.watch_tickers(symbols)  # 任务存储为字典

    results = await asyncio.gather(*tasks.values())  # 获取所有 ticker 数据

    # 重新映射数据，确保交易所名称和 ticker 数据匹配
    for exchange_name, ticker_data in zip(tasks.keys(), results):
        for symbol, ticker in ticker_data.items():
            if symbol not in price_cache:
                price_cache[symbol] = {}
            price_cache[symbol][exchange_name] = ticker['last']


async def watch_all_tickers(exchange_names):
    """
    监控所有交易所的 tickers，并在满足条件时返回符合要求的数据
    """
    last_time = 0

    exchanges = {exchange_name: getattr(ccxtpro, exchange_name)() for exchange_name in exchange_names}
    await load_markets(exchanges)  # 加载市场数据

    # **🔄 初始化价格缓存**
    price_cache = {}

    while True:
        try:
            await subscribe_tickers(exchanges, price_cache)  # 订阅 ticker 数据并更新缓存
            tickers_dict = price_cache.copy()  # 复制缓存数据，避免并发问题

            # 初始化价差数据存储
            spread_data = []
            high_spread_data = []

            # 计算交易所两两配对的共同交易对价差
            for i in range(len(exchange_names)):
                for j in range(i + 1, len(exchange_names)):
                    exchange_1_name = exchange_names[i]
                    exchange_2_name = exchange_names[j]

                    # 获取共同交易对
                    common_pairs = get_common_pairs(exchanges[exchange_1_name], exchanges[exchange_2_name])

                    for symbol in common_pairs:
                        price_1 = tickers_dict.get(symbol, {}).get(exchange_1_name, None)
                        price_2 = tickers_dict.get(symbol, {}).get(exchange_2_name, None)

                        if price_1 is not None and price_2 is not None:
                            spread = price_1 - price_2
                            spread_percent = (spread / price_2) * 100 if price_2 else 0
                            spread_data.append((symbol, exchange_1_name, exchange_2_name, price_1, price_2, spread, spread_percent))

                            # 记录 Spread % 绝对值大于 0.5% 的项
                            if abs(spread_percent) > 0.5:
                                high_spread_data.append((symbol, exchange_1_name, exchange_2_name, price_1, price_2, spread, spread_percent))

            # 按 spread_percent 绝对值降序排序
            spread_data.sort(key=lambda x: abs(x[6]), reverse=True)

            # 显示前 10 个价差信息
            current_time = time.time()
            if current_time - last_time >= 60:
                status = await format_ticker_status(spread_data[:10])
                last_time = current_time
                print(status)

            # 满足条件时返回前 10 个交易对
            if len(spread_data) >= 250 and len(high_spread_data) >= 3:
                await high_spread_queue.put(high_spread_data[:3])

        except Exception as e:
            print(f"Error: {str(e)}")
            await asyncio.sleep(5)  # 遇到错误时稍等一会儿再继续


async def format_ticker_status(spread_data):
    """格式化价差数据并返回字符串"""
    max_symbol_width = max(len(symbol) for symbol, *_ in spread_data) if spread_data else 6
    max_exchange_1_width = 10
    max_exchange_2_width = 10
    max_price_width = 15
    max_spread_width = 12
    max_spread_percent_width = 12

    lines = [Fore.CYAN + "\n=== Ticker Status ===" + Style.RESET_ALL]
    header = (
        f"{Fore.YELLOW}{'Symbol':<{max_symbol_width}}{Style.RESET_ALL}  "
        f"{Fore.YELLOW}{'Exchange 1':<{max_exchange_1_width}}{Style.RESET_ALL}  "
        f"{Fore.YELLOW}{'Exchange 2':<{max_exchange_2_width}}{Style.RESET_ALL}  "
        f"{Fore.YELLOW}{'Price 1':<{max_price_width}}{Style.RESET_ALL}  "
        f"{Fore.YELLOW}{'Price 2':<{max_price_width}}{Style.RESET_ALL}  "
        f"{Fore.YELLOW}{'Spread':<{max_spread_width}}{Style.RESET_ALL}  "
        f"{Fore.YELLOW}{'Spread %':<{max_spread_percent_width}}{Style.RESET_ALL}"
    )
    lines.append(header)
    lines.append(Fore.YELLOW + "=" * (
        max_symbol_width + max_exchange_1_width + max_exchange_2_width +
        max_price_width * 2 + max_spread_width + max_spread_percent_width + 10
    ) + Style.RESET_ALL)

    for symbol, exchange_1, exchange_2, price_1, price_2, spread, spread_percent in spread_data:
        price_1_str = f"{price_1:.8f}"
        price_2_str = f"{price_2:.8f}"
        spread_str = f"{spread:.8f}"
        spread_percent_str = f"{spread_percent:.4f}%"

        line = (
            f"{symbol:<{max_symbol_width}}  "
            f"{exchange_1:<{max_exchange_1_width}}  "
            f"{exchange_2:<{max_exchange_2_width}}  "
            f"{price_1_str:<{max_price_width}}  "
            f"{price_2_str:<{max_price_width}}  "
            f"{spread_str:<{max_spread_width}}  "
            f"{spread_percent_str:<{max_spread_percent_width}}"
        )
        lines.append(line)

    return "\n".join(lines)

# 交易所列表


async def get_high_spread():
    """
    读取队列中的最新 high_spread_data
    """
    while True:
        high_spread = await high_spread_queue.get()
        print("📢 最新高价差交易对:", high_spread)  # 这里可以换成其他操作，比如存入数据库


async def main():
    exchange_list = ['binance', 'okx', 'bybit', 'bitget']

    # **并发运行 watch_all_tickers 和 get_high_spread**
    await asyncio.gather(
        watch_all_tickers(exchange_list),
        get_high_spread()
    )
# 启动 asyncio 事件循环
asyncio.run(main())
