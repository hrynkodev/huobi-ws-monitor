import asyncio
import gzip
import json

from .constants import HUOBI_BASE_URL, INITIAL_DOM_AGGREGATION_LEVEL, ORDER_TYPES


async def schedule_logging(period, fn):
    while True:
        await asyncio.sleep(period)
        fn()


def build_pair_dom_url(pair):
    return f"{HUOBI_BASE_URL}/market/depth?symbol={pair}&type={INITIAL_DOM_AGGREGATION_LEVEL}"


def _get_orders_from_tick(data):
    return dict((key, data.get("tick")[key]) for key in ORDER_TYPES)


def get_orders_from_http(response):
    data = json.loads(response.text)

    return _get_orders_from_tick(data)


def get_ws_data(response):
    data = json.loads(gzip.decompress(response).decode("utf-8"))

    if "tick" not in data:
        return data

    return _get_orders_from_tick(data)


def append_orders_data(dom, pair, order_type, data):
    [dom[pair][order_type].append(order) for order in data[order_type]]

    return dom


def clear_orders(pair):
    for order_type in ORDER_TYPES:
        if len(pair[order_type]) <= 150:
            pass
        pair[order_type] = pair[order_type][-150:]

    return pair
