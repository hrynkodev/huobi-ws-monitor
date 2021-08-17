import asyncio
import requests
import websockets
import ssl
import json
import uuid
from pprint import pprint
from concurrent.futures import ThreadPoolExecutor

from .constants import PAIRS_TO_MONITOR, HUOBI_WS_URL, UPDATE_DOM_AGGREGATION_LEVEL
from .utils import (
    build_pair_dom_url,
    get_orders_from_http,
    get_ws_data,
    append_orders_data,
)


def _get_pair_orders(session, pair):
    url = build_pair_dom_url(pair)

    with session.get(url) as response:
        try:
            return {"pair": pair, "orders": get_orders_from_http(response)}
        except Exception as e:
            pprint(e)


async def get_initial_dom():
    dom_pairs = dict()

    with ThreadPoolExecutor(max_workers=len(PAIRS_TO_MONITOR)) as executor:
        with requests.Session() as session:
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(executor, _get_pair_orders, *(session, pair))
                for pair in PAIRS_TO_MONITOR
            ]

            for response in await asyncio.gather(*tasks):
                dom_pairs[response["pair"]] = response["orders"]

    return dom_pairs


async def update_pair_dom(pair, dom):
    async with websockets.connect(
        HUOBI_WS_URL, ssl=ssl.SSLContext(protocol=ssl.PROTOCOL_TLS)
    ) as ws:
        await ws.send(
            json.dumps(
                {
                    "sub": f"market.{pair}.depth.{UPDATE_DOM_AGGREGATION_LEVEL}",
                    "id": str(uuid.uuid4()),
                }
            )
        )

        while not ws.closed:
            try:
                response = await ws.recv()
                ws_data = get_ws_data(response)

                if "ping" in ws_data:
                    await ws.send(json.dumps({"pong": ws_data["ping"]}))

                if "bids" in ws_data:
                    dom = append_orders_data(dom, pair, "bids", ws_data)

                if "asks" in ws_data:
                    dom = append_orders_data(dom, pair, "asks", ws_data)

            except Exception as e:
                pprint(e)


async def update_dom(dom):
    await asyncio.wait([update_pair_dom(pair, dom) for pair in PAIRS_TO_MONITOR])
