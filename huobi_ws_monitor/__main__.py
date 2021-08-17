import asyncio
from pprint import pprint

from .huobi_service import get_initial_dom, update_dom
from .utils import schedule_logging, clear_orders
from .constants import PAIRS_TO_MONITOR


def log_best_orders():
    for pair in PAIRS_TO_MONITOR:
        best_bid = max(dom[pair]["bids"], key=lambda x: x[0])[0]
        best_ask = min(dom[pair]["asks"], key=lambda x: x[0])[0]

        pprint({"pair": pair, "best_bid": best_bid, "best_ask": best_ask})

        dom[pair] = clear_orders(dom[pair])

    pprint("============================================================")


def main():
    global dom

    loop = asyncio.get_event_loop()

    initial_dom_future = asyncio.ensure_future(get_initial_dom())
    dom = loop.run_until_complete(initial_dom_future)

    loop.create_task(schedule_logging(60, log_best_orders))

    update_dom_future = asyncio.ensure_future(update_dom(dom))

    loop.run_until_complete(update_dom_future)


main()
