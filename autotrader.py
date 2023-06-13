# Copyright 2021 Optiver Asia Pacific Pty. Ltd.
#
# This file is part of Ready Trader Go.
#
#     Ready Trader Go is free software: you can redistribute it and/or
#     modify it under the terms of the GNU Affero General Public License
#     as published by the Free Software Foundation, either version 3 of
#     the License, or (at your option) any later version.
#
#     Ready Trader Go is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU Affero General Public License for more details.
#
#     You should have received a copy of the GNU Affero General Public
#     License along with Ready Trader Go.  If not, see
#     <https://www.gnu.org/licenses/>.
import asyncio
import itertools

from typing import List

from ready_trader_go import BaseAutoTrader, Instrument, Lifespan, MAXIMUM_ASK, MINIMUM_BID, Side

LOT_SIZE = 10
POSITION_LIMIT = 100
TICK_SIZE_IN_CENTS = 100
MIN_BID_NEAREST_TICK = (MINIMUM_BID + TICK_SIZE_IN_CENTS) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
MAX_ASK_NEAREST_TICK = MAXIMUM_ASK // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS

DEBUG = True
SPREAD_OPEN_SIGNAL = 2 * TICK_SIZE_IN_CENTS
SPREAD_CLOSE_SIGNAL = 0 * TICK_SIZE_IN_CENTS


class Order(object):
    def __init__(self, order_id: int, side: Side, price: int, volume: int):
        self.order_id = order_id
        self.side = side
        self.price = price
        self.volume = volume


class AutoTrader(BaseAutoTrader):
    """Example Auto-trader.

    When it starts this auto-trader places ten-lot bid and ask orders at the
    current best-bid and best-ask prices respectively. Thereafter, if it has
    a long position (it has bought more lots than it has sold) it reduces its
    bid and ask prices. Conversely, if it has a short position (it has sold
    more lots than it has bought) then it increases its bid and ask prices.
    """

    """ --- UTILS --- """

    def instrument_to_string(self, instrument):
        return "ETF" if instrument == Instrument.ETF else "FUT"

    def side_to_string(self, side: Side):
        return "SELL" if side == Side.SELL else "BUY"

    def order_book_to_string(self, ask_prices, ask_volumes, bid_prices, bid_volumes):
        return ("BidVol\tPrice\tAskVol\n"
                + "\n".join("\t%dc\t%6d" % (p, v) for p, v in zip(reversed(ask_prices), reversed(ask_volumes)) if p)
                + "\n" + "\n".join("%6d\t%dc" % (v, p) for p, v in zip(bid_prices, bid_volumes) if p))

    def unhedged(self):
        unhedged = -self.etf_position != self.fut_position
        if unhedged and DEBUG:
            self.logger.info(f'UNHEDGED: etf_position={self.etf_position}; fut_position={self.fut_position}')
        return unhedged

    def no_outstanding_orders(self, side: Side):
        if self.last_etf_order == None:
            return True
        if side == Side.SELL:
            return self.last_etf_order.side != Side.SELL
        elif side == Side.BUY:
            return self.last_etf_order.side != Side.BUY

    """ --- PRICING --- """

    def conservative_price(self, instrument: Instrument, side: Side):
        if instrument == Instrument.ETF:
            return self.best_bid_price_etf if side == Side.SELL else self.best_ask_price_etf
        if instrument == Instrument.FUTURE:
            return self.best_bid_price_fut if side == Side.SELL else self.best_ask_price_fut

    def mid_market_price(self, best_bid_price, best_bid_volume, best_ask_price, best_ask_volume):
        return (best_bid_price * best_bid_volume + best_ask_price * best_ask_volume) / (
                best_bid_volume + best_ask_volume)

    # consider other pricing strategies
    def calculate_price(self, instrument: Instrument, side: Side):
        return self.conservative_price(instrument, side)

    def calculate_spread(self, side: Side):
        price_etf = self.calculate_price(Instrument.ETF, side)
        price_fut = self.calculate_price(Instrument.FUTURE, Side.SELL if side == Side.BUY else Side.BUY)
        spread = price_etf - price_fut
        if DEBUG:
            print(f'spread={spread}; side={self.side_to_string(side)}')
            self.logger.info(f'spread={spread}; side={self.side_to_string(side)}')
        return spread

    """ --- SENDING ORDERS --- """

    def insert_order(self, side: Side, price: int, volume: int, lifespan: Lifespan) -> None:
        # abort if position limit is going to breached
        change = volume if side == Side.BUY else -volume
        if abs(self.etf_position + change) > POSITION_LIMIT:
            return

        order_id = next(self.order_ids)
        if DEBUG:
            self.logger.info(
                f'current ETF position: {self.etf_position}')
            self.logger.info(
                f'inserting ETF order: order_id={order_id}; side={self.side_to_string(side)}; price={price}; volume={volume}; lifespan={"FILL_AND_KILL" if lifespan == Lifespan.FILL_AND_KILL else "GOOD_FOR_DAY"}')
        self.send_insert_order(order_id, side, price, volume, lifespan)

        order = Order(order_id, side, price, volume)
        self.last_etf_order = order

    def hedge_order(self, side: Side, price: int, volume: int) -> None:
        # abort if position limit is going to breached
        change = volume if side == Side.BUY else -volume
        if abs(self.fut_position + change) > POSITION_LIMIT:
            return

        order_id = next(self.order_ids)
        if DEBUG:
            self.logger.info(
                f'current FUT position: {self.etf_position}')
            self.logger.info(
                f'inserting FUT order: order_id={order_id}; side={self.side_to_string(side)}; price={price}; volume={volume}')
        self.send_hedge_order(order_id, side, price, volume)

        order = Order(order_id, side, price, volume)
        self.last_fut_order = order

    # order is not guaranteed to be cancelled, it might be filled first
    def cancel_order(self, order_id: int) -> None:
        if DEBUG:
            self.logger.info(
                f'cancelling order: order_id={order_id}')
        self.send_cancel_order(order_id)

    """ --- AUTOTRADER --- """

    def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
        """Initialise a new instance of the AutoTrader class."""
        super().__init__(loop, team_name, secret)
        self.order_ids = itertools.count(1)
        self.bids = set()
        self.asks = set()

        self.last_etf_order = None
        self.last_fut_order = None

        self.etf_position = 0
        self.fut_position = 0

        self.best_bid_price_fut = MIN_BID_NEAREST_TICK
        self.best_ask_price_fut = MAX_ASK_NEAREST_TICK
        self.best_bid_volume_fut = 0
        self.best_ask_volume_fut = 0
        self.best_bid_price_etf = MIN_BID_NEAREST_TICK
        self.best_ask_price_etf = MAX_ASK_NEAREST_TICK
        self.best_bid_volume_etf = 0
        self.best_ask_volume_etf = 0

    def update_state(self, instrument: Instrument, ask_prices: List[int], ask_volumes: List[int], bid_prices: List[int],
                     bid_volumes: List[int]):
        # update best bids/asks
        if instrument == Instrument.FUTURE:
            self.best_bid_price_fut = bid_prices[0]
            self.best_ask_price_fut = ask_prices[0]
            self.best_bid_volume_fut = bid_volumes[0]
            self.best_ask_volume_fut = ask_volumes[0]
        if instrument == Instrument.ETF:
            self.best_bid_price_etf = bid_prices[0]
            self.best_ask_price_etf = ask_prices[0]
            self.best_bid_volume_etf = bid_volumes[0]
            self.best_ask_volume_etf = ask_volumes[0]

    # reestablishes 1:1 hedge
    def fix_hedge(self):
        # fix hedge
        volume = -self.etf_position - self.fut_position
        hedge_side = Side.BUY if volume > 0 else Side.SELL
        min_price = self.last_fut_order.price
        hedge_possible = self.best_ask_price_fut <= min_price if hedge_side == Side.BUY else self.best_bid_price_fut >= min_price
        if hedge_possible:
            best_price = self.best_ask_price_fut if hedge_side == Side.BUY else self.best_bid_price_fut
            self.hedge_order(hedge_side, best_price, abs(volume))
            self.last_fut_order.price = min_price

    # only sends etf orders
    # hedge orders are sent when etf orders are filled
    def increase_position(self, etf: Side, fut: Side):
        # either long etf, short fut or short etf, long fut
        if etf + fut != 1:
            if DEBUG:
                self.logger.info("buy_pair received unexpected parameters")
            return

        if etf == Side.SELL:
            # take the min of etf best bid volume and fut best ask volume
            volume = min(self.best_bid_volume_etf, self.best_ask_volume_fut, POSITION_LIMIT + self.etf_position)
            if volume == 0:
                return
            self.insert_order(Side.SELL, self.best_bid_price_etf, volume,
                              Lifespan.FILL_AND_KILL)
        elif etf == Side.BUY:
            volume = min(self.best_ask_volume_etf, self.best_bid_volume_fut, POSITION_LIMIT - self.etf_position)
            # take the min of etf best ask volume and fut best bid volume
            if volume == 0:
                return
            self.insert_order(Side.BUY, self.best_ask_price_etf, volume,
                              Lifespan.FILL_AND_KILL)

    # only sends etf orders
    # hedge orders are sent when etf orders are filled
    def decrease_position(self, etf: Side, fut: Side):
        if etf + fut != 1:
            if DEBUG:
                self.logger.info("buy_pair received unexpected parameters")
            return

        if etf == Side.SELL:
            self.insert_order(Side.SELL, self.best_bid_price_etf, self.etf_position,
                              Lifespan.FILL_AND_KILL)
        elif etf == Side.BUY:
            self.insert_order(Side.BUY, self.best_ask_price_etf, -self.etf_position,
                              Lifespan.FILL_AND_KILL)

    # bid prices: best bid first (highest bid)
    # ask prices: best ask first (lowest ask)
    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        self.logger.info("received order book for instrument %d with sequence number %d", instrument,
                         sequence_number)
        if DEBUG:
            self.logger.info(
                f'order book ({self.instrument_to_string(instrument)}):\n'
                + self.order_book_to_string(ask_prices, ask_volumes, bid_prices, bid_volumes))

        self.update_state(instrument, ask_prices, ask_volumes, bid_prices, bid_volumes)

        # only try fixing hedge if future order book updated
        # if hedge order with previous future order book did not fill, no point in trying again
        if self.unhedged() and instrument == Instrument.FUTURE:
            self.fix_hedge()
            # dont trade etf until hedge is fixed
            return

        # if spread > 0 then etf price > fut price
        # if spread < 0 then fut price > etf price
        buy_pair_spread = self.calculate_spread(Side.BUY)
        sell_pair_spread = self.calculate_spread(Side.SELL)

        # OPEN SIGNALS
        # sell etf and buy future
        if sell_pair_spread > SPREAD_OPEN_SIGNAL \
                and self.no_outstanding_orders(Side.SELL):
            self.increase_position(etf=Side.SELL, fut=Side.BUY)
        # buy etf and sell future
        elif buy_pair_spread < -SPREAD_OPEN_SIGNAL \
                and self.no_outstanding_orders(Side.BUY):
            self.increase_position(etf=Side.BUY, fut=Side.SELL)
        # CLOSE SIGNALS
        # close etf short position and future long position
        elif buy_pair_spread <= SPREAD_CLOSE_SIGNAL \
                and self.no_outstanding_orders(Side.BUY) \
                and self.etf_position < 0:
            self.decrease_position(etf=Side.BUY, fut=Side.SELL)
        # close etf long position and future short position
        elif sell_pair_spread >= -SPREAD_CLOSE_SIGNAL \
                and self.no_outstanding_orders(Side.SELL) \
                and self.etf_position > 0:
            self.decrease_position(etf=Side.SELL, fut=Side.BUY)

    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your orders is filled, partially or fully.

        The price is the price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        self.logger.info("received order filled for order %d with price %d and volume %d", client_order_id,
                         price, volume)

        if not self.last_etf_order or self.last_etf_order.order_id != client_order_id:
            return

        # bought etf
        if self.last_etf_order.side == Side.BUY:
            self.etf_position += volume
            self.hedge_order(Side.SELL, self.best_bid_price_fut, volume)
        # sold etf
        if self.last_etf_order.side == Side.SELL:
            self.etf_position -= volume
            self.hedge_order(Side.BUY, self.best_ask_price_fut, volume)

    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        """Called when the status of one of your orders changes.

        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees for
        this order. Remember that you pay fees for being a market taker, but
        you receive fees for being a market maker, so fees can be negative.

        If an order is cancelled its remaining volume will be zero.
        """
        self.logger.info("received order status for order %d with fill volume %d remaining %d and fees %d",
                         client_order_id, fill_volume, remaining_volume, fees)

        if remaining_volume == 0:
            self.last_etf_order = None

    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your hedge orders is filled.

        The price is the average price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        self.logger.info("received hedge filled for order %d with average price %d and volume %d", client_order_id,
                         price, volume)

        if self.last_fut_order.order_id != client_order_id:
            return

        # update future position
        if self.last_fut_order.side == Side.BUY:
            self.fut_position += volume
        if self.last_fut_order.side == Side.SELL:
            self.fut_position -= volume

    def on_trade_ticks_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                               ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically when there is trading activity on the market.

        The five best ask (i.e. sell) and bid (i.e. buy) prices at which there
        has been trading activity are reported along with the aggregated volume
        traded at each of those price levels.

        If there are less than five prices on a side, then zeros will appear at
        the end of both the prices and volumes arrays.
        """
        self.logger.info("received trade ticks for instrument %d with sequence number %d", instrument,
                         sequence_number)

    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        """Called when the exchange detects an error.

        If the error pertains to a particular order, then the client_order_id
        will identify that order, otherwise the client_order_id will be zero.
        """
        self.logger.warning("error with order %d: %s", client_order_id, error_message.decode())
        if client_order_id != 0 and self.last_etf_order and self.last_etf_order.order_id == client_order_id:
            self.on_order_status_message(client_order_id, 0, 0, 0)
