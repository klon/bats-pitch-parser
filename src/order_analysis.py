#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys, time
from collections import defaultdict
from pitch import *


class Order(object):
    
    def __init__(self, order_id, symbol, side, size, price):
        self.order_id, self.symbol, self.side, self.size, self.price = order_id, symbol, side, size, price
        self.entry_at_top = False
        self.entry_time = None
        self.exit_time = None

    def __repr__(self):
        return '%s %s' % (self.__class__.__name__, self.__dict__)
    
    def top_of_book_duration(self):
        return self.exit_time - self.entry_time

class NaiveOrderBookOld(object):
    
    def __init__(self, symbol, max_price=1000000):
        self.symbol = symbol
        self.max_price = max_price
        self.orders_by_price = [[] for _ in xrange(max_price)]
        self.ask_min = max_price
        self.bid_max = 0

    def __repr__(self):
        return '%s %s %s<->%s' % (self.__class__.__name__, self.symbol, self.bid_max, self.ask_min)

    def add_order(self, order, msg_time):
        if order.entry_time is None:
            order.entry_time = msg_time
        order_price = order.price
        orders = self.orders_by_price[order_price]
        orders.append(order)
        if order.side == 'B':
            if order_price >= self.bid_max:
                order.entry_at_top = True
            if order_price > self.bid_max:
                if self.bid_max > 0:
                    for o in self.orders_by_price[self.bid_max]:
                        if o.exit_time is None:
                            o.exit_time = msg_time
                self.bid_max = order_price
        else:
            if order_price <= self.ask_min:
                order.entry_at_top = True
            if order_price < self.ask_min:
                if self.ask_min < self.max_price:
                    for o in self.orders_by_price[self.ask_min]:
                        if o.exit_time is None:
                            o.exit_time = msg_time
                self.ask_min = order_price

    def remove_order(self, order, msg_time):
        if order.exit_time is None:
            order.exit_time = msg_time
        order_price = order.price
        orders = self.orders_by_price[order_price]
        orders.remove(order)
        if order.side == 'B':
            if order_price >= self.bid_max:
                p = order_price
                while p > 0 and len(self.orders_by_price[p]) == 0:
                    p -= 1
                self.bid_max = p
        else:
            if order_price <= self.ask_min:
                p = order_price
                while p < self.max_price and len(self.orders_by_price[p]) == 0:
                    p += 1
                self.ask_min = p
                
class NaiveOrderBook(object):
    
    def __init__(self, symbol):
        self.symbol = symbol
        self.bids_by_price = defaultdict(list)
        self.asks_by_price = defaultdict(list)
        self.ask_min = sys.maxint
        self.bid_max = 0

    def __repr__(self):
        return '%s %s %s<->%s' % (self.__class__.__name__, self.symbol, self.bid_max, self.ask_min)

    def add_order(self, order, msg_time):
        if order.entry_time is None:
            order.entry_time = msg_time
        order_price = order.price
        if order.side == 'B':
            self.bids_by_price[order_price].append(order)
            if order_price >= self.bid_max:
                order.entry_at_top = True
            if order_price > self.bid_max:
                for o in self.bids_by_price[self.bid_max]:
                    if o.exit_time is None:
                        o.exit_time = msg_time
                self.bid_max = order_price
        else:
            self.asks_by_price[order_price].append(order)
            if order_price <= self.ask_min:
                order.entry_at_top = True
            if order_price < self.ask_min:
                for o in self.asks_by_price[self.ask_min]:
                    if o.exit_time is None:
                        o.exit_time = msg_time
                self.ask_min = order_price

    def remove_order(self, order, msg_time):
        if order.exit_time is None:
            order.exit_time = msg_time
        order_price = order.price
        if order.side == 'B':
            orders = self.bids_by_price[order_price]
            orders.remove(order)
            if len(orders) == 0:
                del self.bids_by_price[order_price]
            if order_price >= self.bid_max:
                prices = sorted(self.bids_by_price.keys(), reverse=True)
                if len(prices):
                    self.bid_max = prices[0]
                else:
                    self.bid_max = 0
        else:
            orders = self.asks_by_price[order_price]
            orders.remove(order)
            if len(orders) == 0:
                del self.asks_by_price[order_price]
            if order_price <= self.ask_min:
                prices = sorted(self.asks_by_price.keys())
                if len(prices):
                    self.ask_min = prices[0]
                else:
                    self.ask_min = sys.maxint

def main():
    start = time.time()
    n = 0
    books = {}
    orders = {}
    abs_time = 0
    
    with PitchMessageReader(sys.stdin) as r:
        while True:
            msg = r.read_message()
            if msg is None:
                break
            if msg.type in (TRADE_L, TRADE_S, TRADE_BREAK, END_OF_SESSION):
                continue
            if msg.type == TIME:
                abs_time = msg.time
                continue
            
            #symbol = getattr(msg, 'symbol', getattr(orders.get(getattr(msg, 'order_id', None)), 'symbol', None))
            #if symbol != 'PGm   ':
            #    continue
            
            #print books.get(symbol)
            #print msg
            n += 1
            msg_time = abs_time + msg.time_offset / 1000000000.0
            
            if msg.type in (ADD_ORDER_L, ADD_ORDER_S):
                order = orders[msg.order_id] = Order(msg.order_id, msg.symbol, msg.side, msg.shares, msg.price)
                book = books.get(order.symbol)
                if book is None:
                    book = books[order.symbol] = NaiveOrderBook(order.symbol)
                book.add_order(order, msg_time)
                continue
            
            order = orders[msg.order_id]
            book = books[order.symbol]
            if msg.type in (MODIFY_ORDER_L, MODIFY_ORDER_S):
                order.size = msg.shares
                if order.price != msg.price:
                    book.remove_order(order, msg_time)
                    order.price = msg.price
                    book.add_order(order, msg_time)
                elif order.size <= 0:
                    book.remove_order(order, msg_time)
                continue
            if msg.type == DELETE_ORDER:
                book.remove_order(order, msg_time)
                continue
            if msg.type == ORDER_EXECUTED:
                order.size -= msg.executed_shares
                if order.size <= 0:
                    book.remove_order(order, msg_time)
                continue
            if msg.type == ORDER_EXECUTED_AT_PRICE_SIZE:
                order.size -= msg.executed_shares
                if msg.remaining_shares <= 0:
                    book.remove_order(order, msg_time)
                continue
            if msg.type in (REDUCE_SIZE_L, REDUCE_SIZE_S):
                order.size -= msg.canceled_shares
                if order.size <= 0:
                    book.remove_order(order, msg_time)
                continue

    duration_by_symbol = defaultdict(float)
    count_by_symbol = defaultdict(int)
    for o in orders.values():
        if o.entry_at_top:
            duration_by_symbol[o.symbol] += o.top_of_book_duration()
            count_by_symbol[o.symbol] += 1
    for s, d in duration_by_symbol.items():
        print s, d / count_by_symbol[s]
        
    print 'Parsed %s message(s) in %s sec(s).' % (n, (time.time() - start))
    
    
if __name__ == '__main__':
    main()
    