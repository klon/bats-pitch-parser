#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys, time
from struct import unpack

TIME = 0x20
ADD_ORDER_L = 0x21
ADD_ORDER_S = 0x22
ORDER_EXECUTED = 0x23
ORDER_EXECUTED_AT_PRICE_SIZE = 0x24
REDUCE_SIZE_L = 0x25
REDUCE_SIZE_S = 0x26
MODIFY_ORDER_S = 0x27
MODIFY_ORDER_L = 0x28
DELETE_ORDER = 0x29
TRADE_L = 0x2A
TRADE_S = 0x2B
TRADE_BREAK = 0x2C
END_OF_SESSION = 0x2D

MESSAGE_DESCRIPTIONS = {0x20: 'Time', 0x21: 'Add Order - Long', 0x22: 'Add Order - Short', 0x23: 'Order Executed', 0x24: 'Order Executed at Price/Size',
                        0x25: 'Reduce Size - Long', 0x26: 'Reduce Size - Short', 0x27: 'Modify Order - Long', 0x28: 'Modify Order - Short',
                        0x29: 'Delete Order', 0x2A: 'Trade - Long', 0x2B: 'Trade - Short', 0x2C: 'Trade Break', 0x2D: 'End of Session',}

class PitchMessage(object):

    def __init__(self, type, payload):
        self.type = type
        self.payload = payload
        if type == TIME:    #0x20 Time
            self.time = unpack('<L', payload)[0]
        elif type == ADD_ORDER_L: #0x21 Add Order - Long
            self.time_offset, self.order_id, self.side_indicator, self.shares, self.symbol, self.price = unpack('<LQcL6sQ', payload)
        elif type == ADD_ORDER_S: #0x22 Add Order - Short
            self.time_offset, self.order_id, self.side_indicator, self.shares, self.symbol, self.price = unpack('<LQcH6sH', payload)
        elif type == ORDER_EXECUTED: #0x23 Order Executed
            self.time_offset, self.order_id, self.executed_shares, self.execution_id = unpack('<LQLQ', payload)
        elif type == ORDER_EXECUTED_AT_PRICE_SIZE: #0x24 Order Executed at Price/Size
            self.time_offset, self.order_id, self.executed_shares, self.remaining_shares, self.execution_id, self.price = unpack('<LQLLQQ', payload)
        elif type == REDUCE_SIZE_L: #0x25 Reduce Size - Long
            self.time_offset, self.order_id, self.cancelled_shares = unpack('<LQL', payload)
        elif type == REDUCE_SIZE_S: #0x26 Reduce Size - Short
            self.time_offset, self.order_id, self.cancelled_shares = unpack('<LQH', payload)
        elif type == MODIFY_ORDER_S: #0x27 Modify Order - Long
            self.time_offset, self.order_id, self.shares, self.price = unpack('<LQLQ', payload)
        elif type == MODIFY_ORDER_L: #0x28 Modify Order - Short
            self.time_offset, self.order_id, self.shares, self.price = unpack('<LQHH', payload)
        elif type == DELETE_ORDER: #0x29 Delete Order
            self.time_offset, self.order_id = unpack('<LQ', payload)
        elif type == TRADE_L: #0x2A Trade - Long
            self.time_offset, self.order_id, self.side_indicator, self.shares, self.symbol, self.price, self.execution_id = unpack('<LQcL6sQQ', payload) 
        elif type == TRADE_S: #0x2B Trade - Short
            self.time_offset, self.order_id, self.side_indicator, self.shares, self.symbol, self.price, self.execution_id = unpack('<LQcH6sHQ', payload) 
        elif type == TRADE_BREAK: #0x2C Trade Break
            self.time_offset, self.execution_id = unpack('<LQ', payload)
        elif type == END_OF_SESSION: #0x2D End of Session
            self.time_offset = unpack('<L', payload)[0]
        else:
            raise ValueError('Invalid message type: %s', type)

    def __repr__(self):
        return '%s: %s' % (MESSAGE_DESCRIPTIONS[self.type].upper(), self.__dict__)
    

class PitchMessageReader(object):
    
    def __init__(self, stream):
        self.stream = stream
        self.available = 0

    def read_message(self):
        if self.available <= 0:
            buf = self.stream.read(8)
            if buf is None or len(buf) < 8:
                return None
            length, count, unit, sequence = unpack('<HBBL', buf)
            self.available = count

        msg_len, msg_type = unpack('<BB', self.stream.read(2))
        msg_payload = self.stream.read(msg_len - 2)
        if msg_payload:
            self.available -= 1
            return PitchMessage(msg_type, msg_payload)
        return None
        
    def close(self):
        self.stream.close()
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.close()
        except:
            pass
        return False

if __name__ == '__main__':
    start = time.time()
    n = 0
    max_price = -1
    min_price = 1E9
    with PitchMessageReader(sys.stdin) as reader:
        while True:
            msg = reader.read_message()
            if msg:
                n += 1
                if hasattr(msg, 'price'):
                    min_price = min(min_price, msg.price)
                    max_price = max(max_price, msg.price)
                print msg
            else:
                break 
    print 'Parsed %s message(s) in %s second(s), min price was %s and max price was %s.' % (n, (time.time() - start), min_price, max_price)
        
