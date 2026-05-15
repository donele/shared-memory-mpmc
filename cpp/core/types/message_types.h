#pragma once

#include <cstdint>
#include <iostream>

namespace sgt {

namespace msg_type {
    const uint16_t PACKET_HEADER = 0;
    const uint16_t PACKET_BEGIN = 1;
    const uint16_t PACKET_END = 2;
    const uint16_t INCREMENTAL_L3 = 3;
    const uint16_t INCREMENTAL_L2 = 4;
    const uint16_t BOOK_TICKER = 5;
    const uint16_t TRADE = 6;
    const uint16_t FUNDING_RATE = 7;
    const uint16_t LIQUIDATION = 8;
    const uint16_t TRIGGER = 9;
    const uint16_t EXEC_ORDER_REQUEST = 10;
    const uint16_t ORDER_REQUEST = 11;
    const uint16_t ORDER_UPDATE = 12;
    const uint16_t OPEN_ORDER_UPDATE = 13;
    const uint16_t ORDER_TRADE = 14;
    const uint16_t RESET = 15;
    const uint16_t CLOCK = 16;
    const uint16_t END_TIME = 17;
    const uint16_t SYMBOL_TRADING_STATE = 18;
    const uint16_t CONTROL = 19;
    const uint16_t CONTROL_WITH_DATA = 20;
    const uint16_t ROLLING_SNAPSHOT = 21;
    const uint16_t BALANCE = 22;
    const uint16_t POSITION = 23;
    const uint16_t NET_EXPOSURE = 24;
    const uint16_t TRADE_CLUSTER = 25;
    const uint16_t RISK_UPDATE_EXCHANGE = 26;
    const uint16_t RISK_UPDATE_PORTFOLIO = 27;
    const uint16_t RISK_UPDATE_EXCHANGE_END = 28;
    const uint16_t RISK_POSITION_UPDATE = 29;
    const uint16_t RISK_BALANCE_UPDATE = 30;
    const uint16_t STATUS = 31;
    const uint16_t STATUS_REQUEST = 32;
}

struct BufferWrapper {
    char* data;
    int64_t size;
    uint16_t topic = 0;
    uint16_t strategy_id = 0;
};

}
