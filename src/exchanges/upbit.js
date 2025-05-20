const Exchange = require('../exchange')
const Big = require('big.js')
const { sleep } = require('../helper')

class Upbit extends Exchange {
  constructor() {
    super()

    this.id = 'UPBIT'
    this.locallySubscribedPairs = new Set()
    this.KRW_USD = new Big(0.00072)

    this.endpoints = {
      PRODUCTS: 'https://api.upbit.com/v1/market/all'
    }

    this.url = () => 'wss://api.upbit.com/websocket/v1'
  }

  formatProducts(data) {
    return data.map(product => product.market)
  }

  async subscribe(api, pair) {
    if (!(await super.subscribe.apply(this, arguments))) {
      console.warn(`[${this.id}] super.subscribe failed for pair ${pair} on api ${api.id}`);
      return false;
    }

    if (!this.locallySubscribedPairs.has(pair)) {
      this.locallySubscribedPairs.add(pair);
    }

    const subscriptionMessage = [
      { ticket: `aggr-UPBIT-${Date.now()}` },
      { type: 'trade', codes: [pair] },
      { format: 'DEFAULT' }
    ];
    const messageStr = JSON.stringify(subscriptionMessage);

    console.log(`[${this.id}] Sending subscription for ${pair} on API ${api.id}. Message: ${messageStr}`);
    api.send(messageStr);

    await sleep(250);

    return true;
  }

  async unsubscribe(_, pair) {
    if (!(await super.unsubscribe.apply(this, arguments))) {
      return
    }

    const removed = this.locallySubscribedPairs.delete(pair);
    if (!removed) {
      console.log(`[${this.id}] Pair ${pair} was not in locallySubscribedPairs for unsubscription.`);
    } else {
      console.log(`[${this.id}] Unsubscribed ${pair}. Total locally subscribed pairs: ${this.locallySubscribedPairs.size}`);
    }
  }

  formatTrade(trade) {
    const tradePrice = new Big(trade.trade_price)

    return {
      exchange: this.id,
      pair: trade.code,
      timestamp: trade.trade_timestamp,
      price: tradePrice.times(this.KRW_USD).toNumber(),
      size: trade.trade_volume,
      side: trade.ask_bid === 'ASK' ? 'sell' : 'buy'
    }
  }

  onMessage(event, api) {
    let tradeData;
    try {
      const messageString =
        event.data instanceof ArrayBuffer
          ? new TextDecoder().decode(event.data)
          : event.data;
      console.debug(`[${this.id}] Received message on API ${api.id}:`, messageString);
      tradeData = JSON.parse(messageString);
    } catch (e) {
      console.warn(`[${this.id}] Failed to parse JSON message on API ${api.id}:`, event.data, e);
      return;
    }

    if (!tradeData) {
      console.debug(`[${this.id}] Parsed message is null or undefined on API ${api.id}. Original:`, event.data);
      return;
    }

    if (tradeData.type === 'status' || tradeData.status) {
      console.log(`[${this.id}] Received status/info message on API ${api.id}:`, tradeData);
      return;
    }

    if (
      tradeData.type !== 'trade' ||
      tradeData.stream_type !== 'REALTIME' // Upbit sends 'SNAPSHOT' then 'REALTIME'
    ) {
      console.debug(`[${this.id}] Received non-trade or non-realtime message on API ${api.id}: Type: ${tradeData.type}, Stream: ${tradeData.stream_type}, Code: ${tradeData.code}. Full data:`, tradeData);
    }
    
    if (!this.locallySubscribedPairs.has(tradeData.code)) {
      console.warn(`[${this.id}] Received message for non-subscribed pair ${tradeData.code} on API ${api.id}:`, tradeData);
      return;
    }

    return this.emitTrades(api.id, [this.formatTrade(tradeData)]);
  }
}

module.exports = Upbit
