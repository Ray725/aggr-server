const Exchange = require('../exchange')
const Big = require('big.js')

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

    const added = !this.locallySubscribedPairs.has(pair);
    if (added) {
      this.locallySubscribedPairs.add(pair);
      console.log(`[${this.id}] Queued subscription for ${pair}. Total locally subscribed pairs: ${this.locallySubscribedPairs.size}`);
    }

    return true;
  }

  async unsubscribe(_, pair) {
    if (!(await super.unsubscribe.apply(this, arguments))) {
      return
    }

    const removed = this.locallySubscribedPairs.delete(pair);
    if (!removed) {
      // console.log(`[${this.id}] Pair ${pair} was not in locallySubscribedPairs for unsubscription.`);
    } else {
      console.log(`[${this.id}] Unsubscribed ${pair}. Total locally subscribed pairs: ${this.locallySubscribedPairs.size}`);
    }
  }

  formatTrade(trade) {
    const tradePrice = new Big(trade.trade_price)
    const tradeSize = new Big(trade.trade_volume)

    return {
      exchange: this.id,
      pair: trade.code,
      timestamp: trade.trade_timestamp,
      price: tradePrice.times(this.KRW_USD).toNumber(),
      size: tradeSize.times(this.KRW_USD).toNumber(),
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
      // Allow SNAPSHOT messages if they contain trade-like data for initial population,
      // but ensure they are for a subscribed pair.
      if (tradeData.type === 'trade' && tradeData.code && this.locallySubscribedPairs.has(tradeData.code)) {
         // Potentially process snapshot trades if structure is compatible
         // console.log(`[${this.id}] Processing trade snapshot for ${tradeData.code} on API ${api.id}`);
      } else {
        return;
      }
    }
    
    if (!this.locallySubscribedPairs.has(tradeData.code)) {
      console.warn(`[${this.id}] Received message for non-subscribed pair ${tradeData.code} on API ${api.id}:`, tradeData);
      return;
    }

    return this.emitTrades(api.id, [this.formatTrade(tradeData)]);
  }

  onApiCreated(api) {
    console.log(`[${this.id}] API ${api.id} created. WebSocket URL: ${api.ws ? api.ws.url : 'N/A'}`);
    this.startKeepAlive(api, 'PING', 60000); // Standard keep-alive

    const pairsToSubscribe = Array.from(this.locallySubscribedPairs);
    if (pairsToSubscribe.length > 0) {
      const subscriptionMessage = [
        { ticket: `aggr-${api.id}-${Date.now()}` },
        { type: 'trade', codes: pairsToSubscribe },
        { format: 'DEFAULT' }
      ];
      const messageStr = JSON.stringify(subscriptionMessage);
      console.log(`[${this.id}] Sending subscription message to API ${api.id} for pairs (${pairsToSubscribe.length}): ${pairsToSubscribe.join(',')}. Message: ${messageStr}`);
      api.send(messageStr);
    } else {
      console.log(`[${this.id}] API ${api.id} created, but no pairs are currently queued for subscription in locallySubscribedPairs.`);
    }
  }

  onApiRemoved(api) {
    console.log(`[${this.id}] API ${api.id} removed. WebSocket URL: ${api.ws ? api.ws.url : 'N/A'}`);
    this.stopKeepAlive(api);
  }
}

module.exports = Upbit
