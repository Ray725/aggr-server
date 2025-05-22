const Exchange = require('../exchange')
const Big = require('big.js')
const { sleep } = require('../helper')

class BITHUMB extends Exchange {
  constructor() {
    super()

    this.id = 'BITHUMB'
    this.locallySubscribedPairs = new Set()
    this.KRW_USD = new Big(0.00072)
    this.endpoints = {
      PRODUCTS: 'https://vfa-microservice.fly.dev/get-bithumb-pairs'
    }
    this.maxConnectionsPerApi = 100 // Kept from original BITHUMB
    this.delayBetweenMessages = 250 // Kept from original BITHUMB, though Upbit uses sleep(250) in subscribe

    this.url = () => 'wss://ws-api.bithumb.com/websocket/v1'
  }

  formatProducts(data) {
    return data.map(product => product.market)
  }

  /**
   * Sub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async subscribe(api, pair) {
    if (!(await super.subscribe.apply(this, arguments))) {
      console.warn(
        `[${this.id}] super.subscribe failed for pair ${pair} on api ${api.id}`
      )
      return false
    }

    if (!this.locallySubscribedPairs.has(pair)) {
      this.locallySubscribedPairs.add(pair)
    }

    const subscriptionMessage = [
      { ticket: `aggr-${this.id}-${Date.now()}` }, // Unique ticket like Upbit
      { type: 'trade', codes: [pair] },
      { format: 'DEFAULT' } // Assuming 'SIMPLE' or 'DEFAULT' is suitable, original used 'DEFAULT'
    ]
    const messageStr = JSON.stringify(subscriptionMessage)

    console.log(
      `[${this.id}] Sending subscription for ${pair} on API ${api.id}. Message: ${messageStr}`
    )
    api.send(messageStr)

    // Upbit uses sleep(250) here. Bithumb had delayBetweenMessages, which is handled by the base class.
    // If an explicit sleep is needed after subscribing for Bithumb specifically:
    await sleep(this.delayBetweenMessages || 250)

    return true
  }

  /**
   * Unsub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async unsubscribe(_, pair) {
    if (!(await super.unsubscribe.apply(this, arguments))) {
      return false
    }

    const removed = this.locallySubscribedPairs.delete(pair)
    if (!removed) {
      console.log(
        `[${this.id}] Pair ${pair} was not in locallySubscribedPairs for unsubscription.`
      )
    } else {
      console.log(
        `[${this.id}] Unsubscribed ${pair}. Total locally subscribed pairs: ${this.locallySubscribedPairs.size}`
      )
    }
    return true // Indicate success
  }

  formatTrade(trade) {
    let tradePrice = new Big(trade.trade_price)

    return {
      exchange: this.id,
      pair: trade.code,
      timestamp: trade.trade_timestamp,
      price: tradePrice.times(this.KRW_USD).toNumber(),
      size: +trade.trade_volume,
      side: trade.ask_bid === 'ASK' ? 'sell' : 'buy'
    }
  }

  onMessage(event, api) {
    let tradeData
    try {
      const messageString =
        event.data instanceof ArrayBuffer
          ? new TextDecoder().decode(event.data)
          : event.data
      console.debug(
        `[${this.id}] Received message on API ${api.id}:`,
        messageString
      )
      tradeData = JSON.parse(messageString)
    } catch (e) {
      console.warn(
        `[${this.id}] Failed to parse JSON message on API ${api.id}:`,
        event.data,
        e
      )
      return
    }

    if (!tradeData) {
      console.debug(
        `[${this.id}] Parsed message is null or undefined on API ${api.id}. Original:`,
        event.data
      )
      return
    }

    if (tradeData.type === 'status' || tradeData.status) {
      console.log(
        `[${this.id}] Received status/info message on API ${api.id}:`,
        tradeData
      )
      return
    }

    if (
      tradeData.type !== 'trade' ||
      tradeData.stream_type !== 'REALTIME'
    ) {
      console.debug(
        `[${this.id}] Received non-trade or non-realtime message on API ${api.id}: Type: ${tradeData.type}, Stream: ${tradeData.stream_type}, Code: ${tradeData.code}. Full data:`,
        tradeData
      )
    }

    if (!this.locallySubscribedPairs.has(tradeData.code)) {
      console.warn(
        `[${this.id}] Received message for non-subscribed pair ${tradeData.code} on API ${api.id}:`,
        tradeData
      )
      return
    }

    return this.emitTrades(api.id, [this.formatTrade(tradeData)])
  }
}

module.exports = BITHUMB
