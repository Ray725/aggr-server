const Exchange = require('../exchange')

class HYPERLIQUID extends Exchange {
  constructor() {
    super()
    this.id = 'HYPERLIQUID'
    this.subscriptions = {}
    this.endpoints = {
      PRODUCTS: 'https://vfa-microservice.fly.dev/get-hl-pairs'
    }
    this.maxConnectionsPerApi = 100
    this.delayBetweenMessages = 250
  }

  async getUrl() {
    return `wss://api.hyperliquid.xyz/ws`
  }

  formatProducts(data) {
    return data
  }

  /**
   * Sub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async subscribe(api, pair) {
    if (!(await super.subscribe.apply(this, arguments))) {
      return
    }

    const coin = pair.split('-')[0] // Assuming format COIN-USD
    const subscription = { type: 'trades', coin }
    const message = {
      method: 'subscribe',
      subscription
    }

    api.send(JSON.stringify(message))
    this.subscriptions[pair] = subscription // Store the subscription part for unsubscribing

    return true
  }

  /**
   * Sub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async unsubscribe(api, pair) {
    if (!(await super.unsubscribe.apply(this, arguments))) {
      return false
    }

    const originalSubscription = this.subscriptions[pair]
    if (!originalSubscription) {
      console.warn(`No subscription found for ${pair} to unsubscribe.`)
      return false
    }

    const message = {
      method: 'unsubscribe',
      subscription: originalSubscription
    }

    api.send(JSON.stringify(message))

    return true
  }

  formatTrade(trade, pairCoin) {
    return {
      exchange: this.id,
      pair: `${pairCoin}-USD`,
      timestamp: trade.time,
      price: +trade.px,
      size: +trade.sz,
      side: trade.side === 'B' ? 'buy' : 'sell'
    }
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    // Based on the example, trades are in json.data array
    if (json.channel === 'trades' && Array.isArray(json.data)) {
      const tradesToEmit = []
      for (const tradeData of json.data) {
        // The 'coin' field from the trade data is used to form the pair
        tradesToEmit.push(this.formatTrade(tradeData, tradeData.coin))
      }
      if (tradesToEmit.length > 0) {
        return this.emitTrades(api.id, tradesToEmit)
      }
    }
  }
}

module.exports = HYPERLIQUID
