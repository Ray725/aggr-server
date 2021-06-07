const Exchange = require('../exchange')
const { sleep } = require('../helper')

class BinanceFutures extends Exchange {
  constructor(options) {
    super(options)

    this.id = 'BINANCE_FUTURES'
    this.lastSubscriptionId = 0
    this.subscriptions = {}

    this.endpoints = {
      PRODUCTS: ['https://fapi.binance.com/fapi/v1/exchangeInfo', 'https://dapi.binance.com/dapi/v1/exchangeInfo'],
    }

    this.options = Object.assign(
      {
        url: (pair) => {
          if (this.dapi[pair]) {
            return 'wss://dstream.binance.com/ws'
          } else {
            return 'wss://fstream.binance.com/ws'
          }
        },
      },
      this.options
    )
  }

  formatProducts(response) {
    const products = []
    const specs = {}
    const dapi = {}

    for (const data of response) {
      const type = ['fapi', 'dapi'][response.indexOf(data)]

      for (const product of data.symbols) {
        if ((product.contractStatus && product.contractStatus !== 'TRADING') || (product.status && product.status !== 'TRADING')) {
          continue
        }

        const symbol = product.symbol.toLowerCase()

        if (type === 'dapi') {
          dapi[symbol] = true
        }

        if (product.contractSize) {
          specs[symbol] = product.contractSize
        }

        products.push(symbol)
      }
    }

    return {
      products,
      specs,
      dapi,
    }
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

    this.subscriptions[pair] = ++this.lastSubscriptionId

    const params = [pair + '@trade', pair + '@forceOrder']

    api.send(
      JSON.stringify({
        method: 'SUBSCRIBE',
        params,
        id: this.subscriptions[pair],
      })
    )

    // BINANCE: WebSocket connections have a limit of 5 incoming messages per second.
    await sleep(250)
  }

  /**
   * Sub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async unsubscribe(api, pair) {
    if (!(await super.unsubscribe.apply(this, arguments))) {
      return
    }

    const params = [pair + '@trade', pair + '@forceOrder']

    api.send(
      JSON.stringify({
        method: 'UNSUBSCRIBE',
        params,
        id: this.subscriptions[pair],
      })
    )

    delete this.subscriptions[pair]

    // BINANCE: WebSocket connections have a limit of 5 incoming messages per second.
    await sleep(250)
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (!json) {
      return
    } else {
      if (json.e === 'trade' && json.X !== 'INSURANCE_FUND') {
        let size = +json.q

        const symbol = json.s.toLowerCase()

        if (typeof this.specs[symbol] === 'number') {
          size = (size * this.specs[symbol]) / json.p
        }

        return this.emitTrades(api.id, [
          {
            exchange: this.id,
            pair: symbol,
            timestamp: json.T,
            price: +json.p,
            size: size,
            side: json.m ? 'sell' : 'buy',
          },
        ])
      } else if (json.e === 'forceOrder') {
        let size = +json.o.q

        const symbol = json.o.s.toLowerCase()

        if (typeof this.specs[symbol] === 'number') {
          size = (size * this.specs[symbol]) / json.o.p
        }

        return this.emitLiquidations(api.id, [
          {
            exchange: this.id,
            pair: symbol,
            timestamp: json.o.T,
            price: +json.o.p,
            size: size,
            side: json.o.S === 'BUY' ? 'buy' : 'sell',
            liquidation: true,
          },
        ])
      }
    }
  }
}

module.exports = BinanceFutures
