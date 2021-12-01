const cds = require('../../cds')
const LOG = cds.log('messaging')

const MAX_WAITING_TIME = 1480000

const _waitingTime = x => (x > 18 ? MAX_WAITING_TIME : (Math.pow(1.5, x) + Math.random()) * 1000)

const _connectUntilConnected = (client, x) => {
  setTimeout(() => {
    connect(client, true).catch(e => {
      LOG._warn && LOG.warn(`Connection to Enterprise Messaging Client lost: Unsuccessful attempt to reconnect (${x}).`)
      _connectUntilConnected(client, x + 1)
    })
  }, _waitingTime(x))
}

const connect = (client, keepAlive) => {
  return new Promise((resolve, reject) => {
    client
      .once('connected', function () {
        client.removeAllListeners('error')

        client.on('error', err => {
          if (LOG._error) {
            err.message = 'Client error: ' + err.message
            LOG.error(err)
          }
          client.disconnect()
        })

        if (keepAlive) {
          client.once('disconnected', () => {
            client.removeAllListeners('error')
            client.removeAllListeners('connected')
            _connectUntilConnected(client, 0)
          })
        }

        resolve(this)
      })
      .once('error', err => {
        client.removeAllListeners('connected')
        reject(err)
      })

    client.connect()
  })
}

const disconnect = client => {
  return new Promise((resolve, reject) => {
    client.removeAllListeners('disconnected')
    client.removeAllListeners('connected')
    client.removeAllListeners('error')

    client.once('disconnected', () => {
      client.removeAllListeners('error')
      resolve()
    })
    client.once('error', err => {
      client.removeAllListeners('disconnected')
      reject(err)
    })

    client.disconnect()
  })
}

module.exports = {
  connect,
  disconnect
}
