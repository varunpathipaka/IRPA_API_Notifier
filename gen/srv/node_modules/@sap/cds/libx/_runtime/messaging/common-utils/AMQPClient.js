const cds = require('../../cds.js')
const LOG = cds.log('messaging')
const ClientAmqp = require('@sap/xb-msg-amqp-v100').Client
const { connect, disconnect } = require('./connections')

const _JSONorString = string => {
  try {
    return JSON.parse(string)
  } catch (e) {
    return string
  }
}

const addDataListener = (client, queue, prefix, cb) =>
  new Promise((resolve, reject) => {
    const source = `${prefix}${queue}`
    client
      .receiver(queue)
      .attach(source)
      .on('data', async raw => {
        const buffer = raw.payload && Buffer.concat(raw.payload.chunks)
        const payload = buffer && _JSONorString(buffer.toString())
        const topic =
          raw.source &&
          raw.source.properties &&
          raw.source.properties.to &&
          raw.source.properties.to.replace(/^topic:\/*/, '')
        if (!topic) return raw.done()
        await cb(topic, payload, null, { done: raw.done, failed: raw.failed })
      })
      .on('subscribed', () => {
        resolve()
      })
  })

const sender = (client, optionsApp) => client.sender(`${optionsApp.appName}-${optionsApp.appID}`).attach('')

const emit = ({ data, event: topic, headers = {} }, sender, prefix) =>
  new Promise((resolve, reject) => {
    LOG._info && LOG.info('Emit', { topic })
    const message = { ...headers, data }
    const payload = { chunks: [Buffer.from(JSON.stringify(message))], type: 'application/json' }
    const msg = {
      done: resolve,
      failed: reject,
      payload,
      target: {
        properties: {
          to: `${prefix}${topic}`
        }
      }
    }
    sender.write(msg)
  })

class AMQPClient {
  constructor({ optionsAMQP, optionsApp, queueName, prefix, keepAlive = true }) {
    this.optionsAMQP = optionsAMQP
    this.optionsApp = optionsApp
    this.queueName = queueName
    this.prefix = prefix
    this.keepAlive = keepAlive
  }

  connect() {
    this.client = new ClientAmqp(this.optionsAMQP)
    this.sender = sender(this.client, this.optionsApp)
    return connect(this.client, this.keepAlive)
  }

  async disconnect() {
    if (this.client) {
      await disconnect(this.client)
      delete this.client
    }
  }

  async emit(msg) {
    if (!this.client) await this.connect()
    await emit(msg, this.sender, this.prefix.topic)
    if (!this.keepAlive) return this.disconnect()
  }

  listen(cb) {
    return addDataListener(this.client, this.queueName, this.prefix.queue, cb)
  }
}

module.exports = AMQPClient
