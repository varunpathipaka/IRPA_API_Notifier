const cds = require('../cds')
const LOG = cds.log('messaging')
const MessagingService = require('./service.js')
const { queueName } = require('./common-utils/naming-conventions')
const optionsApp = require('../common/utils/vcap.js')

class AMQPWebhookMessaging extends MessagingService {
  async init() {
    this.optionsApp = optionsApp
    if (this.options.queue) {
      const queueConfig = { ...this.options.queue }
      delete queueConfig.name
      if (Object.keys(queueConfig).length) this.queueConfig = queueConfig
    }
    this.queueName = queueName(this.options, this.optionsApp)

    cds.once('listening', () => {
      this.startListening()
    })

    return super.init()
  }

  async emit(event, ...etc) {
    const msg = this.message4(event, ...etc)
    const client = this.getClient()
    await this.queued(() => {})()
    return client.emit(msg)
  }

  startListening(opt = {}) {
    if (this.subscribedTopics.size) {
      const management = this.getManagement()
      if (!opt.doNotDeploy) this.queued(management.createQueueAndSubscriptions.bind(management))()
      this.queued(this.listenToClient.bind(this))(async (_topic, _payload, _other, { done, failed }) => {
        const event = _topic
        // Some messaging systems don't adhere to the standard that the payload has a `data` property.
        // For these cases, we interpret the whole payload as `data`.
        let data, headers
        if (typeof _payload === 'object' && 'data' in _payload) {
          data = _payload.data
          headers = { ..._payload }
          delete headers.data
        } else {
          data = _payload
          headers = {}
        }
        const msg = {
          event,
          data,
          headers,
          inbound: true,
          ...(_other || {})
        }
        if (!msg._) msg._ = {}
        msg._.topic = _topic
        try {
          await super.emit(msg)
          done()
        } catch (e) {
          failed()
          LOG._error && LOG.error(e)
        }
      })
    }
  }

  listenToClient(cb) {
    return this.getClient().listen(cb)
  }
}

module.exports = AMQPWebhookMessaging
