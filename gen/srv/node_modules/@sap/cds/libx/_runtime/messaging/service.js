const cds = require('../cds')
const LOG = cds.log('messaging')
const queued = require('./common-utils/queued')

const _topic = declared => declared['@topic'] || declared.name

let usedTopicOnce = false
const _warnAndStripTopicPrefix = event => {
  if (event.startsWith('topic:')) {
    // backwards compatibility
    event = event.replace(/topic:/, '')
    if (!usedTopicOnce) {
      LOG._warn && LOG.warn('The topic prefix `topic:` is deprecated and has no effect. Please remove it.')
    }
    usedTopicOnce = true
  }
  return event
}

// There's currently no mechanism to detect mocked services, this is the best we can do.
class MessagingService extends cds.Service {
  init() {
    // enables queued async operations (without awaiting)
    this.queued = queued()
    this.subscribedTopics = new Map()
    // Only for one central `messaging` service, otherwise all technical services would register themselves
    if (this.name === 'messaging') {
      // listen for all subscriptions to declared events of remote, i.e. connected services
      cds.on('subscribe', (srv, event) => {
        const declared = srv.events[event]
        if (declared && srv.name in cds.requires && !srv.mocked) {
          // we register self-handlers for declared events, which are supposed
          // to be calles by subclasses calling this.dispatch on incoming events
          const topic = _topic(declared)
          this.on(topic, async (msg, next) => {
            const { data, headers } = msg
            await srv.tx(msg).emit({ event, data, headers, __proto__: msg })
            return next()
          })
        }
      })

      // forward all emits for all declared events of local, i.e. served services
      cds.on('serving', srv => {
        for (const declared of srv.events) {
          const event = declared.name.slice(srv.name.length + 1)
          // calls to srv.emit are forwarded to this.emit, which is expected to
          // be overwritten by subclasses to write events to message channel
          const topic = _topic(declared)
          srv.on(event, async (msg, next) => {
            const { data, headers } = msg
            await this.tx(msg).emit({ event: topic, data, headers })
            return next()
          })
        }
      })
    }

    // if outbox is switched on, decorate the emit method to actually do
    // the emit only when the request succeeded
    if (this.options.outbox) {
      const { emit } = this
      this.emit = function (...args) {
        const context = this.context || cds.context
        if (context && typeof context.on === 'function')
          return context.on('succeeded', () =>
            emit.call(this, ...args).catch(e => {
              LOG._error && LOG.error(e)
            })
          )
        return emit.call(this, ...args)
      }
    }

    const { on } = this
    this.on = function (...args) {
      if (Array.isArray(args[0])) {
        const [topics, ...rest] = args
        return topics.map(t => on.call(this, t, ...rest))
      }
      return on.call(this, ...args)
    }
  }

  emit(event, data, headers) {
    const msg = event instanceof cds.Event ? event : new cds.Event(this.message4(event, data, headers))
    return super.emit(msg)
  }

  on(event, cb) {
    const _event = _warnAndStripTopicPrefix(event)
    // save all subscribed topics (not needed for local-messaging)
    this.subscribedTopics.set(this.prepareTopic(_event, true), _event)
    return super.on(_event, cb)
  }

  prepareTopic(topic, _inbound) {
    // In local messaging there's a 'short curcuit' so we must not modify the topic
    if (this.options.local) return topic
    let res = topic
    if (!_inbound && this.options.publishPrefix) res = this.options.publishPrefix + res
    if (_inbound && this.options.subscribePrefix) res = this.options.subscribePrefix + res
    return res
  }

  prepareHeaders(headers, event) {
    if (this.options.format === 'cloudevents') {
      if (!('id' in headers)) headers.id = cds.utils.uuid()
      if (!('type' in headers)) headers.type = event
      if (!('source' in headers)) headers.source = `/default/sap.cap/${process.pid}`
      if (!('time' in headers)) headers.time = new Date().toISOString()
      if (!('datacontenttype' in headers)) headers.datacontenttype = 'application/json'
      if (!('specversion' in headers)) headers.specversion = '1.0'
    }
  }

  message4(event, data, headers) {
    const msg = typeof event === 'object' ? { ...event } : { event, data, headers }
    msg.event = _warnAndStripTopicPrefix(msg.event)
    if (!msg.headers) msg.headers = {}
    if (!msg.inbound) {
      msg.headers = { ...msg.headers } // don't change the original object
      this.prepareHeaders(msg.headers, msg.event)
      msg.event = this.prepareTopic(msg.event, false)
    } else if (this.subscribedTopics) {
      const subscribedEvent =
        this.subscribedTopics.get(msg.event) ||
        (this.wildcarded && this.subscribedTopics.get(this.wildcarded(msg.event)))
      if (!subscribedEvent) throw new Error(`No handler for incoming message with topic '${msg.event}' found.`)
      msg.event = subscribedEvent
    }
    return msg
  }
}

module.exports = MessagingService
