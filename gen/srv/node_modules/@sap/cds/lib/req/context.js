const cds = require ('../index'), { features } = cds.env, { uuid } = cds.utils
const async_events = { succeeded:1, failed:1, done:1 }
const { EventEmitter } = require('events')

/**
 * This is the base class for `cds.Events` and `cds.Requests`,
 * providing the transaction context nature to all instances.
 * Calling `srv.tx()` without args to start new transacions
 * creates direct instances of this base class.
 */
class EventContext {

  toString() { return `${this.event} ${this.path}` }

  static new (data={}, base = cds.context) {
    if (data && base && features.cds_tx_inheritance) {
      let u = _inherit('user', u => typeof u === 'object' ? Object.create(u) : u)
      if (!u || !u.tenant) _inherit('tenant')
      if (!u || !u.locale) _inherit('locale')
    }
    function _inherit (p,fn) {
      if (p in data) return data[p]
      let pd = Reflect.getOwnPropertyDescriptor(base,p); if (!pd) return
      return data[p] = fn ? fn(pd.value) : pd.value
    }
    // IMPORTANT: ensure user is first to be assigned to call setter_
    if (data.user) data = { user:1, ...data }
    return new this (data)
  }

  constructor(_={}) {
    Object.assign (this, this._set('_',_))
  }

  _set (property, value) {
    Object.defineProperty (this, property, {value,writable:true})
    return value
  }



  //
  // Emitting and listening to succeeded / failed / done events
  //

  /** @returns {EventEmitter} */ get emitter() {
    return this._emitter || (this._emitter = this._propagated('emitter') || new EventEmitter)
  }

  async emit (event,...args) {
    if (!this._emitter) return
    if (event in async_events) {
      for (const each of this._emitter.listeners(event)) {
        await each.call(this,...args)
      }
    }
    else return this._emitter.emit (event,...args)
  }

  on (event, listener) {
    return this.emitter.on (event, listener.bind(this))
  }

  once (event, listener) {
    return this.emitter.once (event, listener.bind(this))
  }

  before (event, listener) {
    return this.emitter.prependListener (event, listener.bind(this))
  }


  //
  // The following properties are inherited from root contexts, if exist...
  //

  set context(c) { if (c) super.context = c }
  get context() { return this }

  set id(c) { if (c) super.id = c }
  get id() {
    return this.id = this._propagated('id') || this.headers[ 'x-correlation-id' ] || uuid()
  }

  set tenant(t) {
    if (t) super.tenant = this.user.tenant = t
  }
  get tenant() {
    return this.tenant = this.user.tenant || this._propagated('tenant')
  }

  set user(u) {
    if (!u) return
    if (typeof u === 'string') u = new cds.User(u)
    if (this._.req) Object.defineProperty(u,'_req',{value:this._.req}) // REVISIT: The following is to support req.user.locale
    super.user = u
  }
  get user() {
    return this.user = this._propagated('user') || new cds.User
  }

  set locale(l) {
    if (l) super.locale = this.user.locale = l
  }
  get locale() {
    return this.locale = this.user.locale || this._propagated('locale')
  }

  get timestamp() {
    return super.timestamp = this._propagated('timestamp') || new Date
  }

  set headers(h) { if (h) super.headers = h }
  get headers() {
    if (this._ && this._.req && this._.req.headers) {
      return super.headers = this._.req.headers
    } else {
      const headers={}, outer = this._propagated('headers')
      if (outer) for (let each of EventContext.propagateHeaders) {
        if (each in outer) headers[each] = outer[each]
      }
      return super.headers = headers
    }
  }


  //
  // Connecting to transactions and request hierarchies
  //

  _propagated (p) {
    const ctx = this.context
    if (ctx !== this) return ctx[p]
  }

  set _tx(tx) {
    Object.defineProperty (this,'_tx',{value:tx}) //> allowed only once!
    const ctx = tx.context
    if (ctx && ctx !== this) {
      this.context = ctx
      // REVISIT: Eliminate req._children
      // only collect children if integrity checks are active
      if (features.assert_integrity !== false) {
        const reqs = ctx._children || (ctx._children = {})
        const all = reqs[tx.name] || (reqs[tx.name] = [])
        all.push(this)
      }
    }
  }


  /** REVISIT: remove -> @deprecated */
  set _model(m){ super._model = m }
  get _model() {
    return super._model = this._tx && this._tx.model || this._propagated('_model')
  }
}


/**
 * Continuation Local Storage for cds.context
 */
 const cls = (cds,v) => {

  if (cds.env.features.cls) {

    const { executionAsyncResource:current, createHook } = module.require ('async_hooks')
    const _context = Symbol('cds.context')

    createHook ({ init(id,t,tid, res) {
      const cr = current(); if (!cr) return
      const ctx = cr[_context]
      if (ctx) res[_context] = ctx
    }}).enable()

    Reflect.defineProperty (cds,'context',{ enumerable:1,
      set(v) {
        const cr = current(); if (!cr) return
        const ctx = typeof v !== 'object' ? v : v.context || (v instanceof EventContext ? v : EventContext.new(v,false))
        cr[_context] = ctx
      },
      get() {
        const cr = current(); if (!cr) return undefined
        return cr[_context]
      },
    })

  } else {

    Reflect.defineProperty (cds,'context',{ enumerable:1,
      get:()=> undefined,
      set:()=> {},
    })

  }

  return v ? cds.context = v : cds.context
}


function cds_spawn (o,fn) {
  if (typeof o === 'function') [ fn, o ] = [ o, fn ]
  if (!o) o = {}
  const em = new EventEmitter()
  const fx = async ()=> {
    // create a new transaction for each run of the background job
    // which inherits from the current event context by default
    const tx = cds.context = cds.tx({...o})
    try {
      const res = await fn(tx)
      await tx.commit()
      for (const handler of em.listeners('succeeded')) await handler(res)
      for (const handler of em.listeners('done')) await handler()
    } catch(e) {
      console.trace (`ERROR occured in background job:`, e) // eslint-disable-line no-console
      await tx.rollback()
      for (const handler of em.listeners('failed')) await handler(e)
      for (const handler of em.listeners('done')) await handler()
    }
  }
  const timer = (
    (o && o.after) ? setTimeout (fx, o.after) :
    (o && o.every) ? setInterval (fx, o.every) :
    setImmediate (fx) )
  return Object.assign(em, { timer })
}


module.exports = Object.assign (EventContext,{
  /** @type {(cds,v)=>EventContext} */ for: cls,
  spawn: cds_spawn,
  propagateHeaders: [ 'x-correlation-id' ]
})
