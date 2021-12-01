/** @typedef {import './serve/Service-api'} Service */
if (global.cds) Object.assign(module,{exports:global.cds}) ; else {

  const facade = class cds extends require('events') {

    get version() { return super.version = require('../package.json').version }
    get env() { return super.env = require('./env') }
    get requires() { return super.requires = this.env.required_services_or_defs }
    get builtin() { return super.builtin = require ('./core') }
    get service() { return super.service = extend (this.builtin.classes.service) .with ({
      /** @param x {(this:Service, srv:Service) => any} */ impl: x=>x,
      /** @type Service[] */ providers: [],
      factory: require ('./serve/factory'),
      bindings: require ('./connect/bindings'),
    })}
    get context() { return require('./req/context').for(this) }
    set context(_){ require('./req/context').for(this,_) }
    get spawn() { return super.spawn = require('./req/context').spawn }

    emit (eve, ...args) {
      if (eve === 'served') return Promise.all (this.listeners(eve).map (l => l.call(this,...args)))
      else return super.emit (eve, ...args)
    }
  }

  const { extend, lazify, lazified } = require ('./lazy')
  const _class = lazy => cds.builtin.classes [lazy]
  const _require = require; require = lazified (module)  // eslint-disable-line

  /** cds is the central facade to all cds functions */
  const cds = module.exports = global.cds = extend (new facade) .with ({

    // Builtin types and classes
    Association:_class,
    Composition:_class,
    entity:_class,
    event:_class,
    type:_class,
    array:_class,
    struct:_class,

    // Model Reflection
    reflect: require ('./core/reflect'),
    linked: require ('./core/reflect'),
    infer: require ('./core/infer'),

    // Loading and Compiling Models
    model: undefined,
    resolve: require ('./compile/resolve'),
    load: require ('./compile/load'), get: lazy => cds.load.parsed,
    parse: require ('./compile/parse'),
    compile: require ('./compile'),
    compiler: require ('./compile/cdsc'),
    deploy: require ('./deploy'),

    // Providing and Consuming Services
    services: new class IterableServices {
      *[Symbol.iterator]() {for (let e in this) yield this[e]}
      get _pending(){ return super._pending = {}}
    },
    serve: require ('./serve'),
    server: require ('../server'),
    connect: require ('./connect'),

    // Core Services API
    Service: require ('./serve/Service-api'),
    EventContext: require ('./req/context'),
    Request: require ('./req/request'),
    Event: require ('./req/event'),
    User: require ('./req/user'),
    ql: require ('./ql'),
    tx: (..._) => (cds.db || cds.Service.prototype) .tx (..._),
    /** @type Service */ db: undefined,

    // Protocols and Periphery
    ApplicationService: lazy => require('../libx/_runtime/cds-services/services/Service.js'),
    MessagingService: lazy => require('../libx/_runtime/messaging/service.js'),
    DatabaseService: lazy => require('../libx/_runtime/db/Service.js'),
    RemoteService: lazy => require('../libx/_runtime/rest/service.js'),
    odata: require('../libx/odata'),

    // Helpers
    localize: require ('./i18n/localize'),
    error: require ('./log/errors'),
    utils: require ('./utils'),
    test: require ('./utils/tests'),
    log: require ('./log'), debug: lazy => cds.log.debug,
    exec: require ('../bin/cds'),
    clone: m => JSON.parse (JSON.stringify(m)),
    lazified, lazify, extend,

    // Configuration & Information
    home: __dirname.slice(0,-4),
    root: lazy => process.cwd(),

  })


  // cds as shortcut to cds.db -> for compatibility only
  extend (cds.__proto__) .with ({
    get entities(){ return (cds.db||_missing).entities },
    transaction: (..._) => (cds.db||_missing).transaction(..._),
    run:         (..._) => (cds.db||_missing).run(..._),
    foreach:     (..._) => (cds.db||_missing).foreach(..._),
    stream:      (..._) => (cds.db||_missing).stream(..._),
    read:        (..._) => (cds.db||_missing).read(..._),
    create:      (..._) => (cds.db||_missing).create(..._),
    insert:      (..._) => (cds.db||_missing).insert(..._),
    update:      (..._) => (cds.db||_missing).update(..._),
    delete:      (..._) => (cds.db||_missing).delete(..._),
    disconnect:  (..._) => (cds.db||_missing).disconnect(..._),
  })
  /** @type Service */ const _missing = new Proxy ({},{
    get:function fn(_,p){ cds.error.no_primary_db(p,fn) }
  })

  // legacy and to be moved stuff -> hidden for tools in cds.__proto__
  extend (cds.__proto__) .with (lazified ({
    /** @deprecated */ in: (cwd) => !cwd ? cds : {__proto__:cds, cwd, env: cds.env.for('cds',cwd) },
    alpha_localized: lazy => require('./compile/etc/_localized'),
    mtx: lazy => require('../bin/mtx/in-cds'),
    build: lazy => require('../bin/build'),
  }))

  // Add global forwards to cds.ql and cds.parse
  const odp = Object.defineProperty, _global = (_,...pp) => pp.forEach (p => odp(global,p,{
    configurable:true, get:()=>{ let v=cds[_][p]; odp(this,p,{value:v}); return v }
  }))
  _global ('ql','SELECT','INSERT','UPDATE','DELETE','CREATE','DROP')
  _global ('parse','CDL','CQL','CXL')

  // Check Node.js version
  if (process.env.CDS_STRICT_NODE_VERSION !== 'false' && !process.env['WORKSPACE_ID']) { // FIXME remove as soon as BAS is ready for the Node version check below
    const v = version => { let vv = version.split('.'); return { version, major: +vv[0], minor: +vv[1] }}
    const required = v('12.18'), given = v(process.version.match(/^v(\d+\.\d+)/)[1])
    if (given.major < required.major || given.major === required.major && given.minor < required.minor) process.exit (process.stderr.write (`
    Node.js v${required.version} or higher is required for @sap/cds.
    Current v${given.version} does not satisfy this.
    \n`) || 1)
  }

  // restore require for subsequent uses in lazy getters
  require = _require  // eslint-disable-line
}
