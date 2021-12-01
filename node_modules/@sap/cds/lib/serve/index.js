const { ProtocolAdapter } = require('./adapters')
const { Service } = require('./factory')
const cds = require ('..')
const _ready = Symbol(), _pending = cds.services._pending || {}

/** @param som - a service name or a model (name or csn) */
function cds_serve (som, _options) { // NOSONAR

  if (Array.isArray(som) && som.length === 1) som = som[0]
  else if (typeof som === 'object' && !is_csn(som)) [som,_options] = [undefined,som]
  const o = {..._options} // we must not modify inbound data

  // 1) Use fluent API to fill in remaining options...
  const fluent = {
    from (model)  { o.from = model;    return this },
    with (impl)   { o.with = impl;     return this },
    at (path)     { o.at   = path;     return this },
    to (protocol) { o.to   = protocol; return this },
  }

  // 2) Ensure options are filled in canonically based on defaults
  const options = Promise.resolve(o).then (o => { // noformat
    if (o.service)     { o.from     ||( o.from    = som); return o }
    if (o.from)        { o.service  ||( o.service = som); return o }
    if (som === 'all') { o.service ='all'; o.from = '*' ; return o }
    if (is_csn(som))   { o.service ='all'; o.from = som ; return o }
    if (is_file(som))  { o.service ='all'; o.from = som ; return o }
    if (is_class(som)) { o.service = som;  o.from = '?' ; return o }
    else               { o.service = som;  o.from = '*' ; return o }
  })

  // 3) Load/resolve the model asynchronously...
  const loaded = options.then (async ({from}=o) => {
    if (!from || from === 'all' || from === '*') from = cds.model || '*'
    if (from.definitions) return from
    if (from === '?') try { return await cds.load('*',o) } catch(e){ return }
    return cds.load(from,o)
  })

  // 4) Pass 1: Construct service provider instances...
  const all=[], provided = loaded.then (csn => { // NOSONAR

    // Shortcut for directly passed service classes
    if (o.service && o.service._is_service_class) {
      const Service = o.service, d = { name: o.service.name }
      return all.push (_new (Service, d,csn,o))
    }

    // Get relevant service definitions from model...
    let {services} = csn = cds.linked (cds.compile.for.odata (csn))
    let specified = o.service
    if (specified && specified !== 'all') {
      // skip services not chosen by o.service, if specified
      if (cds.requires[specified]) specified = cds.requires[specified].service || specified
      services = services.filter (s => s.name.endsWith (specified))
      if (!services.length) throw cds.error (`No such service: '${specified}'`)
    }
    services = services.filter (d => !(
      // skip all services marked to be ignored
      d['@cds.ignore'] || d['@cds.serve.ignore'] ||
      // skip external services, unless asked to mock them and unbound
      cds.requires[d.name] && (!o.mocked || cds.requires[d.name].credentials)
    ))
    if (services.length > 1 && o.at) {
      throw cds.error `You cannot specify 'path' for multiple services`
    }

    // Construct service instances and register them to cds.services
    all.push (...services.map (d => _new (Service,d,csn,o)))
  })

  // 5) Pass 2: Finalize service bootstrapping by calling their impl functions.
  // Note: doing that in a second pass guarantees all own services are in
  // cds.services, so they'll be found when they cds.connect to each others.
  let ready = provided.then (()=> Promise.all (all.map (async srv => {
    srv.init && await srv.prepend (srv.init)
    srv.options.impl && await srv.prepend (srv.options.impl)
    srv[_ready](cds.services[srv.name] = srv)
    return srv
  })))


  // 6) Fluent method to serve constructed providers to express app
  fluent.in = (app) => {
    ready = ready.then (()=>{
      for (let each of all) {
        ProtocolAdapter.serve(each).in(app)
        if (!o.silent) cds.emit ('serving',each)
      }
    })
    return fluent
  }

  // 7) Finally resolve to a single picked provider or a map of all
  fluent.then = (resolve, failed) => ready.then (()=>{
    if (all.length === 0) return resolve()
    let response={}
    for (let each of all) {
      response[each.name] = !each.definition ? each : ProtocolAdapter.serve(each).asRouter()
    }
    if (all.length === 1 && all[0].name.endsWith (o.service)) {
      response = Object.assign (all[0], response)
    }
    return resolve (response)
  }, failed)
  fluent.catch = (e) => ready.catch(e)

  return fluent
}


function _new (Service, d,m,o) {
  const srv = new Service (d.name,m,o)
  const required = cds.requires[d.name]
  if (required) {
    if (required.name) srv.name = required.name
    if (o.mocked) srv.mocked = true
  }
  if (!srv.path) srv.path = path4(srv,o.at)
  cds.service.providers.push (srv)
  _pending[srv.name] = new Promise (r => srv[_ready]=r).finally(()=>{
    delete _pending[srv.name]
    delete srv[_ready]
  })
  return srv
}


/**
 * Resolve a service endpoint path to mount it to as follows...
 * Use _path or def[@path] if given with leading '/' prepended if necessary.
 * Otherwise, use the service definition name with stripped 'Service'
 */
function path4 (srv, _path = (srv.definition || srv)['@path']) {
	if (_path)  return _path.replace(/^[^/]/, c => '/'+c)
	else  return '/' + ( // generate one from the service's name
		/[^.]+$/.exec(srv.name)[0]  //> my.very.CatalogService --> CatalogService
		.replace(/Service$/,'')     //> CatalogService --> Catalog
		.replace(/([a-z0-9])([A-Z])/g, (_,c,C) => c+'-'+C.toLowerCase())  //> ODataFooBarX9 --> odata-foo-bar-x9
		.replace(/_/g,'-')  //> foo_bar_baz --> foo-bar-baz
		.toLowerCase()      //> FOO --> foo
	)
}


const is_csn = x => x && x.definitions
const is_file = x => typeof x === 'string' && !/^[\w$]*$/.test(x)
const is_class = x => typeof x === 'function' && x.prototype && /^class\b/.test(x)

module.exports = Object.assign (cds_serve, { path4 })
