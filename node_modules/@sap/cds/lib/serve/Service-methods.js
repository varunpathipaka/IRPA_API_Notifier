
module.exports = (srv) => {
  for (const each of srv.operations) {
    add_handler_for (srv, each)
  }
  for (const each of srv.entities) {
    for (const a in each.actions) {
      add_handler_for (srv, each.actions[a])
    }
  }
}

const add_handler_for = (srv, def) => {
  const event = def.name.match(/\w*$/)[0]
  const method = srv[event]
  if (method) {
    // use existing method as handler
    if (method._handled || method.name in srv.__proto__) return
    srv.on (event, ({data}) => {
      const args = []; if (def.parent) args.push (def.parent)
      for (let p in data) args.push(data[p])
      return method.apply (srv,args)
    })
    method._handled = true
  } else {
    // add method to emit request
    const method = srv[event] = function (...args) {
      const [$] = args, target = this.entities [ $ && $.name ? $.name.match(/\w*$/)[0] : $ ]
      const data = {}
      if (target) { //> it's a bound action
        def = target.actions[event]; args.shift()
        // In case of bound actions, first argument(s) are expected to be the target's primary key,
        // which we need to fill in to named properties in data
        // object variant of keys, ensure keys are present in args[0] because of complex param
        if (typeof args[0] === 'object' && Object.keys(target.keys).every(k => k in args[0])) {
          const keys = args.shift()
          for (const p in target.keys)  {
            data[p] = keys[p]
          }
        } else { // positional
          for (let p in target.keys)  {
            data[p] = args.shift()
          }
        }
        
      }

      // object variant of params, ensure at least one param is present in args[0]
      // REVISIT: still not bullet proof, but parameters might be optional
      if ( args[0] !== null && typeof args[0] === 'object' && args.length === 1 && Object.keys(def.params).some(p => p in args[0])) {
        const params = args.shift()
        for (const p in def.params)  {
          data[p] = params[p]
        }
      } else { // positional
        for (let p in def.params)  {
          data[p] = args.shift()
        }
      }

      // REVISIT: What happens with name clashes of keys and params?
      return this.send ({ event, target, data })
    }
    method._handled = true
  }
}
