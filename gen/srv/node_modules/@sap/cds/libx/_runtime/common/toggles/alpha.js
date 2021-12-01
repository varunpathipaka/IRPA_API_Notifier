/*
 * additions for feature toggles
 */
module.exports = cds => {
  if (!cds.env.features.alpha_toggles) return (req, res, next) => next()

  return (req, res, next) => {
    // attach _getHash helper to mtx
    if (!cds.mtx._getHash) {
      cds.mtx._getHash = req => {
        let hash = req.tenant
        if (req.features) hash += ':' + Object.keys(req.features).join(';')
        return hash
      }
    }

    // inject features from dwc header
    const fth = req.headers['dwc-product-configuration']
    if (fth) {
      const { features } = JSON.parse(Buffer.from(fth, 'base64').toString('utf-8'))
      req.features = features
        .sort((a, b) => a.name.localeCompare(b.name))
        .reduce((acc, cur) => {
          if (cur.enabled) acc[cur.name] = 1
          return acc
        }, {})
    }

    // extend @sap/cds's dispatch listener
    const {
      eventEmitter: mtx,
      events: { TENANT_UPDATED }
    } = cds.mtx
    req.on('dispatch', req => {
      // clear ext map for next request as soon as it was dispatched
      mtx.emit(TENANT_UPDATED, cds.mtx._getHash(req._.req))
      // set cds.context.features here so we don't need to touch @sap/cds
      cds.context.features = req._.req.features
    })

    next()
  }
}
