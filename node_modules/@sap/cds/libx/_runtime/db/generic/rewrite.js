const { cqn2cqn4sql } = require('../../common/utils/cqn2cqn4sql')
const generateAliases = require('../utils/generateAliases')
const { restoreLink } = require('../../common/utils/resolveView')

const _isLinked = req => {
  if (req.query.INSERT && req.query.INSERT.entries) {
    if (Array.isArray(req.query.INSERT.entries)) return req.data === req.query.INSERT.entries[0]
    else return req.data === req.query.INSERT.entries
  } else if (req.query.UPDATE && req.query.UPDATE.data) {
    return req.data === req.query.UPDATE.data
  }
}

function handler(req) {
  // REVISIT: req.target._unresolved for join queries
  if (!this.model || typeof req.query === 'string' /* || !req.target || req.target._unresolved */) {
    return
  }

  const streaming = req.query._streaming
  const validationQuery = req.query._validationQuery

  // for restore link to req.data
  const linked = _isLinked(req)

  // convert to sql cqn
  req.query = cqn2cqn4sql(req.query, this.model, { service: this })

  // REVISIT: should not be necessary
  // restore link to req.data
  if (linked) restoreLink(req)

  if (streaming) req.query._streaming = streaming
  if (validationQuery) req.query._validationQuery = validationQuery

  generateAliases(req.query)
}

handler._initial = true
module.exports = handler
