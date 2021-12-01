const cds = require('../cds')
const search2cqn4sql = require('./search2cqn4sql')

function searchHandler(req) {
  // REVISIT: remove feature toggle optimized_search after grace period
  // inject the search2cqn4sql module into the rewrite handler only when
  // the optimized search feature toggle is turned on
  if (!cds.env.features.optimized_search) return

  const query = req.query
  const search = query && query.SELECT && query.SELECT.search

  if (search) {
    Object.defineProperty(req.query, '_searchOptions', { value: { search2cqn4sql, locale: req.locale } })
  }
}

// handlers marked with `._initial = true` run in sequence
searchHandler._initial = true
module.exports = searchHandler
