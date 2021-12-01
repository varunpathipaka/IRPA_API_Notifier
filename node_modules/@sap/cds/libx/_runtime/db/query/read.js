const { timestampToISO } = require('../data-conversion/timestamp')

function _arrayWithCount(a, count) {
  const _map = a.map
  const map = (..._) => _arrayWithCount(_map.call(a, ..._), count)
  return Object.defineProperties(a, {
    $count: { value: count, enumerable: false, configurable: true, writable: true },
    map: { value: map, enumerable: false, configurable: true, writable: true }
  })
}

function _createCountQuery(query) {
  const _query = JSON.parse(JSON.stringify(query)) // REVISIT: Use query.clone() instead
  _query.SELECT.columns = [{ func: 'count', args: [{ val: 1 }], as: '$count' }]
  delete _query.SELECT.groupBy
  delete _query.SELECT.limit
  delete _query.SELECT.orderBy // not necessary to keep that
  // Also change columns in sub queries
  if (_query.SELECT.from.SET) {
    _query.SELECT.from.SET.args.forEach(subCountQuery => {
      subCountQuery.SELECT.columns = [{ val: 1 }]
    })
  }
  if (query.SELECT._4odata) _query.SELECT._4odata = true
  return _query
}

const countValue = countResult => {
  if (countResult._counted_ != null) return countResult._counted_
  if (countResult.$count != null) return countResult.$count
}

const read = (executeSelectCQN, executeStreamCQN) => (model, dbc, query, req) => {
  const { user, locale, timestamp } = req
  const isoTs = timestampToISO(timestamp)

  if (query._streaming) {
    if (!query.SELECT || (query.SELECT && (!query.SELECT.columns || query.SELECT.columns.length !== 1))) {
      req.reject(400)
    }
    return executeStreamCQN(model, dbc, query, user, locale, isoTs)
  }

  // needed in case of expand
  if (query._target !== req.target) query._target = req.target

  if (query.SELECT.count) {
    if (query.SELECT.limit) {
      const countQuery = _createCountQuery(query)
      const countResultPromise = executeSelectCQN(model, dbc, countQuery, user, locale, isoTs)
      if (query.SELECT.limit.rows && query.SELECT.limit.rows.val === 0) {
        // We don't need to perform our result query
        return countResultPromise.then(countResult => _arrayWithCount([], countValue(countResult[0])))
      } else {
        const resultPromise = executeSelectCQN(model, dbc, query, user, locale, isoTs)
        return Promise.all([countResultPromise, resultPromise]).then(([countResult, result]) =>
          _arrayWithCount(result, countValue(countResult[0]))
        )
      }
    } else {
      return executeSelectCQN(model, dbc, query, user, locale, isoTs).then(result =>
        _arrayWithCount(result, result.length)
      )
    }
  }

  return executeSelectCQN(model, dbc, query, user, locale, isoTs)
}

module.exports = read
