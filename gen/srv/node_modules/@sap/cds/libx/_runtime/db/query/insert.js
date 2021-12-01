const { hasDeepInsert, getDeepInsertCQNs, cleanEmptyCompositionsOfMany } = require('../../common/composition')
const { getFlatArray, processCQNs } = require('../utils/deep')
const { timestampToISO } = require('../data-conversion/timestamp')

const insert = executeInsertCQN => async (model, dbc, query, req) => {
  const { user, locale, timestamp } = req
  const isoTs = timestampToISO(timestamp)

  if (hasDeepInsert(model && model.definitions, query)) {
    const cqns = getFlatArray(getDeepInsertCQNs(model && model.definitions, query))

    // return array of individual results
    if (cqns.length === 0) return []
    const results = await processCQNs(executeInsertCQN, cqns, model, dbc, user, locale, isoTs)
    return getFlatArray(results)
  }

  cleanEmptyCompositionsOfMany(model && model.definitions, query)
  return executeInsertCQN(model, dbc, query, user, locale, isoTs)
}

module.exports = insert
