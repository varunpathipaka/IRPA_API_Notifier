const { computeColumnsToBeSearched } = require('../../../../libx/_runtime/cds-services/services/utils/columns')
const searchToLike = require('./searchToLike')

// convert $search system query option to WHERE/HAVING clause using
// the operator LIKE or CONTAINS
const search2cqn4sql = (query, model, options) => {
  const { search2cqn4sql, targetName = query.SELECT.from.ref[0] } = options
  const entity = model.definitions[targetName]
  const columns = computeColumnsToBeSearched(query, entity)

  // Call custom (optimized search to cqn for sql implementation) that tries
  // to optimize the search behavior for a specific database service.
  // Note: $search query option combined with $filter is not currently optimized
  if (typeof search2cqn4sql === 'function' && !query.SELECT.where) {
    const search2cqnOptions = { columns, locale: options.locale }
    return search2cqn4sql(query, entity, search2cqnOptions)
  }

  const cqnSearchPhrase = query.SELECT.search
  const expression = searchToLike(cqnSearchPhrase, columns)

  // REVISIT: find out here if where or having must be used
  query._aggregated ? query.having(expression) : query.where(expression)
}

module.exports = search2cqn4sql
