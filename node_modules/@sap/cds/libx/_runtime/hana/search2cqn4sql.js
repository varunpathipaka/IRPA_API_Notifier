const { computeColumnsToBeSearched } = require('../cds-services/services/utils/columns')
const searchToLike = require('../common/utils/searchToLike')
const { isContainsPredicateSupported, searchToContains } = require('./searchToContains')

/**
 * Computes a CQN expression for a search query.
 *
 * For performance reasons, search queries over localized elements use the `CONTAINS` predicate in the `WHERE` clause
 * of a `SELECT` statement instead of the `LIKE` predicate when possible. The `LIKE` predicate might cause a high CPU
 * load on SAP HANA servers because the SAP HANA optimizer cannot push down `LIKE` expressions to the column storage
 * layer. As a result, search queries over large data sets are slow.
 *
 * The `CONTAINS` predicates with exact search option (default behavior) is rendered as `LIKE` by the HANA optimizer.
 * But in contrast to the explicitly written `LIKE ?`, the parameter is already resolved to its concrete value, making
 * it better optimizable by the HANA optimizer.
 *
 * @param {object} query The CQN object
 * @param {import('@sap/cds-compiler/lib/api/main').CSN} entity The target entity for the search query
 * @param {import('../types/api').search2cqnOptions} [options]
 * @returns {object} The modified CQN object
 */
const search2cqn4sql = (query, entity, options) => {
  const cqnSearchPhrase = query.SELECT.search
  if (!cqnSearchPhrase) return query

  let { columns: columnsToBeSearched = computeColumnsToBeSearched(query, entity), locale } = options
  const localizedAssociation = _getLocalizedAssociation(entity)

  // If the localized association is defined for the target entity,
  // there should be at least one localized element.
  const resolveLocalizedTextsAtRuntime = !!localizedAssociation

  // suppress the localize handler from redirecting the query's target to the localized view
  Object.defineProperty(query, '_suppressLocalization', { value: true })

  if (resolveLocalizedTextsAtRuntime) {
    const onCondition = entity._relations[localizedAssociation.name].join(localizedAssociation.target, entity.name)

    // replace $user_locale placeholder with the user locale or the HANA session context
    onCondition[onCondition.length - 2] = { val: locale || "SESSION_CONTEXT('LOCALE')" }

    // inner join the target table with the _texts table (the _texts table contains
    // the translated texts)
    const localizedEntityName = localizedAssociation.target
    query.join(localizedEntityName).on(onCondition)

    // prevent SQL ambiguity error for columns with the same name
    columnsToBeSearched = _addAliasToColumns(query, entity, columnsToBeSearched)
  } // else --> resolve localized texts via localized view (default)

  const useContains = isContainsPredicateSupported(query)
  let expression

  if (useContains) {
    expression = searchToContains(cqnSearchPhrase, columnsToBeSearched)
  } else {
    // No CONTAINS optimization possible. The search implementation for localized
    // texts falls back to the LIKE predicate.
    expression = searchToLike(cqnSearchPhrase, columnsToBeSearched)
  }

  // REVISIT: find out here if where or having must be used
  query._aggregated ? query.having(expression) : query.where(expression)
  return query
}

const _getLocalizedAssociation = entity => {
  const associations = entity.associations
  return associations && associations.localized
}

// The inner join modifies the original SELECT ... FROM query and adds ambiguity,
// therefore add the table/entity name (as a preceding element) to the columns ref
// to prevent a SQL ambiguity error.
const _addAliasToColumns = (query, entity, columnsToBeSearched) => {
  const localizedEntityName = _getLocalizedAssociation(entity).target
  const elements = entity.elements
  const entityName = entity.name
  const _addAliasToColumn = (entityName, localizedEntityName, elements) => column => {
    const columnRef = column.ref
    if (!columnRef) return column
    const columnName = columnRef[0]
    const localizedElement = elements[columnName].localized
    const targetEntityName = localizedElement ? localizedEntityName : entityName
    return { ref: [targetEntityName, columnName] }
  }

  query.SELECT.columns = query.SELECT.columns.map(_addAliasToColumn(entityName, localizedEntityName, elements))
  const columns = columnsToBeSearched.map(_addAliasToColumn(entityName, localizedEntityName, elements))

  if (query.SELECT.groupBy) {
    query.SELECT.groupBy = query.SELECT.groupBy.map(_addAliasToColumn(entityName, localizedEntityName, elements))
  }

  return columns
}

module.exports = search2cqn4sql
