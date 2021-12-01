const { getArgumentByName } = require('./utils')
const astToWhere = require('./where')
const astToOrderBy = require('./orderBy')
const astToLimit = require('./limit')
const { ARGUMENT } = require('../../../constants/adapter')

const astToColumns = selections => {
  let columns = []

  for (const selection of selections) {
    const column = { ref: [selection.name.value] }
    if (selection.alias) {
      column.as = selection.alias.value
    }

    if (selection.selectionSet && selection.selectionSet.selections) {
      column.expand = astToColumns(selection.selectionSet.selections)
    }

    const filter = getArgumentByName(selection.arguments, ARGUMENT.FILTER)
    if (filter) {
      column.where = astToWhere(filter).xpr
    }

    const orderBy = getArgumentByName(selection.arguments, ARGUMENT.ORDER_BY)
    if (orderBy) {
      column.orderBy = astToOrderBy(orderBy)
    }

    const top = getArgumentByName(selection.arguments, ARGUMENT.TOP)
    const skip = getArgumentByName(selection.arguments, ARGUMENT.SKIP)
    if (top) {
      column.limit = astToLimit(top, skip)
    }

    columns.push(column)
  }

  return columns
}

module.exports = astToColumns
