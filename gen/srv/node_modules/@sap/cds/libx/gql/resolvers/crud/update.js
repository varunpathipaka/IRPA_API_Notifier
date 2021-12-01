const { ARGUMENT } = require('../../constants/adapter')
const { getArgumentByName, astToColumns, astToWhere, astToEntries } = require('../parse/ast2cqn')
const { entriesStructureToEntityStructure } = require('./utils')

module.exports = async (service, entityFQN, selection) => {
  const filter = getArgumentByName(selection.arguments, ARGUMENT.FILTER)

  let queryBeforeUpdate = service.read(entityFQN)
  queryBeforeUpdate.columns(astToColumns(selection.selectionSet.selections))

  if (filter) {
    queryBeforeUpdate.where(astToWhere(filter))
  }

  const resultBeforeUpdate = await service.tx(tx => tx.run(queryBeforeUpdate))

  let query = service.update(entityFQN)

  if (filter) {
    query.where(astToWhere(filter))
  }

  const input = getArgumentByName(selection.arguments, ARGUMENT.INPUT)
  const entries = entriesStructureToEntityStructure(service, entityFQN, astToEntries(input))
  query.with(entries)

  const result = await service.tx(tx => tx.run(query))

  // Merge selected fields with updated data
  return resultBeforeUpdate.map(original => ({ ...original, ...result }))
}
