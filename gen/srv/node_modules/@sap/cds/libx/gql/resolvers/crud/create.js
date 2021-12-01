const { ARGUMENT } = require('../../constants/adapter')
const { getArgumentByName, astToEntries } = require('../parse/ast2cqn')
const { entriesStructureToEntityStructure } = require('./utils')

module.exports = async (service, entityFQN, selection) => {
  let query = service.create(entityFQN)

  const input = getArgumentByName(selection.arguments, ARGUMENT.INPUT)
  const entries = entriesStructureToEntityStructure(service, entityFQN, astToEntries(input))
  query.entries(entries)

  const result = await service.tx(tx => tx.run(query))

  return Array.isArray(result) ? result : [result]
}
