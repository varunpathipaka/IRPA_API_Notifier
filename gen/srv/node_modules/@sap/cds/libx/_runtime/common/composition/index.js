const { getCompositionTree, getCompositionRoot } = require('./tree')
const { hasDeepInsert, getDeepInsertCQNs, cleanEmptyCompositionsOfMany } = require('./insert')
const { hasDeepUpdate, getDeepUpdateCQNs } = require('./update')
const { hasDeepDelete, getDeepDeleteCQNs } = require('./delete')
const { selectDeepUpdateData } = require('./data')

module.exports = {
  // tree
  getCompositionTree,
  getCompositionRoot,
  // insert
  hasDeepInsert,
  getDeepInsertCQNs,
  cleanEmptyCompositionsOfMany,
  // update
  hasDeepUpdate,
  getDeepUpdateCQNs,
  // delete
  hasDeepDelete,
  getDeepDeleteCQNs,
  // data
  selectDeepUpdateData
}
