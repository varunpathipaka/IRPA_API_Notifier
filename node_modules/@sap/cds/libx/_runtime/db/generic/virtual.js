/*
 * handler for converting virtual fields to { val: null, as: 'myVirtualField' } by READ
 */

const { ensureNoDraftsSuffix } = require('../../common/utils/draft')

const _convert = (columns, target, model) => {
  if (!target) return
  for (const col of columns) {
    const element = col.ref && target.elements[col.ref[col.ref.length - 1]]
    if (element) {
      if (element.virtual) {
        col.as = col.as || col.ref[col.ref.length - 1]
        delete col.ref
        col.val = (element.default && element.default.val) || null
      }
      if (col.expand && element.isAssociation) {
        _convert(col.expand, model.definitions[element.target], model)
      }
    }
  }
}

const convertVirtuals = function (req, _model) {
  const model = this.model || _model
  // target.name ensures it is not a union or join
  if (typeof req.query === 'string' || !req.target || typeof req.target.name !== 'string' || !model) return
  const target = (!req.target._unresolved && req.target) || model.definitions[ensureNoDraftsSuffix(req.target.name)]
  const columns = (req.query && req.query.SELECT && req.query.SELECT.columns) || []
  _convert(columns, target, model)
  if (req.query.SELECT.from && req.query.SELECT.from.SET) {
    for (const arg of req.query.SELECT.from.SET.args) {
      const target = model.definitions[ensureNoDraftsSuffix(arg._target.name)]
      const columns = (arg.SELECT && arg.SELECT.columns) || []
      _convert(columns, target, model, true)
    }
  }
}

convertVirtuals._initial = true

module.exports = {
  convertVirtuals
}
