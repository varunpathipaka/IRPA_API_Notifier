const cds = require('../../_runtime/cds')

const { deepCopyObject } = require('../../_runtime/common/utils/copy')
const { checkKeys, checkStatic } = require('../../_runtime/cds-services/util/assert')
const { MULTIPLE_ERRORS } = require('../../_runtime/common/error/constants')

// this can be reused for flattening data on db layer, if necessary
// const _getFlattenedCopy = (data, key, entity) => {
//   const d = {}
//   const prefix = key + '_'
//   // TODO: ignore or preserve unknown?
//   const matches = Object.keys(entity.elements)
//     .filter(ele => ele.startsWith(prefix))
//     .map(ele => ele.replace(prefix, ''))
//   if (matches.length) {
//     const current = data[key]
//     for (const k of matches) if (current[k] !== undefined) d[prefix + k] = current[k]
//     const nested = Object.keys(current).filter(k => current[k] && typeof current[k] === 'object')
//     for (const k of nested) Object.assign(d, _getFlattenedCopy({ [prefix + k]: current[k] }, prefix + k, entity))
//   }
//   return d
// }

const _getDeepCopy = (data, definition, model, validations, skipKeys) => {
  skipKeys || (definition.keys && validations.push(...checkKeys(definition, data)))
  validations.push(...checkStatic(definition, data, true))

  const d = {}
  for (const k in data) {
    const element = (definition.elements && definition.elements[k]) || (definition.params && definition.params[k])
    if (!element) {
      const { additional_properties } = cds.env.features
      if (additional_properties === 'ignore' || !additional_properties) {
        // ignore input (the default)
      } else if (additional_properties === 'error') {
        validations.push({ message: `Unknown property "${k}"`, code: 400 })
      } else {
        d[k] = data[k] && typeof data[k] === 'object' ? deepCopyObject(data[k]) : data[k]
      }
    } else if (element.isAssociation) {
      d[k] = Array.isArray(data[k])
        ? data[k].map(d => _getDeepCopy(d, model.definitions[element.target], model, validations))
        : _getDeepCopy(data[k], model.definitions[element.target], model, validations)
    } else if (element._isStructured) {
      d[k] = _getDeepCopy(data[k], element, model, validations)
    } else {
      d[k] = data[k]
    }
  }
  return d
}

const getDeepCopy = (data, definition, model, skipKeys) => {
  const validations = []
  const copy = Array.isArray(data)
    ? data.map(d => _getDeepCopy(d, definition, model, validations, skipKeys))
    : _getDeepCopy(data, definition, model, validations, skipKeys)
  if (!validations.length) return [undefined, copy]
  if (validations.length === 1) return [validations[0]]
  return [Object.assign(new Error(MULTIPLE_ERRORS), { details: validations })]
}

module.exports = {
  getDeepCopy
}
