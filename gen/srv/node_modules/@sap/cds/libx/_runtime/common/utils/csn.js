const cds = require('../../cds')

const { ensureNoDraftsSuffix } = require('./draft')

const getEtagElement = entity => {
  return Object.values(entity.elements).find(element => element['@odata.etag'])
}

const _isDependent = (assoc, parent, target) => {
  return (
    assoc._isAssociationStrict &&
    assoc.is2one &&
    !assoc.on &&
    !parent['@cds.persistence.skip'] &&
    assoc['@assert.integrity'] !== false &&
    parent['@assert.integrity'] !== false &&
    (!parent._service || parent._service['@assert.integrity'] !== false) &&
    !assoc._isCompositionBacklink
  )
}

/*
 * this modifies the csn on purpose for caching effect!
 * doing as aspect is difficult due to no global definitons per tenant
 */
const getDependents = (entity, model) => {
  if (entity.own('__dependents')) return entity.__dependents

  /** @type {Array|boolean} */
  let dependents = []
  for (const def of Object.values(model.definitions)) {
    if (def.kind !== 'entity') continue
    if (!def.associations) continue

    for (const assoc of Object.values(def.associations)) {
      if (assoc.target !== entity.name) continue

      const parent = assoc.parent
      const target = model.definitions[assoc.target]
      if (_isDependent(assoc, parent, target)) {
        dependents.push({ element: assoc, parent, target })
      }
    }
  }

  if (dependents.length === 0) dependents = false
  return entity.set('__dependents', dependents)
}

const _getUps = (entity, model) => {
  const ups = []
  for (const def of Object.values(model.definitions)) {
    if (def.kind !== 'entity') continue
    if (!def.associations) continue
    for (const element of Object.values(def.associations)) {
      if (element.target !== entity.name || element._isBacklink) continue
      ups.push(element)
    }
  }
  return ups
}

const _ifDataSubject = (entity, role) => {
  return entity['@PersonalData.EntitySemantics'] === 'DataSubject' && entity['@PersonalData.DataSubjectRole'] === role
}

const _getDataSubjectUp = (role, model, element, first = element) => {
  const upElements = _getUps(element.parent, model)
  for (const element of upElements) {
    if (_ifDataSubject(element.parent, role)) {
      return { element: first, up: { element }, entity: element.parent }
    }
    // dfs is a must here
    const dataSubject = _getDataSubjectUp(role, model, element, first)
    if (dataSubject) {
      dataSubject.up = { element, up: dataSubject.up }
      return dataSubject
    }
  }
}

const getDataSubject = (entity, model, role, element) => {
  const hash = '__dataSubject4' + role
  if (entity.own(hash)) return entity[hash]
  if (_ifDataSubject(element.parent, role)) return entity.set(hash, { element, entity: element.parent })
  return entity.set(hash, _getDataSubjectUp(role, model, element))
}

const _resolve = (name, model, namespace) =>
  model.entities(namespace)[name] || model.definitions[`${namespace}.${name}`]

const _findRootEntity = (model, edmName, namespace) => {
  const parts = edmName.split('_')
  let csnName = parts.shift()
  let target = _resolve(csnName, model, namespace)
  const len = parts.length
  // try to find a correct entity "greedy" and count leftovers (x4 case below)
  // e.g. we have 2 entities: 'C_root_' and dependant 'C_root_.kid_'
  // for 'C_root_.kid_assoc_prop' it should find 'C_root_.kid_'
  // and report 2 leftovers (namely 'assoc' and 'prop')
  let left = len
  let acc = 0
  for (let i = 0; i < len; i++) {
    /**
     * Calculate CSN name.
     * if target in entities connect with .
     * if target not in entities connect with _
     */
    csnName = `${csnName}${_resolve(csnName, model, namespace) ? '.' : '_'}${parts[i]}`
    ++acc
    if (_resolve(csnName, model, namespace)) {
      target = _resolve(csnName, model, namespace)
      left -= acc
      acc = 0
    }
  }
  // make sure we consider leftovers only for x4
  return { left: (cds.env.effective.odata.proxies && left) || 0, target: target }
}

const findCsnTargetFor = (edmName, model, namespace) => {
  const cache =
    model._edmToCSNNameMap || Object.defineProperty(model, '_edmToCSNNameMap', { value: {} })._edmToCSNNameMap
  const mapping =
    cache[namespace] ||
    Object.defineProperty(cache, namespace, { enumerable: true, configurable: true, value: {} })[namespace]

  if (mapping[edmName]) return mapping[edmName]

  // simple cases
  let target = _resolve(edmName, model, namespace) || _resolve(edmName.replace(/_/g, '.'), model, namespace)

  // hard cases
  if (!target) {
    // probably, a combination of '_' and '.', resolving
    const finding = _findRootEntity(model, edmName, namespace)
    target = finding.target
    // something left in navigation path => x4 navigation
    // resolving within found entity
    if (target && finding.left > 0) {
      const left = edmName.split('_').slice(-finding.left)
      while (target && left.length) {
        let elm = left.shift()
        while (!target.elements[elm]) elm = `${elm}_${left.shift()}`
        target = target.elements[elm]
      }
    }
  }
  // remember edm <-> csn
  if (target) {
    mapping[edmName] = target
  }
  return mapping[edmName]
}

const getElementDeep = (entity, ref) => {
  let current = entity
  for (const r of ref) {
    current = current && current.elements && current.elements[r]
  }
  return current
}

const isRootEntity = (definitions, entityName) => {
  const entity = definitions[entityName]
  if (!entity) return false

  // TODO: There can be unmanaged relations to some parent -> not detected by the following code
  const associationElements = Object.keys(entity.elements)
    .map(key => entity.elements[key])
    .filter(element => element._isAssociationEffective)

  for (const { target } of associationElements) {
    const parentEntity = definitions[target]
    for (const parentElementName in parentEntity.elements) {
      const parentElement = parentEntity.elements[parentElementName]
      if (
        parentElement._isCompositionEffective &&
        parentElement.target === entityName &&
        !(parentElement.parent && ensureNoDraftsSuffix(parentElement.parent.name) === entityName)
      ) {
        return false
      }
    }
  }
  return true
}

function alias2ref(service, edm) {
  const defs = edm[service.definition.name]
  for (const each of Object.values(service.entities)) {
    const def = defs[each.name.replace(service.definition.name + '.', '').replace(/\./g, '_')]
    if (!def || !def.$Key || def.$Key.every(ele => typeof ele === 'string')) continue
    each._alias2ref = {}
    for (const mapping of def.$Key.filter(ele => typeof ele !== 'string')) {
      for (const [key, value] of Object.entries(mapping)) {
        each._alias2ref[key] = value.split('/')
      }
    }
  }
}

module.exports = {
  getEtagElement,
  findCsnTargetFor,
  getElementDeep,
  getDependents,
  isRootEntity,
  getDataSubject,
  alias2ref
}
