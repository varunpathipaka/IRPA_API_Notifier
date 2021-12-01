const resolveStructured = require('./resolveStructured')
const { ensureNoDraftsSuffix } = require('../../common/utils/draft')
// TODO move to commons as also used in cqn2cqn4sql
const OPERATIONS = ['=', '>', '<', '!=', '<>', '>=', '<=', 'like', 'between', 'in', 'not in']

const _getEntityNames = from => {
  if (from.ref) {
    return [ensureNoDraftsSuffix(from.ref[0])]
  }

  if (from.SET) {
    return Array.from(
      from.SET.args.reduce((set, elem) => {
        for (const entityName of _getEntityNames(elem.SELECT.from)) {
          set.add(entityName)
        }
        return set
      }, new Set())
    )
  }

  if (Array.isArray(from.args)) {
    // TODO this only considers first level refs and not from sub selects
    return from.args.filter(arg => arg.ref).map(arg => ensureNoDraftsSuffix(arg.ref[0]))
  }

  return []
}

const _flattenStructuredInExpand = (column, { _target: expandedEntity }) => {
  const flattenedElements = []
  const toBeDeleted = []
  for (const expandElement of column.expand) {
    if (expandElement.expand) {
      _flattenStructuredInExpand(expandElement, getNavigationIfStruct(expandedEntity, expandElement.ref))
      continue
    }

    if (!expandElement.ref) continue
    const propertyName = expandElement.ref[expandElement.ref.length - 1]
    const element = expandedEntity.elements[expandElement.ref[0]] // TODO alias
    if (!element) continue

    if (element._isStructured) {
      toBeDeleted.push(propertyName)
      flattenedElements.push(
        ...resolveStructured(
          { structName: element.name, structProperties: expandElement.ref.slice(1) },
          element.elements
        )
      )
    }
  }

  const orderBy = _flattenStructuredOrderBy(column.orderBy, expandedEntity)
  if (orderBy) {
    column.orderBy = orderBy
  }
  column.where = flattenStructuredWhereHaving(column.where, expandedEntity)
  column.expand = column.expand.filter(e => !e.ref || !toBeDeleted.includes(e.ref[e.ref.length - 1]))
  column.expand.push(...flattenedElements)
}

const _flattenStructuredOrderBy = (orderBy, csnEntity) => {
  if (orderBy) {
    const newOrder = []
    for (const order of orderBy) {
      const element = order.ref && csnEntity.elements[order.ref[0]]
      if (!element) {
        newOrder.push(order)
        continue
      }

      if (element._isStructured) {
        const flattenedStructOrder = resolveStructured(
          { structName: order.ref[0], structProperties: order.ref.slice(1) },
          element.elements
        )
        newOrder.push(...flattenedStructOrder.map(element => ({ ref: element.ref, sort: order.sort })))
      } else {
        newOrder.push(order)
      }
    }
    return newOrder
  }
}

const _getVal = (data, name) => {
  if (!data) return null

  if (typeof data !== 'object') return data

  if (name in data) {
    return data[name]
  }

  return null
}

const _filterForStructProperty = (structElement, structData, op, prefix = '', nav = []) => {
  const filterArray = []

  for (const elementName in structElement.elements) {
    const element = structElement.elements[elementName]
    if (!element) continue

    if (element._isStructured) {
      filterArray.push(
        ..._filterForStructProperty(
          element,
          structData && structData[element.name],
          op,
          prefix + '_' + element.name,
          nav
        )
      )
    } else {
      if (element.isAssociation) continue
      if (element['@odata.foreignKey4']) {
        const assocName = element['@odata.foreignKey4']
        const assoc = structElement.elements[assocName]
        if (assoc.is2one && !assoc.on) {
          for (const key in assoc._target.keys) {
            if (element.name === `${assocName}_${key}`) {
              const ref = [`${prefix}_${assocName}_${key}`]
              const val = _getVal(structData[assocName], key)
              filterArray.push({ ref }, op, { val }, 'and')
            }
          }
        }
        continue
      }
      filterArray.push(
        { ref: [...nav, `${prefix}_${element.name}`] },
        op,
        { val: _getVal(structData, element.name) },
        'and'
      )
    }
  }

  return filterArray
}

const _nestedStructElement = (ref, element, prefix = `${element.name}`) => {
  const nestedElement = element.elements[ref[0]]

  if (!ref.length) return { prefix, nestedElement: element }

  if (ref.length === 1) {
    if (nestedElement.isAssociation)
      return { prefix: `${prefix}_${nestedElement.name}`, nestedElement: nestedElement._target }
    return { prefix: `${prefix}_${nestedElement.name}`, nestedElement }
  }

  if (nestedElement._isStructured) {
    return _nestedStructElement(ref.slice(1), nestedElement, `${prefix}_${nestedElement.name}`)
  }
  if (nestedElement.isAssociation) {
    return _nestedStructElement(ref.slice(1), nestedElement._target, `${prefix}_${nestedElement.name}`)
  }
}

const _transformStructToFlatWhereHaving = ([first, op, second], resArray, structElement, structIdx) => {
  const ref = first.ref || second.ref
  const val = first.val === undefined ? second.val : first.val

  const structName = ref[structIdx]
  const structProperties = ref.slice(structIdx + 1)
  const nav = structIdx > 0 ? ref.slice(0, structIdx) : []
  const flattenedElements = resolveStructured({ structName, structProperties }, structElement.elements)
  const flattenedElement = flattenedElements.find(el => el.ref[0] === [structName, ...structProperties].join('_'))
  let structData = val
  try {
    structData = JSON.parse(val)
  } catch (e) {
    /* since val === string */
  }
  if (flattenedElement && (structData === val || `${structData}` === val)) {
    flattenedElement.ref.unshift(...nav)
    resArray.push(flattenedElement, op, { val })
  } else {
    // transform complex structured to multiple single structured
    const { nestedElement, prefix } = _nestedStructElement(structProperties, structElement)
    resArray.push(..._filterForStructProperty(nestedElement, structData, op, prefix, nav))
  }

  if (resArray[resArray.length - 1] === 'and') {
    resArray.pop()
  }
}

const _structFromRef = (ref, csnEntity, model) => {
  let entity = csnEntity
  if (!ref) return {}
  for (let idx = 0; idx < ref.length; idx++) {
    const part = ref[idx]
    const element = entity.elements[part]
    if (!element) return {}
    if (element._isStructured) return { element, idx }
    if (element.target) entity = model.definitions[element.target]
    else return {}
  }
}

const flattenStructuredWhereHaving = (filterArray, csnEntity, model) => {
  if (filterArray) {
    const newFilterArray = []
    for (let i = 0; i < filterArray.length; i++) {
      if (OPERATIONS.includes(filterArray[i + 1])) {
        const refElement = filterArray[i].ref ? filterArray[i] : filterArray[i + 2]
        // copy for processing
        const ref = refElement.ref && refElement.ref.map(ele => ele)
        // is ref[0] an alias? -> remove
        const isAliased = ref && ref.length > 1 && !csnEntity.elements[ref[0]]
        if (isAliased) ref.shift()
        const { element, idx } = _structFromRef(ref, csnEntity, model)
        // REVISIT: We cannot make the simple distinction between ref and others
        // for xpr, subselect, we need to call this method recursively
        if (element) {
          if (isAliased) refElement.ref.shift()
          // REVISIT: This does not support operator like "between", "in" or a different order of elements like val,op,ref or expressions like ref,op,val+val
          _transformStructToFlatWhereHaving(filterArray.slice(i, i + 3), newFilterArray, element, idx)
          i += 2 // skip next two entries e.g. ('=', '{struct:{int:1}}')
          continue
        }
      }

      newFilterArray.push(filterArray[i])
    }
    return newFilterArray
  }
}
const _entityFromRef = ref => {
  if (ref) return ref[0].id || ref[0]
}
const getNavigationIfStruct = (entity, ref) => {
  const element = entity && entity.elements && entity.elements[_entityFromRef(ref)]
  if (!element) return
  if (ref.length > 1) return getNavigationIfStruct(element._target || element, ref.slice(1))
  return element
}

const _flattenColumns = (SELECT, flattenedElements, toBeDeleted, csnEntity) => {
  for (const column of SELECT.columns) {
    if (!column.ref) continue

    // TODO aliases are not working right now
    const structName = column.ref[0]

    const element = csnEntity.elements[structName]
    if (!element) continue

    if (column.expand) {
      _flattenStructuredInExpand(column, getNavigationIfStruct(csnEntity, column.ref))
      continue
    }

    if (element._isStructured) {
      toBeDeleted.push(structName) // works with aliases?
      flattenedElements.push(
        ...resolveStructured({ structName, structProperties: column.ref.slice(1) }, element.elements)
      )
    }
  }
}

const flattenStructuredSelect = ({ SELECT }, model) => {
  const entityNames = _getEntityNames(SELECT.from) // TODO consider alias for custom CQNs?

  for (const entityName of entityNames) {
    const entity = model.definitions[entityName]

    if (Array.isArray(SELECT.columns) && SELECT.columns.length > 0) {
      const flattenedElements = []
      const toBeDeleted = []
      _flattenColumns(SELECT, flattenedElements, toBeDeleted, entity)
      SELECT.columns = SELECT.columns.filter(e => (e.ref && !toBeDeleted.includes(e.ref[0])) || e.func || e.expand) // TODO aliases?
      SELECT.columns.push(...flattenedElements)
    }
    if (SELECT.from.args) {
      for (const arg of SELECT.from.args) {
        if (arg.SELECT) {
          flattenStructuredSelect(arg, model)
        }
      }
    }

    const orderBy = _flattenStructuredOrderBy(SELECT.orderBy, entity)
    if (orderBy) SELECT.orderBy = orderBy
    const flattenedWhere = flattenStructuredWhereHaving(SELECT.where, entity, model)
    if (flattenedWhere) SELECT.where = flattenedWhere
    const flattenedHaving = flattenStructuredWhereHaving(SELECT.having, entity, model)
    if (flattenedHaving) SELECT.having = flattenedHaving
  }
}

module.exports = {
  flattenStructuredSelect,
  flattenStructuredWhereHaving,
  getNavigationIfStruct
}
