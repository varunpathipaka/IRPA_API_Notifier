const cds = require('../../cds')
const { SELECT, INSERT, DELETE, UPDATE } = cds.ql

const { resolveView } = require('./resolveView')
const { ensureNoDraftsSuffix } = require('./draft')
const { flattenStructuredSelect } = require('./structured')
const search2cqn4sql = require('./search2cqn4sql')
const { getEntityNameFromCQN } = require('./entityFromCqn')
const getError = require('../../common/error')
const { rewriteAsterisks } = require('./rewriteAsterisks')
const { getPathFromRef, getEntityFromPath } = require('../../common/utils/path')
const { removeIsActiveEntityRecursively } = require('../../fiori/utils/where')

const PARENT_ALIAS = '_parent_'
const PARENT_ALIAS_REGEX = new RegExp('^' + PARENT_ALIAS + '\\d*$')
const FOREIGN_ALIAS = '_foreign_'
const OPERATIONS = ['=', '>', '<', '!=', '<>', '>=', '<=', 'like', 'between', 'in', 'not in']

// special case in lambda functions
const _addParentAlias = (where, alias) => {
  where.forEach(e => {
    if (e.ref && e.ref[0].match(PARENT_ALIAS_REGEX)) {
      e.ref[0] = alias
    }
  })
}

const _addAliasToElement = (e, alias) => {
  if (e.ref) {
    return { ref: [alias, ...e.ref] }
  }

  if (e.list) {
    return { list: e.list.map(arg => _addAliasToElement(arg, alias)) }
  }

  if (e.func) {
    const args = e.args.map(arg => _addAliasToElement(arg, alias))
    return { ...e, args }
  }

  if (e.SELECT && e.SELECT.where) {
    _addParentAlias(e.SELECT.where, alias)
  }

  if (e.xpr) {
    return { xpr: e.xpr.map(e1 => _addAliasToElement(e1, alias)) }
  }

  return e
}

const _addAliasToExpression = (expression, alias) => {
  if (!alias) {
    return expression
  }

  return expression.map(e => _addAliasToElement(e, alias))
}

const _elementFromRef = (name, entity) => {
  if (!entity) return

  if (entity.elements) {
    return entity.elements[_getTargetFromRef(name)]
  }
}

const _getTargetFromRef = ref => {
  return ref.id || ref
}

const _getEntityName = (fromClause, entity, i) => {
  const targetName = _getTargetFromRef(fromClause.ref[i])
  return i === 0 ? targetName : entity && entity.elements[targetName] && entity.elements[targetName].target
}

const convertPathExpressionToWhere = (fromClause, model, options) => {
  if (fromClause.ref.length === 1) {
    const target = _getTargetFromRef(fromClause.ref[0])
    const alias = fromClause.as
    const { where, cardinality } = fromClause.ref[0]
    return { target, alias, where, cardinality }
  }

  let previousSelect, previousEntityName, previousTableAlias, structParent
  let prefix = []
  let columns
  for (let i = 0; i < fromClause.ref.length; i++) {
    const entity = structParent || model.definitions[previousEntityName]
    const element = _elementFromRef(fromClause.ref[i], entity)

    if (element && element._isStructured) {
      prefix.push(element.name)
      structParent = element
      continue
    } else if (element && element.isAssociation) {
      _modifyNavigationInWhere(fromClause.ref[i].where, element._target)
    } else if (element && previousSelect && i === fromClause.ref.length - 1) {
      columns = [{ ref: [...prefix, element.name] }]
      fromClause.ref.splice(-1)
      continue
    }

    const currentEntityName = _getEntityName(fromClause, entity, i)
    if (!currentEntityName) continue
    const tableAlias = `T${i}`
    const currentSelect = SELECT.from(`${currentEntityName} as ${tableAlias}`)

    if (fromClause.ref[i].where) {
      currentSelect.where(_addAliasToExpression(fromClause.ref[i].where, tableAlias))
    }

    if (i !== fromClause.ref.length - 1) {
      currentSelect.columns([1])
    }

    if (previousSelect) {
      const navigation = _getTargetFromRef(fromClause.ref[i])
      previousSelect.where(
        model.definitions[previousEntityName]._relations[[...prefix, navigation]].join(tableAlias, previousTableAlias)
      )
      _convertSelect(previousSelect, model, options)
      currentSelect.where('exists', previousSelect)
    }

    structParent = undefined
    prefix = []
    previousTableAlias = tableAlias
    previousSelect = currentSelect
    previousEntityName = currentEntityName
  }

  return {
    target: previousEntityName,
    alias: previousTableAlias,
    where: previousSelect && previousSelect.SELECT && previousSelect.SELECT.where,
    columns
  }
}

const _convertPathExpressionForInsertOrDelete = (intoClause, model) => {
  // .into is plain string or csn entity
  if (typeof intoClause === 'string' || intoClause.name) {
    return intoClause
  }

  return intoClause.ref.reduce((res, curr, i) => {
    if (i === 0) {
      return curr.id || curr
    }

    return model.definitions[res].elements[curr.id || curr].target
  }, '')
}

const _getBottomTopRefOrVal = (func, refVal) => {
  return func.args.filter(el => el[refVal])
}

const _getWindowWhere = (where, bottomTop) => {
  const windWhere = where || []
  const bottomTopVal = _getBottomTopRefOrVal(bottomTop[0], 'val')[0]
  bottomTopVal.val = parseInt(bottomTopVal.val, 10)

  if (windWhere.length > 0) {
    windWhere.push('and')
  }

  windWhere.push({ ref: ['rowNumber'] }, '<=', bottomTopVal)
  return windWhere
}

const _getOrderByForWindowFn = bottomTop => {
  const orderBy = _getBottomTopRefOrVal(bottomTop[0], 'ref')[0]
  orderBy.sort = bottomTop[0].func === 'topcount' ? 'desc' : 'asc'
  return orderBy
}

const _getWindowXpr = (groupBy, bottomTop) => {
  const xpr = [{ func: 'ROW_NUMBER', args: [] }, 'OVER', '(']
  xpr.push(
    'PARTITION BY',
    ...groupBy.reduce((acc, el, i) => {
      if (i < groupBy.length - 1) {
        acc.push(el)
        acc.push(',')
      } else {
        acc.push(el)
      }

      return acc
    }, [])
  )
  xpr.push('ORDER BY', _getOrderByForWindowFn(bottomTop))
  xpr.push(')')
  return { xpr: xpr, as: 'rowNumber' }
}

const _isBottomTop = columns => {
  return columns.some(el => el.func && (el.func === 'topcount' || el.func === 'bottomcount'))
}

const _getWindColumns = (columns, groupBy, bottomTop) => {
  return [].concat(columns, _getWindowXpr(groupBy, bottomTop))
}

const _createWindowCQN = (SELECT, model) => {
  const bottomTop = SELECT.columns.filter(el => _isBottomTop([el]))
  const columns = (SELECT.columns = [])
  const { elements } = model.definitions[SELECT.from.ref[0]]

  for (const el in elements) {
    if (!elements[el].isAssociation) {
      columns.push({ ref: [el] })
    }
  }

  SELECT.where = _getWindowWhere(SELECT.where, bottomTop)
  SELECT.from = {
    SELECT: {
      columns: _getWindColumns(columns, SELECT.groupBy, bottomTop),
      from: SELECT.from
    }
  }

  delete SELECT.groupBy
}

const _isAny = element => {
  return Array.isArray(element.ref) && element.ref.slice(-1)[0].id
}

const _isAll = element => {
  const last = Array.isArray(element.ref) && element.ref.slice(-1)[0]
  return last && last.id && last.where && last.where[0] === 'not' && last.where[1].xpr
}

// eslint-disable-next-line complexity
const _getLambdaSubSelect = (cqn, where, index, lambdaOp, model, options) => {
  const _unshiftRefsWithNavigation = nav => el => {
    if (el.ref) return { ref: [...nav, ...el.ref] }
    if (el.xpr) return { xpr: el.xpr.map(_unshiftRefsWithNavigation(nav)) }
    return el
  }

  if (!options.lambdaIteration) options.lambdaIteration = 1
  const outerAlias =
    (cqn.SELECT.from.ref && cqn.SELECT.from.as) ||
    (cqn.SELECT.from.args && cqn.SELECT.from.args[0].ref && cqn.SELECT.from.args[0].as) ||
    PARENT_ALIAS + options.lambdaIteration
  const innerAlias = FOREIGN_ALIAS + options.lambdaIteration
  cqn.SELECT.from.as = outerAlias
  const ref = cqn.SELECT.from.ref || (cqn.SELECT.from.args && cqn.SELECT.from.args[0].ref)
  const queryTarget = getEntityFromPath(getPathFromRef(ref), model)

  const nav = where[index].ref.map(el => (el.id ? el.id : el))
  const last = where[index].ref.slice(-1)[0]
  const navName = queryTarget.elements[nav[0]] ? nav[0] : nav[nav.length - 1]
  const navElement = queryTarget.elements[navName]
  const lastElement = nav.reduce((csn, segment) => {
    if (csn.items) return csn // arrayed not supported
    if (csn.elements) {
      const next = csn.elements[segment]
      if (!next) return csn
      if (next.target) return model.definitions[next.target]
      return next
    }
  }, queryTarget)
  if (lastElement.items)
    /* arrayed */
    throw getError(501, `Condition expression with arrayed elements is not supported`)
  const condition = last.where
    ? nav.length > 1
      ? last.where.map(_unshiftRefsWithNavigation(nav.slice(1)))
      : last.where
    : undefined

  const subSelect = SELECT.from({ ref: [navElement.target], as: innerAlias })
  if (condition) {
    if (subSelect.SELECT.from.as) {
      for (let i = 0; i < condition.length; i++) {
        if (
          condition[i].ref &&
          condition[i].ref.length > 1 &&
          condition[i].ref.every(r => typeof r === 'string') &&
          condition[i].ref[0] === navName
        ) {
          condition[i].ref[0] = subSelect.SELECT.from.as
        }
      }
    }
    subSelect.where(condition)
  }
  subSelect.where(queryTarget._relations[navName].join(innerAlias, outerAlias))
  if (cds.env.effective.odata.structs) {
    flattenStructuredSelect(subSelect, model)
  }
  subSelect.columns([{ val: 1 }])

  // nested where exists needs recursive conversion
  options.lambdaIteration++
  return _convertSelect(subSelect, model, options)
}

const _convertLambda = (cqn, model, options) => {
  const where = cqn.SELECT.where
  if (where) _recurse(where)
  function _recurse(where) {
    where.forEach((element, index) => {
      if (element.xpr) {
        _recurse(element.xpr) // recursing into nested {xpr}
      } else if (element === 'exists' && _isAny(where[index + 1])) {
        where[index + 1] = _getLambdaSubSelect(cqn, where, index + 1, 'any', model, options)
      } else if (element === 'not' && where[index + 1] === 'exists' && _isAll(where[index + 2])) {
        where[index + 2] = _getLambdaSubSelect(cqn, where, index + 2, 'all', model, options)
      }
    })
  }
}

const _getRefIndex = (where, index) => {
  if (
    where[index - 1].ref &&
    // REVISIT DRAFT HANDLING: cqn2cqn4sql must not be called there
    where[index - 1].ref[where[index - 1].ref.length - 1] !== 'InProcessByUser' &&
    where[index + 1].val !== undefined &&
    where[index + 1].val !== null &&
    where[index + 1].val !== false
  ) {
    return index - 1
  }
  if (
    where[index + 1].ref &&
    // REVISIT DRAFT HANDLING: cqn2cqn4sql must not be called there
    where[index + 1].ref[where[index + 1].ref.length - 1] !== 'InProcessByUser' &&
    where[index - 1].val !== undefined &&
    where[index - 1].val !== null &&
    where[index - 1].val !== false
  ) {
    return index + 1
  }
}

const _convertNotEqual = container => {
  const where = container.where

  if (where) {
    let changed
    where.forEach((el, index) => {
      if (el === '!=') {
        const refIndex = _getRefIndex(where, index)
        if (refIndex !== undefined) {
          where[index - 1] = {
            xpr: [where[index - 1], el, where[index + 1], 'or', where[refIndex], '=', { val: null }]
          }
          where[index] = where[index + 1] = undefined
          changed = true
        }
      }

      if (el && el.SELECT) {
        _convertNotEqual(el.SELECT)
      }
    })

    // delete undefined values
    if (changed) {
      container.where = where.filter(el => el)
    }
  }

  if (container.columns) {
    container.columns.forEach(col => {
      if (typeof col === 'object') {
        if (col.SELECT) {
          _convertNotEqual(col.SELECT)
        } else if (col.where) {
          _convertNotEqual(col)
        }
      }
    })
  }
}

const _ifOrderByOrWhereSkip = (queryTarget, ref, model) => {
  if (!queryTarget.elements[ref[0]]) return
  return ref.some(el => {
    if (typeof el !== 'string') return
    const orderbyTarget = queryTarget.elements[el]._isStructured
      ? queryTarget.elements[el]
      : model.definitions[queryTarget.elements[el].target]
    if (orderbyTarget) queryTarget = orderbyTarget
    return (
      (orderbyTarget && orderbyTarget._hasPersistenceSkip) ||
      (queryTarget.elements[el] && queryTarget.elements[el].virtual)
    )
  })
}

const _skip = (queryTarget, ref, model) => {
  if (ref.length === 1) return queryTarget && queryTarget.elements[ref[0]] && queryTarget.elements[ref[0]].virtual
  return _ifOrderByOrWhereSkip(queryTarget, ref, model)
}

const _convertOrderByIfSkip = (orderByCQN, index) => {
  orderByCQN.splice(index, 1)
}

const _convertWhereIfSkip = (whereCQN, index) => {
  whereCQN.splice(index, 1, '1 = 1')
  OPERATIONS.includes(whereCQN[index + 1]) ? whereCQN.splice(index + 1, 2) : whereCQN.splice(index - 2, 2)
}

const _convertOrderByOrWhereCQN = (orderByOrWhereCQN, target, model, processFn) => {
  const queryTarget = model.definitions[ensureNoDraftsSuffix(target)]

  orderByOrWhereCQN.forEach((el, index) => {
    if (el.ref && _skip(queryTarget, el.ref, model)) processFn(orderByOrWhereCQN, index)
  })
}

const _convertOrderByOrWhereIfSkip = (cqn, target, model) => {
  if (cqn.SELECT.orderBy && cqn.SELECT.orderBy.length > 1) {
    _convertOrderByOrWhereCQN(cqn.SELECT.orderBy, target, model, _convertOrderByIfSkip)
  }

  if (cqn.SELECT.where) {
    _convertOrderByOrWhereCQN(cqn.SELECT.where, target, model, _convertWhereIfSkip)
  }
}

const _convertExpand = expand => {
  expand.forEach(expandElement => {
    if (expandElement.ref && expandElement.ref[0]) {
      if (expandElement.ref[0].where) {
        expandElement.where = expandElement.ref[0].where
      }
      if (expandElement.ref[0].id) {
        expandElement.ref[0] = expandElement.ref[0].id
      }
    }
    if (expandElement.expand) {
      _convertExpand(expandElement.expand)
    }
  })
}

const _convertRefWhereInExpand = columns => {
  if (columns) {
    columns.forEach(col => {
      if (col.expand) {
        _convertExpand(col.expand)
      }
    })
  }
}

const _flattenCQN = cqn => {
  if (Array.isArray(cqn)) cqn.forEach(_flattenCQN)
  else if (cqn) {
    if (cqn.SELECT) _flattenCQN(cqn.SELECT)
    if (cqn.from) _flattenCQN(cqn.from)
    if (cqn.ref) _flattenCQN(cqn.ref)
    if (cqn.SET) _flattenCQN(cqn.SET)
    if (cqn.args) _flattenCQN(cqn.args)
    if (cqn.columns) _flattenCQN(cqn.columns)
    if (cqn.expand) _flattenCQN(cqn.expand)
    if (cqn.where) _flattenXpr(cqn.where)
    if (cqn.having) _flattenXpr(cqn.having)
  }
}

const _flattenXpr = cqn => {
  if (!Array.isArray(cqn)) {
    if (cqn.xpr) cqn = cqn.xpr
    return
  }

  let idx = cqn.findIndex(el => el.xpr)
  while (idx > -1) {
    cqn.splice(idx, 1, '(', ...cqn[idx].xpr, ')')
    idx = cqn.findIndex(el => el.xpr)
  }

  cqn.forEach(_flattenCQN)
}

// eslint-disable-next-line complexity
const _convertSelect = (query, model, options) => {
  const isDB = options.service instanceof cds.DatabaseService
  const isDraft = options.draft
  // REVISIT: a temporary workaround for xpr from new parser
  if (cds.env.features.odata_new_parser) _flattenCQN(query)

  // lambda functions
  _convertLambda(query, model, options)

  // add 'or is null' in case of '!='
  if (query.SELECT._4odata) _convertNotEqual(query.SELECT)

  let searchOptions = query._searchOptions
  const cqnSelectFromRef = query.SELECT.from.ref

  // no path expression
  if (!cqnSelectFromRef || (cqnSelectFromRef.length === 1 && !cqnSelectFromRef[0].where)) {
    rewriteAsterisks(query, cqnSelectFromRef && model.definitions[cqnSelectFromRef[0]], isDB, isDraft)
    // extract where clause if it is in column expand ref
    _convertRefWhereInExpand(query.SELECT.columns)
    // remove virtual and with skip annotated fields in orderby and where
    _convertOrderByOrWhereIfSkip(query, getEntityNameFromCQN(query), model)

    if (cqnSelectFromRef && query.SELECT.search && !options.suppressSearch) {
      searchOptions = { ...searchOptions, ...{ targetName: cqnSelectFromRef[0] } }
      search2cqn4sql(query, model, searchOptions)
    }

    if (query.SELECT.columns && cds.env.effective.odata.structs) {
      flattenStructuredSelect(query, model)
    }

    // topcount with groupby
    if (query.SELECT.columns && _isBottomTop(query.SELECT.columns)) {
      _createWindowCQN(query.SELECT, model)
    }

    return query
  }

  // path expression handling
  const { target, alias, where, cardinality, columns } = convertPathExpressionToWhere(query.SELECT.from, model, options)
  rewriteAsterisks(query, model.definitions[target], isDB, isDraft, !!columns)
  if (columns) {
    if (query._streaming) query.SELECT.columns = columns
    else query.SELECT.columns.push(...columns)
  }

  // extract where clause if it is in column expand ref
  _convertRefWhereInExpand(query.SELECT.columns)

  // remove virtual and with skip annotated fields in orderby and where
  _convertOrderByOrWhereIfSkip(query, target, model)

  const select = SELECT.from(target, query.SELECT.columns)

  if (alias) {
    select.SELECT.from.as = alias
  }

  // TODO: REVISIT: We need to add alias to subselect in .where, .columns, .from, ... etc
  if (where) {
    if (!isDraft) {
      select.where(removeIsActiveEntityRecursively(where))
    } else {
      select.where(where)
    }
  }

  if (cardinality && cardinality.max === 1) {
    query.SELECT.one = true
  }

  if (query.SELECT.search) {
    searchOptions = { ...searchOptions, ...{ targetName: target } }
    search2cqn4sql(query, model, searchOptions)
  }

  if (query.SELECT.where) {
    select.where(_addAliasToExpression(query.SELECT.where, select.SELECT.from.as))
  }

  // We add all previous properties ot the newly created query.
  // Reason is to not lose the query API functionality
  Object.assign(select.SELECT, query.SELECT, {
    columns: select.SELECT.columns,
    from: select.SELECT.from,
    where: select.SELECT.where
  })

  if (select.SELECT.columns && cds.env.effective.odata.structs) {
    flattenStructuredSelect(select, model)
  }

  return select
}

const _convertInsert = (query, model, options) => {
  // resolve path expression
  const resolvedIntoClause = _convertPathExpressionForInsertOrDelete(query.INSERT.into, model)

  // overwrite only .into, foreign keys are already set
  const insert = INSERT.into(resolvedIntoClause)

  // REVISIT flatten structured types, currently its done in SQL builder

  // We add all previous properties ot the newly created query.
  // Reason is to not lose the query API functionality
  Object.assign(insert.INSERT, query.INSERT, { into: resolvedIntoClause })

  const targetName = insert.INSERT.into.name || insert.INSERT.into

  const target = model.definitions[targetName]
  if (!target) return insert

  const resolvedView = resolveView(insert, model, cds.db)

  if (query.INSERT.into.ref && query.INSERT.into.ref.length > 1) {
    const copyFrom = [...query.INSERT.into.ref]
    copyFrom.pop()
    resolvedView._validationQuery = _convertSelect(SELECT.from({ ref: copyFrom }).columns([1]), model, options)
  }

  return resolvedView
}

function _modifyNavigationInWhere(whereClause, target) {
  if (!whereClause) return
  whereClause.forEach(e => {
    if (e.ref && e.ref.length > 1 && target.elements[e.ref[0]]) {
      const element = target.elements[e.ref[0]]
      if (!element.isAssociation) return
      const foreignKeys = element._foreignKeys
      const joined = e.ref.join('_')

      for (const { parentElement } of foreignKeys) {
        if (parentElement && parentElement.name === joined) {
          e.ref = [joined]
        }
      }
    }
  })
}

const _plainDelete = (cqn, model) => {
  const name = cqn.DELETE.from.name || (cqn.DELETE.from.ref && cqn.DELETE.from.ref[0]) || cqn.DELETE.from
  const target = model.definitions[name]
  if (!target) return cqn

  return resolveView(cqn, model, cds.db)
}

const _convertDelete = (query, model, options) => {
  // .from is plain string or csn entity
  if (
    typeof query.DELETE.from === 'string' ||
    query.DELETE.from.name ||
    (query.DELETE.from.ref && typeof query.DELETE.from.ref[0] === 'string')
  ) {
    return _plainDelete(query, model)
  }

  const { target, alias, where } = convertPathExpressionToWhere(query.DELETE.from, model, options)
  const deleet = DELETE('x')
  Object.assign(deleet.DELETE, query.DELETE, { from: target, where: undefined })

  if (alias) deleet.DELETE.from = { ref: [target], as: alias }
  if (where) deleet.where(where)
  if (query.DELETE.where) deleet.where(_addAliasToExpression(query.DELETE.where, alias))

  const targetEntity = model.definitions[target]
  if (!targetEntity) return deleet

  return resolveView(deleet, model, cds.db)
}

function _plainUpdate(cqn, model) {
  const name = cqn.UPDATE.entity.name || (cqn.UPDATE.entity.ref && cqn.UPDATE.entity.ref[0]) || cqn.UPDATE.entity
  const target = model.definitions[name]
  if (!target) return cqn

  return resolveView(cqn, model, cds.db)
}

const _convertUpdate = (query, model, options) => {
  // REVISIT flatten structured types, currently its done in SQL builder

  // .into is plain string or csn entity
  if (
    typeof query.UPDATE.entity === 'string' ||
    query.UPDATE.entity.name ||
    (query.UPDATE.entity.ref && typeof query.UPDATE.entity.ref[0] === 'string' && query.UPDATE.entity.ref.length === 1)
  ) {
    return _plainUpdate(query, model)
  }

  const { target, alias, where } = convertPathExpressionToWhere(query.UPDATE.entity, model, options)

  // link .with and .data and set query target and remove current where clause
  // REVISIT: update statement does not accept cqn partial as input
  const update = UPDATE('x')
  Object.assign(update.UPDATE, query.UPDATE, { entity: target, where: undefined })

  if (alias) update.UPDATE.entity = { ref: [target], as: alias }
  if (where) update.where(where)
  if (query.UPDATE.where) update.where(_addAliasToExpression(query.UPDATE.where, alias))

  const targetEntity = model.definitions[target]
  if (!targetEntity) return update

  return resolveView(update, model, cds.db)
}

/**
 * Converts a CQN with path expression into exists clause.
 * Converts insert/update/delete on view to target table including renaming of properties
 * REVISIT structured
 * REVISIT topcount when the additional layer for Analytics before SQLBuilder is ready
 *
 * @param {object} query - incoming query
 * @param {object} model - csn model
 * @param {import('../../types/api').cqn2cqn4sqlOptions} [options] Additional options.
 */
const cqn2cqn4sql = (query, model, options = { suppressSearch: false }) => {
  if (query.SELECT) {
    return _convertSelect(query, model, options)
  }

  if (query.UPDATE) {
    return _convertUpdate(query, model, options)
  }

  if (query.INSERT) {
    return _convertInsert(query, model, options)
  }

  if (query.DELETE) {
    return _convertDelete(query, model, options)
  }

  return query
}

module.exports = {
  cqn2cqn4sql,
  convertPathExpressionToWhere
}
