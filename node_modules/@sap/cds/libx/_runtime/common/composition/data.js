const { getCompositionTree } = require('./tree')
const ctUtils = require('./utils')
const { getEntityNameFromUpdateCQN } = require('../utils/cqn')

const { ensureNoDraftsSuffix } = require('../utils/draft')
const { getDBTable } = require('../utils/resolveView')
const { cqn2cqn4sql } = require('../utils/cqn2cqn4sql')
const cds = require('../../cds')
const { SELECT } = cds.ql

/*
 * own utils
 */

const _isSameEntity = (cqn, req) => {
  const where = cqn.UPDATE.where || []
  const persistentObj = Array.isArray(req._.partialPersistentState)
    ? req._.partialPersistentState[0]
    : req._.partialPersistentState
  if (!persistentObj) {
    // If no data was found we don't know if it is the same entity
    return false
  }
  const target = getDBTable(req.target)
  if (target.name !== (cqn.UPDATE.entity.ref && cqn.UPDATE.entity.ref[0]) && target.name !== cqn.UPDATE.entity) {
    return false
  }
  for (let i = 0; i < where.length; i++) {
    if (!where[i] || !where[i].ref || !target.elements[where[i].ref]) {
      continue
    }
    const key = where[i].ref
    const val = where[i + 2].val
    const sign = where[i + 1]
    // eslint-disable-next-line
    if (target.elements[key].key && key in persistentObj && sign === '=' && val !== persistentObj[key]) {
      return false
    }
  }
  return true
}

const _getLinksOfCompTree = compositionTree => {
  const links = []
  for (const link of [...compositionTree.backLinks, ...compositionTree.customBackLinks]) {
    links.push(link.entityKey)
  }
  for (const compElement of compositionTree.compositionElements || []) {
    for (const link of [...compElement.backLinks, ...compElement.customBackLinks]) {
      links.push(link.targetKey)
    }
  }
  return links
}

const _whereKeys = keys => {
  const where = []
  keys.forEach(key => {
    if (where.length) where.push('or')
    where.push('(', ...ctUtils.whereKey(key), ')')
  })
  return where
}

const _parentKey = (element, key) => {
  return [...element.customBackLinks, ...element.backLinks].reduce((parentKey, customBackLink) => {
    // TODO: why Object.prototype.hasOwnProperty?
    parentKey[customBackLink.entityKey] = Object.prototype.hasOwnProperty.call(key, customBackLink.targetKey)
      ? key[customBackLink.targetKey]
      : customBackLink.targetVal

    // nested
    if (!parentKey[customBackLink.entityKey]) {
      const splitted = customBackLink.targetKey.split('_')
      let current
      let joined = ''
      while (splitted.length > 1) {
        if (joined) joined += '_'
        joined += splitted.shift()
        if (Object.prototype.hasOwnProperty.call(key, joined)) current = key[joined]
      }
      if (current) parentKey[customBackLink.entityKey] = current[splitted[0]]
    }

    return parentKey
  }, {})
}

const _findWhere = (data, where) => {
  return data.filter(entry => {
    return Object.keys(where).every(key => {
      return where[key] === entry[key]
    })
  })
}

const _keys = (entity, data) => {
  return data.map(entry => {
    return ctUtils.key(entity, entry)
  })
}

const _parentKeys = (element, keys) => {
  return keys.map(key => _parentKey(element, key)).filter(ele => Object.keys(ele).length)
}

const _subData = (data, prop) =>
  data &&
  data.reduce((result, entry) => {
    if (prop in entry) {
      const elementValue = ctUtils.val(entry[prop])
      result.push(...ctUtils.array(elementValue))
    }
    return result
  }, [])

const _subWhere = (result, element) => {
  let where
  const links = [...element.backLinks, ...element.customBackLinks]
  if (links && links.length > 0) {
    where = []
    for (const row of result) {
      if (where.length > 0) {
        where.push('or')
      }
      const whereObj = links.reduce((res, currentLink) => {
        if (Object.prototype.hasOwnProperty.call(row, currentLink.targetKey))
          res[currentLink.entityKey] = row[currentLink.targetKey]
        return res
      }, {})
      const whereCQN = ctUtils.whereKey(whereObj)
      if (whereCQN.length) where.push('(', ...whereCQN, ')')
    }
  }
  return where
}

const _mergeResults = (result, selectData, root, definitions, compositionTree, entityName) => {
  if (root) {
    return [...selectData, ...result]
  } else {
    const parent = definitions[compositionTree.target] || definitions[entityName]
    const assoc = (parent && parent.elements[compositionTree.name]) || {}
    return selectData.map(selectEntry => {
      if (assoc.is2one) {
        selectEntry[compositionTree.name] = selectEntry[compositionTree.name] || {}
      } else if (assoc.is2many) {
        selectEntry[compositionTree.name] = selectEntry[compositionTree.name] || []
      }
      const newData = _findWhere(result, _parentKey(compositionTree, selectEntry))
      if (assoc.is2one && newData[0]) {
        selectEntry[compositionTree.name] = Object.assign(selectEntry[compositionTree.name], newData[0])
      } else if (assoc.is2many) {
        selectEntry[compositionTree.name].push(...newData)
      }
      return selectEntry
    })
  }
}

const _columns = (entity, data, compositionTree, selectAll) => {
  const backLinkKeys = _getLinksOfCompTree(compositionTree)
  const columns = []
  for (const elementName in entity.elements) {
    const element = entity.elements[elementName]
    if (element.virtual || element.isAssociation) continue
    if (
      selectAll ||
      element.key ||
      backLinkKeys.includes(element.name) ||
      (Array.isArray(data) && data.find(entry => element.name in entry))
    ) {
      columns.push({ ref: [element.name] })
    }
  }
  return columns
}

const _select = ({
  definitions,
  entityName,
  draft,
  alias,
  compositionTree,
  data,
  root,
  includeAllRootColumns,
  includeAllColumns,
  where,
  parentKeys,
  orderBy,
  singleton
}) => {
  const entity = definitions && definitions[entityName]
  const from = ctUtils.addDraftSuffix(draft, entity.name)
  const selectCQN = SELECT.from(from)
  if (alias) selectCQN.SELECT.from.as = alias
  const selectAll = includeAllColumns || (includeAllRootColumns && root)
  selectCQN.SELECT.columns = _columns(entity, data, compositionTree, selectAll)
  if (where) selectCQN.SELECT.where = where
  else if (parentKeys) selectCQN.SELECT.where = _whereKeys(parentKeys)
  if (orderBy) selectCQN.SELECT.orderBy = orderBy
  if (singleton) selectCQN.SELECT.limit = { rows: { val: 1 } }
  // REVISIT: remove once SELECT builder does flattening!
  return cqn2cqn4sql(selectCQN, { definitions })
}

const _selectDeepUpdateData = async args => {
  const { definitions, compositionTree, entityName, data, includeAllColumns, root, selectData, tx } = args
  const selectCQN = _select(args)
  const result = await tx.run(selectCQN)
  if (!result.length) return Promise.resolve(result)

  const keys = _keys(definitions[entityName], result)
  await Promise.all(
    compositionTree.compositionElements.map(element => {
      if (element.skipPersistence) return Promise.resolve()
      if (data !== undefined && !data.find(entry => element.name in entry) && !(includeAllColumns && result.length))
        return Promise.resolve()
      const subs = {
        compositionTree: element,
        entityName: element.source,
        data: _subData(data, element.name),
        where: _subWhere(result, element),
        selectData: result,
        parentKeys: _parentKeys(element, keys),
        orderBy: false,
        root: false
      }

      // REVISIT: remove null elements
      subs.data = subs.data.filter(d => d)

      return _selectDeepUpdateData({ ...args, ...subs })
    })
  )

  return _mergeResults(result, selectData || [], root, definitions, compositionTree, entityName)
}

/*
 * exports
 */

const selectDeepUpdateData = (
  definitions,
  cqn,
  req,
  includeAllRootColumns = false,
  includeAllColumns = false,
  service
) => {
  // REVISIT this should be done somewhere before, so it is not done twice for deep updates
  const sqlQuery = cqn2cqn4sql(cqn, { definitions })

  if (req && _isSameEntity(sqlQuery, req)) {
    return Promise.resolve(req._.partialPersistentState)
  }

  const from = getEntityNameFromUpdateCQN(sqlQuery)
  const alias = sqlQuery.UPDATE.entity.as
  const where = sqlQuery.UPDATE.where || []
  const entityName = ensureNoDraftsSuffix(from)
  const draft = entityName !== from
  const orderBy = req && req.target && req.target.query && req.target.query.SELECT && req.target.query.SELECT.orderBy
  const data = Object.assign({}, sqlQuery.UPDATE.data || {}, cqn.UPDATE.with || {})
  const compositionTree = getCompositionTree({
    definitions,
    rootEntityName: entityName, // REVISIT: drafts are resolved too eagerly
    checkRoot: false,
    resolveViews: !draft,
    service
  })

  return _selectDeepUpdateData({
    tx: cds.tx(req),
    definitions,
    compositionTree,
    entityName,
    data: [data],
    where,
    orderBy,
    draft,
    includeAllRootColumns,
    singleton: req && req.target && req.target._isSingleton,
    alias,
    includeAllColumns: cqn._selectAll || includeAllColumns,
    root: true,
    service
  })
}

module.exports = {
  selectDeepUpdateData
}
