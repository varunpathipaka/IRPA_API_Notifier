const cds = require('../../cds')

const { getCompositionTree } = require('./tree')
const ctUtils = require('./utils')

const { ensureNoDraftsSuffix } = require('../utils/draft')
const { getEntityNameFromDeleteCQN } = require('../utils/cqn')

/*
 * own utils
 */

// Poor man's alias algorithm
// REVISIT: Extract and adapt the alias functionality from `expandCQNToJoin.js`: _adaptWhereOrderBy
const _recursivelyAliasRefs = (something, newAlias, oldAlias, subselect = false) => {
  if (Array.isArray(something)) {
    for (const s of something) _recursivelyAliasRefs(s, newAlias, oldAlias, subselect)
  } else if (typeof something === 'object') {
    if (Array.isArray(something.ref)) {
      if (oldAlias && something.ref[0] === oldAlias) something.ref[0] = newAlias
      else if (!subselect) something.ref.unshift(newAlias)
    } else {
      for (const key in something) {
        if (key === 'from') continue // Workaround: Deep delete to be rewritten
        _recursivelyAliasRefs(something[key], newAlias, oldAlias, subselect || key === 'SELECT')
      }
    }
  }
}

function _getSubWhereAndEntities(element, parentWhere, draft, level = 0, compositionTree = {}) {
  const allBackLinks = [...element.backLinks, ...element.customBackLinks]
  let entity1, entity2
  const linksForWhere = allBackLinks.length ? allBackLinks : element.links

  const subWhere = linksForWhere.reduce((result, backLink) => {
    // exclude static values from subwhere
    if (backLink.entityKey && !backLink.targetKey && backLink.targetVal !== undefined) {
      return result
    }
    if (result.length > 0) {
      result.push('and')
    }

    entity1 = {
      alias: `ALIAS${level + 1}`,
      entityName: ctUtils.addDraftSuffix(draft, element.source),
      propertyName: backLink.entityKey
    }

    const res1 = backLink.entityKey ? { ref: [entity1.alias, entity1.propertyName] } : { val: backLink.entityVal }

    entity2 = {
      alias: `ALIAS${level}`,
      entityName: ctUtils.addDraftSuffix(draft, element.target || element.source),
      propertyName: backLink.targetKey
    }

    const res2 = backLink.targetKey ? { ref: [entity2.alias, entity2.propertyName] } : { val: backLink.targetVal }

    result.push(res1, '=', res2)
    return result
  }, [])

  const where = []
  if (!subWhere.length) return { where, entity1, entity2 }

  let whereKeys = _getWhereKeys(allBackLinks, entity1)
  const staticWhereValues = _getStaticWhere(allBackLinks, entity1)
  if (whereKeys.length === 0 && element.links.length === 1) {
    // add is null check for each unused backlink
    for (const ce of compositionTree.compositionElements || []) {
      if (ce.source !== element.source) continue
      if (ce.name === element.name) continue
      const wk = _getWhereKeys([...ce.backLinks, ...ce.customBackLinks], entity1, 'null')
      if (whereKeys.length === 0) whereKeys = wk
      else whereKeys.push('and', ...wk)
    }
  }

  if (whereKeys.length > 0) {
    where.push('(', ...whereKeys, ')', 'and')
  }
  if (staticWhereValues.length > 0) {
    where.push('(', ...staticWhereValues, ')', 'and')
  }
  where.push('exists', {
    SELECT: {
      columns: [{ val: 1, as: '_exists' }],
      from: { ref: [entity2.entityName], as: entity2.alias },
      where: parentWhere ? ['(', ...parentWhere, ')', 'and', '(', ...subWhere, ')'] : subWhere
    }
  })

  return {
    where,
    entity1,
    entity2
  }
}

function _getWhereKeys(allBackLinks, entity1, is) {
  return allBackLinks.reduce((result, backLink) => {
    // exclude static keys
    if (backLink.entityKey && !backLink.targetKey && backLink.targetVal !== undefined) {
      return result
    }
    if (result.length > 0) {
      result.push('or')
    }
    if (backLink.entityKey && is) {
      result.push({ ref: [entity1.alias, backLink.entityKey] }, 'is ' + is)
    } else if (backLink.entityVal !== undefined) {
      // static values should not be included
      result.pop()
    }
    return result
  }, [])
}

function _getStaticWhere(allBackLinks, entity1) {
  return allBackLinks.reduce((result, backLink) => {
    if (result.length > 0) {
      result.push('and')
    }
    if (backLink.entityKey && !backLink.targetKey && backLink.targetVal !== undefined) {
      result.push({ ref: [entity1.alias, backLink.entityKey] }, '=', { val: backLink.targetVal })
    }
    return result
  }, [])
}

const _is2oneComposition = (element, definitions) => {
  const csnElement = element.target && definitions[element.target].elements[element.name]
  return csnElement && csnElement.is2one && csnElement._isCompositionEffective
}

const _addToCQNs = (cqns, subCQN, element, definitions, level) => {
  cqns[level] = cqns[level] || []
  // Since `>2.5.2` compiler generates constraints for compositions of one like for annotations
  // Thus only single 2one case (`$self`-managed composition) has DELETE CASCADE
  // Here it's ignored to simplify i.e. handle all "2ones" in a same manner
  if (!cds.env.features._foreign_key_constraints || _is2oneComposition(element, definitions)) {
    cqns[level].push(subCQN)
  }
}

// unofficial config!
const DEEP_DELETE_MAX_RECURSION_DEPTH =
  (cds.env.features.recursion_depth && Number(cds.env.features.recursion_depth)) || 2

const _addSubCascadeDeleteCQN = (
  definitions,
  compositionTree,
  parentWhere,
  level,
  cqns,
  draft,
  elementMap = new Map()
) => {
  for (const element of compositionTree.compositionElements) {
    if (element.skipPersistence) continue

    const fqn = compositionTree.source + ':' + element.name
    const seen = elementMap.get(fqn)
    if (seen && seen >= DEEP_DELETE_MAX_RECURSION_DEPTH) {
      // recursion -> abort
      continue
    }

    // REVISIT: sometimes element.target is undefined which leads to self join
    if (!element.target) element.target = compositionTree.source

    const { entity1, where } = _getSubWhereAndEntities(element, parentWhere, draft, level, compositionTree)
    if (where.length) {
      const subCQN = { DELETE: { from: { ref: [entity1.entityName], as: entity1.alias }, where: where } }

      _addToCQNs(cqns, subCQN, element, definitions, level)

      // Make a copy and do not share the same map among brother compositions
      // as we're only interested in deep recursions, not wide recursions.
      const newElementMap = new Map(elementMap)
      newElementMap.set(fqn, (seen && seen + 1) || 1)
      _addSubCascadeDeleteCQN(definitions, element, subCQN.DELETE.where, level + 1, cqns, draft, newElementMap)
    }
  }

  return cqns
}

/*
 * exports
 */

const hasDeepDelete = (definitions, cqn) => {
  const from = getEntityNameFromDeleteCQN(cqn)
  if (!from) return false

  // hidden flag for DELETEs on draft root, we have a separate mechanism that deletes the rows using the DraftUUID
  // Hence, we do not need a deep delete in that case.
  if (cqn._suppressDeepDelete) return false

  const entity = definitions && definitions[ensureNoDraftsSuffix(from)]

  if (entity) return !!Object.keys(entity.elements || {}).find(k => entity.elements[k]._isCompositionEffective)

  return false
}

const _getSetNullParentForeignKeyCQNs = (definitions, entityName, parentWhere, draft) => {
  const setNullCQNs = []
  for (const { elements } of definitions[entityName].__oneCompositionParents.values()) {
    for (const element of elements.values()) {
      const data = element.links.reduce((d, fk) => {
        d[fk.entityKey] = null
        return d
      }, {})
      const { entity1, where } = _getSubWhereAndEntities(element, parentWhere, draft)
      if (where.length) {
        setNullCQNs.push({
          UPDATE: {
            entity: { ref: [entity1.entityName], as: entity1.alias },
            data,
            where,
            _beforeDelete: true
          }
        })
      }
    }
  }
  return setNullCQNs
}

const getDeepDeleteCQNs = (definitions, cqn) => {
  const from = getEntityNameFromDeleteCQN(cqn)
  if (!from) return [[cqn]]

  const entityName = ensureNoDraftsSuffix(from)
  // REVISIT: baaad check!
  const draft = entityName !== from
  const compositionTree = getCompositionTree({
    definitions,
    rootEntityName: entityName,
    checkRoot: false,
    resolveViews: !draft,
    service: cds.db
  })
  const parentWhere = cqn.DELETE.where && JSON.parse(JSON.stringify(cqn.DELETE.where))
  if (parentWhere) {
    const parentAlias = cqn.DELETE.from.as || (cqn.DELETE.from.ref && cqn.DELETE.from.ref[0]) || cqn.DELETE.from // or however we get the table name...
    _recursivelyAliasRefs(parentWhere, 'ALIAS0', parentAlias)
  }
  const setNullUpdates = []
  if (cds.env.features._foreign_key_constraints && definitions[entityName].own('__oneCompositionParents')) {
    setNullUpdates.push(..._getSetNullParentForeignKeyCQNs(definitions, entityName, parentWhere, draft))
  }
  const subCascadeDeletes = _addSubCascadeDeleteCQN(definitions, compositionTree, parentWhere, 0, [], draft)
  return [[cqn], ...subCascadeDeletes, ...setNullUpdates].reverse()
}

module.exports = {
  hasDeepDelete,
  getDeepDeleteCQNs
}
