const cds = require('../../cds')

const { getCompositionTree } = require('./tree')
const ctUtils = require('./utils')

const { ensureNoDraftsSuffix } = require('../utils/draft')
const { deepCopyArray } = require('../utils/copy')

/*
 * own utils
 */

function _hasCompOrAssocIgnoreEmptyToMany(entity, k, data) {
  // TODO once REST also uses same logic as odata structured check if we can omit 'entity.elements[k] &&'
  return entity.elements[k] && (entity.elements[k].is2one || (entity.elements[k].is2many && data[k] && data[k].length))
}

const _addSubDeepInsertCQN = (definitions, compositionTree, data, cqns, draft) => {
  compositionTree.compositionElements.forEach(element => {
    if (element.skipPersistence) {
      return
    }
    // element source must be changed in comp tree
    const subEntity = definitions[element.source]
    const into = ctUtils.addDraftSuffix(draft, subEntity.name)
    const insertCQN = { INSERT: { into: into, entries: [] } }
    const subData = data.reduce((result, entry) => {
      if (element.name in entry) {
        const elementValue = ctUtils.val(entry[element.name])
        if (elementValue != null) {
          // remove empty entries
          const subData = ctUtils.array(elementValue).filter(ele => Object.keys(ele).length > 0)
          if (subData.length > 0) {
            // REVISIT: this can make problems
            insertCQN.INSERT.entries.push(...ctUtils.cleanDeepData(subEntity, subData))
            result.push(...subData)
          }
        }
      }
      return result
    }, [])
    if (insertCQN.INSERT.entries.length > 0) {
      cqns.push(insertCQN)
    }
    if (subData.length > 0) {
      _addSubDeepInsertCQN(definitions, element, subData, cqns, draft)
    }
  })
  return cqns
}

/*
 * exports
 */

const _entityFromINSERT = (definitions, INSERT) => {
  if (INSERT && INSERT.into) {
    const entityName = ensureNoDraftsSuffix(INSERT.into.name || INSERT.into)
    return definitions && definitions[entityName]
  }
}

const cleanEmptyCompositionsOfMany = (definitions, cqn) => {
  const entity = _entityFromINSERT(definitions, cqn.INSERT)
  if (!entity) return
  for (const entry of cqn.INSERT.entries || []) {
    for (const elName in entry || {}) {
      const el = entity.elements[elName]
      if (!el) continue
      if (el.is2many && !entry[elName].length) delete entry[elName]
    }
  }
}

const hasDeepInsert = (definitions, cqn) => {
  if (cqn.INSERT.entries) {
    const entity = _entityFromINSERT(definitions, cqn.INSERT)
    if (entity) {
      return !!cqn.INSERT.entries.find(entry => {
        return !!Object.keys(entry || {}).find(k => {
          return _hasCompOrAssocIgnoreEmptyToMany(entity, k, entry)
        })
      })
    }
  }
  return false
}

const getDeepInsertCQNs = (definitions, cqn) => {
  const into = cqn.INSERT.into.name || cqn.INSERT.into
  const entityName = ensureNoDraftsSuffix(into)
  const draft = entityName !== into
  const dataEntries = cqn.INSERT.entries ? deepCopyArray(cqn.INSERT.entries) : []
  const entity = definitions && definitions[entityName]
  const compositionTree = getCompositionTree({
    definitions,
    rootEntityName: entityName,
    checkRoot: false,
    resolveViews: !draft,
    service: cds.db
  })

  const flattenedCqn = { INSERT: Object.assign({}, cqn.INSERT) }
  flattenedCqn.INSERT.entries = []

  dataEntries.forEach(dataEntry =>
    flattenedCqn.INSERT.entries.push(ctUtils.cleanDeepData(entity, Object.assign({}, dataEntry)))
  )

  return [flattenedCqn, ..._addSubDeepInsertCQN(definitions, compositionTree, dataEntries, [], draft)]
}

module.exports = {
  cleanEmptyCompositionsOfMany,
  hasDeepInsert,
  getDeepInsertCQNs
}
