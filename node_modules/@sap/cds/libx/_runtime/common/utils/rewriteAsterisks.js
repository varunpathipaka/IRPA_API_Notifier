const { getNavigationIfStruct } = require('./structured')
const getColumns = require('../../db/utils/columns')
const { ensureDraftsSuffix } = require('./draft')

const isAsteriskColumn = col => col === '*' || (col.ref && col.ref[0] === '*' && !col.expand)

const _isDuplicate = newColumn => column => {
  if (newColumn.as) return column.as && column.as === newColumn.as
  if (!column.ref) return
  if (Array.isArray(newColumn)) newColumn = { ref: newColumn }
  return newColumn.ref ? newColumn.ref.join('_') === column.ref.join('_') : newColumn === column.ref.join('_')
}

const _cqlDraftColumns = target => {
  if (target.name.endsWith('DraftAdministrativeData')) return []
  const draftName = ensureDraftsSuffix(target.name)
  const subSelect = SELECT.from(draftName).columns([1])
  for (const key in target.keys) {
    if (key !== 'IsActiveEntity') subSelect.where([{ ref: [target.name, key] }, '=', { ref: [draftName, key] }])
  }
  return [
    { val: true, as: 'IsActiveEntity', cast: { type: 'cds.Boolean' } },
    { val: false, as: 'HasActiveEntity', cast: { type: 'cds.Boolean' } },
    {
      xpr: ['case', 'when', 'exists', subSelect, 'then', 'true', 'else', 'false', 'end'],
      as: 'HasDraftEntity',
      cast: { type: 'cds.Boolean' }
    }
  ]
}

const _expandColumn = (column, target, db) => {
  if (!(column.ref && column.expand)) return
  const nextTarget = getNavigationIfStruct(target, column.ref)
  if (nextTarget && nextTarget._target && nextTarget._target.elements) _rewriteAsterisks(column, nextTarget._target, db)
  return column
}

const rewriteExpandAsterisk = (columns, target) => {
  const expandAllColIdx = columns.findIndex(col => {
    if (col.ref || !col.expand) return
    return !Array.isArray(col.expand) ? col.expand === '*' : col.expand.indexOf('*') > -1
  })
  if (expandAllColIdx > -1) {
    const { expand } = columns.splice(expandAllColIdx, 1)[0]
    for (const elName in target.elements) {
      if (target.elements[elName]._target && !columns.find(col => col.expand && col.ref && col.ref[0] === elName)) {
        columns.push({ ref: [elName], expand: [...expand] })
      }
    }
  }
}

const _rewriteAsterisk = (columns, target, db, isRoot) => {
  const asteriskColumnIndex = columns.findIndex(col => isAsteriskColumn(col))
  if (asteriskColumnIndex > -1) {
    columns.splice(
      asteriskColumnIndex,
      1,
      ...getColumns(target, { db })
        .map(c => ({ ref: [c.name] }))
        .filter(c => !columns.find(_isDuplicate(c)) && (isRoot || c.ref[0] !== 'DraftAdministrativeData_DraftUUID'))
    )
  }
}

const _rewriteAsterisks = (cqn, target, db, isRoot) => {
  if (cqn.expand === '*') cqn.expand = ['*']
  const columns = cqn.expand || cqn.columns
  _rewriteAsterisk(columns, target, db, isRoot)
  rewriteExpandAsterisk(columns, target)
  for (const column of columns) {
    _expandColumn(column, target, db)
  }
  return columns
}

const rewriteAsterisks = (query, target, db = false, isDraft = false, onlyKeys = false) => {
  if (!target || target.name.endsWith('_drafts')) return
  if (!query.SELECT.columns || (query.SELECT.columns && !query.SELECT.columns.length)) {
    if (isDraft || db) {
      query.SELECT.columns = getColumns(target, { db, onlyKeys }).map(col => ({ ref: [col.name] }))
      if (db && target._isDraftEnabled) query.SELECT.columns.push(..._cqlDraftColumns(target))
    }
    return
  }
  query.SELECT.columns = _rewriteAsterisks(query.SELECT, target, db, true)
}

module.exports = {
  rewriteAsterisks,
  isAsteriskColumn,
  rewriteExpandAsterisk
}
