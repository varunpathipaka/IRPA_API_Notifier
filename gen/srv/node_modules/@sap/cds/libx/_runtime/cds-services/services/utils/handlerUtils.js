const cds = require('../../../cds')

const { SELECT } = cds.ql

const { checkReferenceIntegrity } = require('../../util/assert')
const { processDeep, processDeepAsync } = require('../../util/dataProcessUtils')

const { DRAFT_COLUMNS } = require('../../../common/constants/draft')

const _isAssociationToOneManaged = element => element._isAssociationEffective && element.is2one && !element.on

const _getWheres = (key, data) => {
  const wheres = []
  for (const d of data) {
    wheres.push({ [key.name]: d[key.name] })
  }
  return wheres
}

const allKeysAreProvided = req => {
  const data = req.data && (Array.isArray(req.data) ? req.data : [req.data])
  for (const key of Object.values(req.target.keys || {})) {
    if (key._isAssociationStrict || DRAFT_COLUMNS.includes(key.name)) {
      continue
    }
    for (const d of data) {
      if (d[key.name] === undefined) return false
    }
  }
  return true
}

const _addWhereToCqn = (wheres, target, cqn) => {
  if (wheres.length === 1) {
    const whereObj = wheres[0]
    for (const key in whereObj) {
      // check if we have structured elements
      if (target.elements[key].elements) {
        whereObj[key] = { val: JSON.stringify(whereObj[key]) }
      }
    }
    cqn.where(whereObj)
  } else {
    cqn.where({ or: wheres })
  }
}

const _getSelectCQN = (req, columns) => {
  const cqn = SELECT.from(req.target)

  if (columns) {
    cqn.columns(...columns)
  }

  const data = req.data && (Array.isArray(req.data) ? req.data : [req.data])

  for (const key of Object.values(req.target.keys || {})) {
    if (key._isAssociationStrict || DRAFT_COLUMNS.includes(key.name)) {
      continue
    }

    const wheres = _getWheres(key, data)
    if (wheres.length) {
      _addWhereToCqn(wheres, req.target, cqn)
    }
  }

  if (req.target.query && req.target.query.SELECT && req.target.query.SELECT.orderBy) {
    cqn.SELECT.orderBy = req.target.query.SELECT.orderBy
  }

  return cqn
}

function _fillForeignKeysWithNull(managedAssocToOneElement, row) {
  for (const key of managedAssocToOneElement._foreignKeys) {
    if (key.parentElement) row[key.parentElement.name] = null
  }
}

const _flattenManagedToOneAssociation = (managedAssocToOneElement, entity, row, property, csn, req) => {
  const targetEntity = managedAssocToOneElement._target
  for (const key in targetEntity.keys) {
    const el = targetEntity.keys[key]
    if (_isAssociationToOneManaged(el)) {
      if (row[managedAssocToOneElement.name] === null) {
        _fillForeignKeysWithNull(managedAssocToOneElement, row)
        continue
      }
      _flattenManagedToOneAssociation(el, targetEntity, row[managedAssocToOneElement.name], key, csn, req)
      continue
    }

    if (row[managedAssocToOneElement.name] === null) {
      row[managedAssocToOneElement.name + '_' + key] = null
    } else {
      row[managedAssocToOneElement.name + '_' + key] = row[managedAssocToOneElement.name][key]
      delete row[managedAssocToOneElement.name][key]
    }
  }

  if (row[managedAssocToOneElement.name] === null || Object.keys(row[managedAssocToOneElement.name]).length === 0) {
    // if there are no non key values left, remove assoc object
    delete row[managedAssocToOneElement.name]
  }
}

const _flattenDeepToOneAssociations = (entity, data, csn, req) => {
  if (!Array.isArray(data)) {
    return _flattenDeepToOneAssociations(entity, [data], csn, req)
  }

  for (const row of data) {
    for (const property in row) {
      const element = entity.elements[property]
      if (!element) continue

      if (_isAssociationToOneManaged(element)) {
        _flattenManagedToOneAssociation(element, entity, row, property, csn, req)
      } else if (element.elements) {
        _flattenDeepToOneAssociations(element, [row[element.name]], csn, req)
      }
    }
  }
}

const flattenDeepToOneAssociations = (req, csn) => {
  if (!req.target) {
    return
  }

  if (req.event !== 'CREATE' && req.event !== 'UPDATE') {
    return
  }

  // REVISIT: adopt template mechanism?
  processDeep(
    (data, entity) => {
      _flattenDeepToOneAssociations(entity, data, csn, req)
    },
    req.data,
    req.target,
    false,
    true
  )
}

const checkIntegrityWrapper = (req, csn, run) => async (data, entity) => {
  const errors = await checkReferenceIntegrity(entity, data, req, csn, run)
  if (errors && errors.length !== 0) {
    for (const err of errors) {
      req.error(err)
    }
  }
}

const _isUncheckableInsert = query => {
  return query.INSERT && (query.INSERT.rows || query.INSERT.values || query.INSERT.as)
}

// REVISIT: lower to db layer, where it's used
const checkIntegrityUtil = async (req, csn, run) => {
  if (!run) return

  // REVISIT
  if (typeof req.query === 'string' || req.target._unresolved) return

  // FIXME: doesn't work for uncheckable inserts
  if (_isUncheckableInsert(req.query)) return

  // REVISIT: integrity check needs context.data
  if (Object.keys(req.data).length === 0) {
    // REVISIT: We may need to double-check re req.data being undefined or empty
    if (req.query.DELETE) {
      req.data = req._beforeDeleteData || {}
    } else if (req.context && req.context.data && Object.keys(req.context.data).length > 0) {
      req.data = req.context.data
    }
  }
  if (Object.keys(req.data).length === 0) return

  await processDeepAsync(checkIntegrityWrapper(req, csn, run), req.data, req.target, false, true)
}

/*
 * merge CQNs
 */
const _mergeExpandCQNs = cqns => {
  const cols = cqns[0].SELECT.columns
  for (const c of cqns) {
    for (const col of c.SELECT.columns) {
      if (col.expand && !cols.find(ele => ele.ref[0] === col.ref[0])) {
        cols.push(col)
      }
    }
  }
  return cqns[0]
}

/*
 * recursively builds a select cqn for deep read after write
 * (depth determined by req.data)
 */
const getDeepSelect = req => {
  // REVISIT: Why do we do such expensive deep reads after write at all ???
  const { elements } = req.target
  const cols = []

  for (const each in elements) {
    if (DRAFT_COLUMNS.includes(each)) continue
    const e = elements[each]
    if (!e.isAssociation) cols.push(each)
    else if (e._isCompositionEffective) {
      const cqns = []
      // REVISIT: This recursively creates lots of nested selects only to keep a few
      for (const row of Array.isArray(req.data) ? req.data : [req.data]) {
        let d = row[each]
        if (!d) continue
        else if (!Array.isArray(d)) d = [d]
        else if (d.length === 0) continue
        const nested = d.map(data => getDeepSelect({ target: e._target, data }))
        cqns.push(_mergeExpandCQNs(nested))
      }
      if (cqns.length > 0) {
        const { SELECT } = _mergeExpandCQNs(cqns)
        cols.push({ ref: [each], expand: SELECT.columns })
      }
    }
  }

  // root? -> with where clause
  return req.event ? _getSelectCQN(req, cols) : SELECT.from(req.target, cols)
}

module.exports = {
  getDeepSelect,
  allKeysAreProvided,
  checkIntegrityUtil,
  flattenDeepToOneAssociations
}
