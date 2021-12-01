/*
 * input handler on DatabaseService level
 *
 * - remove virtual fields
 * - add managed, if not provided
 * - assert not nullable without default (for better error message)
 * - add default values, if not provided (mainly for rest response body, odata does read after write)
 * - add UUIDs
 * - propagate keys
 */

const cds = require('../../cds')

const normalizeTimeData = require('../utils/normalizeTimeData')

const { enrichDataWithKeysFromWhere } = require('../../common/utils/keys')
const { propagateForeignKeys } = require('../../common/utils/propagateForeignKeys')
const getTemplate = require('../../common/utils/template')
const templateProcessor = require('../../common/utils/templateProcessor')

const { checkIfAssocDeep } = require('../../cds-services/util/assert')

const { DRAFT_COLUMNS_MAP } = require('../../common/constants/draft')

const _isManaged = (category, event) =>
  (category === '@cds.on.insert' && event === 'CREATE') || (category === '@cds.on.update' && event === 'UPDATE')

const _processComplexCategory = ({ row, key, val, category, req, element }) => {
  const categoryArgs = category.args
  category = category.category

  // propagate keys
  if (category === 'propagateForeignKeys') {
    propagateForeignKeys(key, row, element._foreignKeys, element._isCompositionEffective)
    return
  }

  // managed
  if (val === undefined && _isManaged(category, req.event)) {
    if (typeof categoryArgs === 'object') {
      const val = categoryArgs['=']
      if (val.match(/^\$/)) row[key] = val
      else row[key] = row[val]
    } else {
      row[key] = categoryArgs
    }
    return
  }

  // not null with default for rest response body and ensure utc
  if (category === 'default' && val === undefined && req.event === 'CREATE') {
    const { default: dfault } = categoryArgs
    if ('val' in dfault) row[key] = dfault.val
    else if ('ref' in dfault && dfault.ref[0] === '$now') {
      row[key] =
        categoryArgs.type === 'cds.DateTime'
          ? new Date(req.timestamp).toISOString().replace(/\.\d\d\d/, '')
          : req.timestamp
    }
  }
}

const _processCategory = ({ category, row, key, element, val, req }) => {
  // use args only inside this if (sonar type error warning)
  if (typeof category === 'object') {
    _processComplexCategory({ category, row, key, val, req, element })
    return
  }

  // virtual
  if (category === 'virtual') {
    delete row[key]
    return
  }

  // not null without default (for better error message)
  if (category === '!default' && val == null && req.event === 'CREATE') {
    req.error(400, 'ASSERT_NOT_NULL', key, [key])
    return
  }

  // generate UUIDs
  if (category === 'uuid' && !val && req.event === 'CREATE') {
    row[key] = cds.utils.uuid()
  }

  // check for forbidden deep operations for association
  if (category === 'associationEffective' && (req.event === 'CREATE' || req.event === 'UPDATE')) {
    checkIfAssocDeep(element, val, req)
  }
}

const processorFn =
  req =>
  ({ row, key, element, plain }) => {
    const categories = plain.categories
    const val = row[key]

    for (const category of categories) {
      _processCategory({ category, row, key, element, val, req })
    }
  }

// params: element, target, parent, templateElements
const _pick = element => {
  // collect actions to apply
  const categories = []

  if (element.virtual) {
    categories.push('virtual')
    return { categories } // > no need to continue
  }

  if (
    element.notNull &&
    element['@assert.notNull'] !== false &&
    !element.default &&
    element.type !== 'cds.Association'
  ) {
    categories.push('!default')
  }

  if (element.default && !DRAFT_COLUMNS_MAP[element.name]) {
    categories.push({ category: 'default', args: element })
  }

  if (element['@cds.on.insert']) {
    categories.push({ category: '@cds.on.insert', args: element['@cds.on.insert'] })
  }

  if (element['@cds.on.update']) {
    categories.push({ category: '@cds.on.update', args: element['@cds.on.update'] })
  }

  if (element._isAssociationEffective && !element._target._hasPersistenceSkip) {
    categories.push('associationEffective')
  }

  if (element.isAssociation && element._foreignKeys.length) {
    categories.push({ category: 'propagateForeignKeys' })
  }

  // generate uuid
  if (element.key && !DRAFT_COLUMNS_MAP[element.name] && element.type === 'cds.UUID') {
    categories.push('uuid')
  }

  if (categories.length) return { categories }
}

const _pickVirtual = element => {
  // collect actions to apply
  const categories = []
  if (element.virtual) categories.push('virtual')
  if (categories.length) return { categories }
}

function _handler(req) {
  if (!this.model || typeof req.query === 'string' || !req.target) return

  // call with this for this.model
  normalizeTimeData.call(this, req)

  const draft = req.target.name && req.target.name.match(/_drafts$/)

  const target =
    req.target._unresolved && req.target.name
      ? this.model.definitions[req.target.name.replace(/_drafts$/, '')]
      : req.target
  if (!target || target._unresolved) return

  // REVISIT: probably need to filter for .columns/.rows combination as well
  if (req.query.INSERT && !req.query.INSERT.entries) return

  let template
  if (draft) {
    // draft -> filter virtual only
    template = getTemplate('db-virtual', this, target, { pick: _pickVirtual })
  } else {
    template = getTemplate('db-input', this, target, { pick: _pick })
  }

  if (template.elements.size === 0) return

  if (!draft) enrichDataWithKeysFromWhere(req.data, req, this)

  const data = Array.isArray(req.data) ? req.data : [req.data]
  for (const row of data) {
    templateProcessor({ processFn: processorFn(req), row, template })
  }
}

_handler._initial = true

module.exports = _handler
