const cds = require('../../../../cds')
const getTemplate = require('../../../../common/utils/template')
const templateProcessor = require('../../../../common/utils/templateProcessor')
const { big } = require('@sap/cds-foss')
const { omitValue, applyOmitValuesPreference } = require('./omitValues')

const METADATA = {
  $context: '*@odata.context',
  $count: '*@odata.count',
  $etag: '*@odata.etag',
  $metadataEtag: '*@odata.metadataEtag',
  $bind: '*@odata.bind',
  $id: '*@odata.id',
  $delta: '*@odata.delta',
  $removed: '*@odata.removed',
  $type: '*@odata.type',
  $nextLink: '*@odata.nextLink',
  $deltaLink: '*@odata.deltaLink',
  $editLink: '*@odata.editLink',
  $readLink: '*@odata.readLink',
  $navigationLink: '*@odata.navigationLink',
  $associationLink: '*@odata.associationLink',
  $mediaEditLink: '*@odata.mediaEditLink',
  $mediaReadLink: '*@odata.mediaReadLink',
  $mediaContentType: '*@odata.mediaContentType',
  $mediaEtag: '*@odata.mediaEtag'
}

/**
 * Convert any result to the result object structure, which is expected of odata-v4.
 *
 * @param {*} result
 * @param {*} [arg]
 * @returns {string | object}
 */
const toODataResult = (result, arg) => {
  if (result === undefined || result === null) return ''

  if (arg) {
    if (typeof arg === 'object') {
      arg = arg._.odataReq.getUriInfo().getLastSegment().isCollection() ? 'Array' : ''
    }

    if (!Array.isArray(result) && arg === 'Array') {
      result = [result]
    } else if (Array.isArray(result) && arg !== 'Array') {
      result = result[0]
    }
  }

  const odataResult = {
    value: result
  }

  if (typeof result === 'object') {
    for (const key in METADATA) {
      // do not set "@odata.context" as it may be inherited of remote service
      if (key === '$context') {
        delete result[key]
        continue
      }

      if (key in result) {
        odataResult[METADATA[key]] = result[key]
        delete result[key]
      }
    }
  }

  return odataResult
}

const addEtags = (row, key) => {
  row['*@odata.etag'] = row[key]
}

const convertDecimal = (row, key, options) => {
  const bigValue = big(row[key])
  row[key] = options.decimals.exponential ? bigValue.toExponential() : bigValue.toFixed()
}

const addAssociationToRow = (row, foreignKey, foreignKeyElement) => {
  const assocName = foreignKeyElement['@odata.foreignKey4']
  const assoc = foreignKeyElement.parent.elements[assocName]

  if (!row[assocName]) {
    row[assocName] = {}
  }

  const keyOfAssociatedEntity = foreignKey.replace(`${assocName}_`, '')

  // REVISIT: structured keys, see xtests in structured-x4
  if (assoc._target.keys[keyOfAssociatedEntity] && assoc._target.keys[keyOfAssociatedEntity]['@odata.foreignKey4']) {
    // assoc as key
    row[assocName][keyOfAssociatedEntity] = row[foreignKey]
    delete row[foreignKey]
    addAssociationToRow(row[assocName], keyOfAssociatedEntity, assoc._target.keys[keyOfAssociatedEntity])
    return
  }

  // foreign key null or undefined, set assoc to null
  if (row[foreignKey] == null) {
    row[assocName] = null
    delete row[foreignKey]
    return
  }

  if (row[assocName][keyOfAssociatedEntity] === undefined) {
    row[assocName][keyOfAssociatedEntity] = row[foreignKey]
  }

  delete row[foreignKey]
}

const _processCategory = (category, processArgs, req, options, previousResult) => {
  const { row, key, element } = processArgs

  switch (category) {
    case '@odata.omitValues':
      omitValue(processArgs, req, options.omitValuesPreference, previousResult)
      break

    case '@odata.etag':
      addEtags(row, key)
      break

    case '@cds.Decimal':
      if (options.decimals && row[key]) convertDecimal(row, key, options)
      break

    case '@odata.foreignKey4':
      addAssociationToRow(row, key, element)
      break

    case '@cleanup':
      if (key !== 'DraftAdministrativeData_DraftUUID') delete row[key]
      break

    // no default
  }
}

const _processorFn = (req, previousResult, options) => processArgs => {
  const { row, key, plain } = processArgs
  if (typeof row !== 'object' || !Object.prototype.hasOwnProperty.call(row, key)) return
  const categories = plain.categories

  for (const category of categories) {
    _processCategory(category, processArgs, req, options, previousResult)
  }
}

const _getParent = (model, name) => {
  const target = model.definitions[name]

  if (target && target.elements) {
    for (const elementName in target.elements) {
      const element = target.elements[elementName]
      if (element._anchor && element._anchor._isContained) return element._anchor
    }
  }

  return null
}

const _isUpAssoc = element => element && /^up_(_up_)*$/.test(element.name) && _isContainedOrBackLink(element)

const _isContainedOrBackLink = element =>
  element &&
  element.isAssociation &&
  element.keys &&
  (element._isContained || (element._anchor && element._anchor._isContained))

const _assocs = (element, target) => {
  const assocName = element['@odata.foreignKey4']
  const assoc = assocName && target.elements[assocName]

  if (cds.env.effective.odata.refs) {
    // expand assoc keys except of up_ backlinks
    if (element['@odata.foreignKey4'] && !_isUpAssoc(assoc)) {
      return ['@odata.foreignKey4']
    }

    if (element['@odata.containment.ignore']) {
      return ['@cleanup']
    }
  }

  if (_isContainedOrBackLink(assoc)) {
    return ['@cleanup']
  }

  return []
}

const _pick = options => (element, target, parent) => {
  const categories = []

  if (element['@odata.etag']) categories.push('@odata.etag')
  if (element.type === 'cds.Decimal') categories.push('@cds.Decimal')
  categories.push(..._assocs(element, target))
  if (options.omitValuesPreference) categories.push('@odata.omitValues')
  if (categories.length) return { categories }
}

const _getOptions = headers => {
  const options = {
    decimals: null,
    omitValuesPreference: null
  }

  if (!headers) return options

  // REVISIT: options ie headers are not used in _pick since its cached
  // consequently, all decimals are always picked even w/o these headers
  const acceptHeader = headers.accept

  if (acceptHeader && acceptHeader.includes('IEEE754Compatible=true')) {
    options.decimals = { exponential: acceptHeader.includes('ExponentialDecimals=true') }
  }

  const preferHeader = headers.prefer

  if (preferHeader && preferHeader.includes('omit-values=')) {
    options.omitValuesPreference = new Map()
  }

  return options
}

const _generateCacheKey = (headers, options) => {
  let key = 'postProcess' // default template cache key for post processing
  if (headers.prefer) key += `:${headers.prefer}`
  if (options.decimals) key += `:exponentialDecimals=true`
  return key
}

const postProcess = (req, res, service, result, previousResult) => {
  const { model } = service
  const { headers, target } = req

  if (!target || !result || !model || !model.definitions[target.name]) return

  const options = _getOptions(headers)
  const cacheKey = _generateCacheKey(headers, options)
  const parent = _getParent(model, target.name)
  const template = getTemplate(cacheKey, service, target, { pick: _pick(options) }, parent)

  if (template.elements.size === 0) return

  // normalize result to rows
  result = result.value != null && Object.keys(result).filter(k => !k.match(/^\W/)).length === 1 ? result.value : result

  if (typeof result === 'object' && result != null) {
    const rows = Array.isArray(result) ? result : [result]

    // process each row
    const processFn = _processorFn(req, previousResult, options)

    for (const row of rows) {
      const args = {
        processFn,
        row,
        template,
        pathOptions: {
          includeKeyValues: false
        }
      }

      templateProcessor(args)
    }
  }

  applyOmitValuesPreference(res, options.omitValuesPreference)
}

const postProcessMinimal = (req, result) => {
  const { target } = req

  if (!target || !result) return

  const etagElement = Object.values(target.elements).find(el => el['@odata.etag'])

  if (!etagElement) return

  const etag = etagElement.name

  // normalize result to rows
  result = result.value && Object.keys(result).filter(k => !k.match(/^\W/)).length === 1 ? result.value : result
  const rows = Array.isArray(result) ? result : [result]

  // process each row
  for (const row of rows) {
    if (typeof row !== 'object' || !Object.prototype.hasOwnProperty.call(row, etag)) return

    addEtags(row, etag)
  }
}

module.exports = {
  toODataResult,
  postProcess,
  postProcessMinimal
}
