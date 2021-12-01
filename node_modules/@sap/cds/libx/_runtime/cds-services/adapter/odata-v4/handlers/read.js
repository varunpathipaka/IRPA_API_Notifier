const cds = require('../../../../cds')
const { SELECT } = cds.ql
const ODataRequest = require('../ODataRequest')
const { rewriteExpandAsterisk } = require('../../../../common/utils/rewriteAsterisks')

const {
  QueryOptions,
  Components: { DATA_READ_HANDLER },
  uri: {
    UriResource: {
      ResourceKind: { BOUND_FUNCTION, COUNT, FUNCTION_IMPORT, NAVIGATION_TO_ONE, VALUE, SINGLETON, PRIMITIVE_PROPERTY }
    }
  }
} = require('../okra/odata-server')

const getError = require('../../../../common/error')
const { getSapMessages } = require('../../../../common/error/frontend')
const { isCustomOperation, skipToken } = require('../utils/request')
const { actionAndFunctionQueries, getActionOrFunctionReturnType } = require('../utils/handlerUtils')
const { validateResourcePath } = require('../utils/request')
const { toODataResult, postProcess } = require('../utils/result')
const { isStreaming, getStreamProperties } = require('../utils/stream')
const { resolveStructuredName } = require('../utils/handlerUtils')
const { ensureNoDraftsSuffix } = require('../../../../common/utils/draft')

/**
 * Checks whether a bound function or function import is invoked.
 *
 * @param {Array} segments - The uri path segments of the request.
 * @returns {boolean} - True if a function is invoked, else false.
 * @private
 */
const _isFunction = segments => [BOUND_FUNCTION, FUNCTION_IMPORT].includes(segments[segments.length - 1].getKind())

/**
 * Invoke a function.
 *
 * @param {object} tx
 * @param {object} req
 * @param odataReq
 * @returns {Promise}
 * @private
 */
const _invokeFunction = async (tx, req, odataReq) => {
  const result = await tx.dispatch(req)

  const functionReturnType = getActionOrFunctionReturnType(
    odataReq.getUriInfo().getPathSegments(),
    tx.model.definitions
  )

  if (functionReturnType && functionReturnType.kind === 'entity' && odataReq.getQueryOptions()) {
    await actionAndFunctionQueries(req, odataReq, result, tx, functionReturnType)
  }

  return toODataResult(result, req)
}

/**
 * Checks whether a count of entities is requested
 * (not count embedded into collection).
 *
 * @param {Array} segments - The uri path segments of the request.
 * @returns {boolean} - True if a count of entities is requested, else false.
 * @private
 */
const _isCount = segments => segments[segments.length - 1].getKind() === COUNT

/**
 * Get the count by using the general READ CQN and alter it to a COUNT query.
 *
 * @param {object} tx
 * @param {object} readReq
 * @returns {Promise}
 * @private
 */
const _getCount = async (tx, readReq) => {
  // REVISIT: this process appears to be rather clumsy

  // Copy CQN including from and where and changing columns
  const select = SELECT.from(readReq.query.SELECT.from)
  select.SELECT.columns = [{ func: 'count', args: [{ val: '1' }], as: '$count' }]

  if (readReq.query.SELECT.where) select.SELECT.where = readReq.query.SELECT.where
  if (readReq.query.SELECT.search) select.SELECT.search = readReq.query.SELECT.search
  const req = readReq

  // preserve _target
  select._target = req.query._target

  // remove as Object.defineProperty would cause a conflict
  delete req.query

  // Define new CQN
  req.query = select
  // todo check limit
  const result = await tx.dispatch(req)

  const count = (result[0] && (result[0].$count || result[0]._counted_)) || 0

  // Transform into scalar result
  return toODataResult(count)
}

/**
 * Checks whether a collection of entities or a single entity is requested.
 * Returns false in case of a custom operation.
 *
 * @param segments
 * @returns {boolean} - True if a collection of entities is requested, else false.
 * @private
 */
const _isCollection = segments => {
  const lastEntitySegment = Array.from(segments)
    .reverse()
    .find(segment => segment.getProperty() === null)
  const kind = lastEntitySegment.getKind()

  return (
    !isCustomOperation(segments) &&
    kind !== NAVIGATION_TO_ONE &&
    kind !== COUNT &&
    kind !== VALUE &&
    kind !== SINGLETON &&
    lastEntitySegment.getKeyPredicates().length === 0
  )
}

/**
 * Checks whether single entity via navigation-to-one is requested.
 *
 * @param segments
 * @returns {boolean}
 * @private
 */
const _isNavigationToOne = segments => {
  return segments[segments.length - 1].getKind() === NAVIGATION_TO_ONE
}

const _hasRedirectProperty = elements => {
  return Object.values(elements).some(val => {
    return val['@Core.IsURL']
  })
}

const _addMediaType = (key, entry, mediaType) => {
  if (mediaType) {
    if (typeof mediaType === 'object') {
      entry[`${key}@odata.mediaContentType`] = entry[Object.values(mediaType)[0]]
    } else {
      entry[`${key}@odata.mediaContentType`] = mediaType
    }
  }
}

const _transformRedirectProperties = (req, result) => {
  if (!Array.isArray(result) || result.length === 0) {
    return
  }

  // optimization
  if (!_hasRedirectProperty(req.target.elements)) {
    return
  }

  for (const entry of result) {
    for (const key in entry) {
      if (entry[key] !== undefined && req.target.elements[key]['@Core.IsURL']) {
        entry[`${key}@odata.mediaReadLink`] = entry[key]
        _addMediaType(key, entry, req.target.elements[key]['@Core.MediaType'])
        delete entry[key]
      }
    }
  }
}

const _getResult = (nameArr, result) => {
  if (nameArr.length === 0) return result
  return _getResult(nameArr.slice(1), result[nameArr[0]])
}

/**
 * Reads the entire entity or only property of it is alike.
 *
 * In case of an entity, odata-v4 wants the value an object structure, in case of a property as scalar.
 *
 * @param {import('../../../../cds-services/services/Service')} tx
 * @param {import('../../../../cds-services/adapter/odata-v4/ODataRequest')} req
 * @param {Array<import('../okra/odata-commons/uri/UriResource')>} segments
 * @returns {Promise}
 * @private
 */
const _readEntityOrProperty = async (tx, req, segments) => {
  let result = await tx.dispatch(req)

  /*
   * OData spec:
   * - Requesting Individual Entities:
   *     If no entity exists with the key values specified in the request URL, the service responds with 404 Not Found.
   * - Requesting Individual Properties:
   *     If the property is single-valued and has the null value, the service responds with 204 No Content.
   *     If the property is not available, for example due to permissions, the service responds with 404 Not Found.
   * - Requesting Related Entities (to one):
   *     If no entity is related, the service returns 204 No Content.
   */
  if (result == null) {
    if (_isNavigationToOne(segments)) return toODataResult(null)
    throw getError(404)
  }

  if (!Array.isArray(result)) result = [result]

  if (result.length === 0 && _isNavigationToOne(segments)) return toODataResult(null)

  // Reading one entity or a property of it should yield only a result length of one.
  if (result.length !== 1) throw getError(404)

  const index = segments[segments.length - 1].getKind() === VALUE ? 2 : 1
  const propertyElement = segments[segments.length - index].getProperty()

  if (propertyElement === null) {
    _transformRedirectProperties(req, result)
    return toODataResult(result[0])
  }

  const name = resolveStructuredName(segments, segments.length - 2)
  const res = _getResult(name, result[0])

  const odataResult = toODataResult(typeof res === 'object' ? res[propertyElement.getName()] : res)
  if (req.target._etag) odataResult['*@odata.etag'] = res[req.target._etag]

  // property is read via a to one association and last segment is not $value
  if (index !== 2 && segments.length > 2 && segments[segments.length - 2].getKind() === NAVIGATION_TO_ONE) {
    // find keys in result
    const keys = Object.keys(result[0])
      .filter(k => segments[segments.length - index - 1].getEdmType().getOwnKeyPropertyRefs().has(k))
      .reduce((res, curr) => {
        res[curr] = result[0][curr]
        return res
      }, {})

    // prepare key map for Okra
    odataResult.keysForParam = new Map().set(segments[segments.length - index - 1], keys)
  }

  return odataResult
}

/**
 * Read an entity collection without including the count of the total amount of entities.
 *
 * @param {object} tx
 * @param {object} req
 * @param odataReq
 * @returns {Promise}
 * @private
 */
const _readCollection = async (tx, req, odataReq) => {
  const result = (await tx.dispatch(req)) || []
  const odataResult = toODataResult(result, req)

  // REVISIT: better
  if (!odataResult['*@odata.count'] && req.query.SELECT.count) {
    odataResult['*@odata.count'] = 0
  }

  if (!odataResult['*@odata.nextLink']) {
    const limit = req.query && req.query.SELECT.limit && req.query.SELECT.limit.rows && req.query.SELECT.limit.rows.val
    if (limit && limit === result.length && limit !== odataReq.getUriInfo().getQueryOption(QueryOptions.TOP)) {
      odataResult['*@odata.nextLink'] = skipToken(odataReq.getUriInfo()) + limit
    }
  }

  _transformRedirectProperties(req, result)

  return odataResult
}

/**
 * Reading the full entity or only a property of it is alike.
 * In case of an entity, odata-v4 wants the value an object structure,
 * in case of a property as scalar.
 *
 * @param {object} tx
 * @param {object} req
 * @param {Array} segments
 * @returns {Promise}
 * @private
 */
const _readStream = async (tx, req, segments) => {
  req.query._streaming = true

  let result = await tx.dispatch(req)

  // REVISIT: compat, should actually be treated as object
  if (!Array.isArray(result)) result = [result]

  // Reading one entity or a property of it should yield only a result length of one.
  if (result.length === 0 || result[0] === undefined) throw getError(404)

  if (result.length > 1) throw getError(400)

  if (result[0] === null) return null

  const streamObj = result[0]
  const stream = streamObj.value

  if (stream) {
    stream.on('error', () => {
      stream.removeAllListeners('error')
      // stream.destroy() does not end stream in node 10 and 12
      stream.push(null)
    })
  }

  const { contentType, contentDisposition } = await getStreamProperties(segments, tx, req)

  const headers = req._.odataReq.getHeaders()
  if (
    headers &&
    headers.accept &&
    contentType &&
    !headers.accept.includes('*/*') &&
    !headers.accept.includes(contentType) &&
    !headers.accept.includes(contentType.split('/')[0] + '/*')
  ) {
    req.reject(406, `Content type "${contentType}" not listed in accept header "${headers.accept}".`)
  }

  if (contentType) streamObj['*@odata.mediaContentType'] = contentType
  if (contentDisposition) {
    req._.odataRes.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(contentDisposition)}"`)
  }
  return streamObj
}

const _readSingleton = async (tx, req, lastSegment) => {
  let result = await tx.dispatch(req)

  if (result === null && !req.target['@odata.singleton.nullable']) throw getError(404)

  if (lastSegment.getKind() === PRIMITIVE_PROPERTY) {
    result = result[lastSegment.getProperty().getName()]
  }

  return toODataResult(result, req)
}

/**
 * Depending on the read request segments, create one ore more reading service request.
 *
 * @param {object} tx
 * @param {object} req
 * @param odataReq
 * @returns {Promise}
 * @private
 */
const _readAndTransform = (tx, req, odataReq) => {
  const segments = odataReq.getUriInfo().getPathSegments()

  if (_isFunction(segments)) {
    return _invokeFunction(tx, req, odataReq)
  }

  // Scalar count is requested
  if (_isCount(segments)) {
    return _getCount(tx, req)
  }

  if (_isCollection(segments)) {
    if (odataReq.getUriInfo().getQueryOption(QueryOptions.COUNT)) {
      req.query.SELECT.count = true
    }

    return _readCollection(tx, req, odataReq)
  }

  // REVISIT: move to afterburner
  if (segments[segments.length - 1]._isStreamByDollarValue) {
    for (const k in req.target.elements) {
      if (req.target.elements[k]['@Core.MediaType']) {
        req.query.SELECT.columns = [{ ref: [k] }]
        break
      }
    }

    return _readStream(tx, req, segments)
  }

  if (isStreaming(segments)) {
    return _readStream(tx, req, segments)
  }

  if (req.target._isSingleton) {
    return _readSingleton(tx, req, segments[segments.length - 1])
  }

  return _readEntityOrProperty(tx, req, segments)
}

const _postProcess = (odataReq, req, odataRes, service, result) => {
  const functionReturnType = getActionOrFunctionReturnType(
    odataReq.getUriInfo().getPathSegments(),
    service.model.definitions
  )
  const _req = Object.assign(req, { target: functionReturnType || req.target })
  postProcess(_req, odataRes, service, result)
}

const _removeKeysForParams = result => {
  let options

  if (result.keysForParam) {
    options = { keys: result.keysForParam }
    delete result.keysForParam
  }

  return options
}

const _getTarget = (ref, target, definitions) => {
  if (cds.env.effective.odata.proxies) {
    const target_ = target.elements[ref[0]]

    if (ref.length === 1) {
      return definitions[ensureNoDraftsSuffix(target_.target)]
    }

    return _getTarget(ref.slice(1), target_, definitions)
  }

  const target_ = target.elements[ref.join('_')]
  return definitions[ensureNoDraftsSuffix(target_.target)]
}

const _getRestrictedExpand = (columns, target, definitions) => {
  if (!columns || !target || columns === '*') return

  const annotation = target['@Capabilities.ExpandRestrictions.NonExpandableProperties']
  const restrictions = annotation && annotation.map(element => element['='])

  rewriteExpandAsterisk(columns, target)

  for (const col of columns) {
    if (col.expand) {
      if (restrictions && restrictions.length !== 0) {
        const ref = col.ref.join('_')
        const ref_ = restrictions.find(element => element.replace(/\./g, '_') === ref)
        if (ref_) return ref_
      }

      const restricted = _getRestrictedExpand(col.expand, _getTarget(col.ref, target, definitions), definitions)
      if (restricted) return restricted
    }
  }
}

/**
 * The handler that will be registered with odata-v4.
 *
 * If an entity collection is read, it calls next with result as an Array with all entities of the collection.
 * If a count of the entities in the collection is requested, it uses number of the entities as a Number value.
 * If an single entity is read, it uses the entity as an object.
 * If a property of a single entity is requested (e.g. /Books(1)/name), it unwraps the property from the result.
 * If the single entity to be read does not exist, calls next with error to return a 404.
 * In all other failure cases it calls next with error to return a 500.
 *
 * @param {import('../../../services/Service')} service
 * @returns {function}
 */
const read = service => {
  return async (odataReq, odataRes, next) => {
    let req
    try {
      validateResourcePath(odataReq, service)
      req = new ODataRequest(DATA_READ_HANDLER, service, odataReq, odataRes)
    } catch (e) {
      return next(e)
    }

    // REVISIT: this should be in common/generic/auth.js with the rest of the access control stuff
    const restricted = _getRestrictedExpand(
      req.query.SELECT && req.query.SELECT.columns,
      req.target,
      service.model.definitions
    )
    if (restricted) {
      return next(getError(400, 'EXPAND_IS_RESTRICTED', [restricted]))
    }

    const changeset = odataReq.getAtomicityGroupId()
    const tx = changeset ? odataReq.getBatchApplicationData().txs[changeset] : service.tx(req)
    cds.context = tx

    let result, err, commit
    let additional = {}
    try {
      // REVISIT: refactor _readAndTransform
      result = await _readAndTransform(tx, req, odataReq)

      if (result === null) {
        result = { value: null }
      } else {
        _postProcess(odataReq, req, odataRes, service, result)
        additional = _removeKeysForParams(result)
      }

      if (changeset) {
        // for passing into commit
        odataReq.getBatchApplicationData().results[changeset].push({ result, req })
      } else {
        commit = true
        await tx.commit(result)
      }
    } catch (e) {
      err = e
      if (!changeset && !commit) {
        // ignore rollback error, which should never happen
        await tx.rollback(e).catch(() => {})
      } else if (changeset) {
        // for passing into rollback
        odataReq.getBatchApplicationData().errors[changeset].push({ error: e, req })
      }
    } finally {
      req.messages && odataRes.setHeader('sap-messages', getSapMessages(req.messages, req._.req))

      if (err) next(err)
      else next(null, result, additional)
    }
  }
}

module.exports = read
