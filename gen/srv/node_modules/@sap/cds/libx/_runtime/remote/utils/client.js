const cds = require('../../cds')
const LOG = cds.log('remote')

const cdsLocale = require('../../../../lib/req/locale')

const { convertV2ResponseData } = require('./dataConversion')

let _cloudSdkCore

const PPPD = {
  POST: 1,
  PUT: 1,
  PATCH: 1,
  DELETE: 1
}

const KINDS_SUPPORTING_BATCH = { odata: 1, 'odata-v2': 1, 'odata-v4': 1 }

const _executeHttpRequest = async ({ requestConfig, destination, destinationOptions, jwt }) => {
  const { getDestination, executeHttpRequest } = cloudSdkCore()

  const destinationName = typeof destination === 'string' && destination
  if (destinationName) {
    destination = await getDestination(destinationName, resolveDestinationOptions(destinationOptions, jwt))
  } else if (destination.forwardAuthToken) {
    destination = {
      ...destination,
      headers: destination.headers ? { ...destination.headers } : {},
      authentication: 'NoAuthentication'
    }
    delete destination.forwardAuthToken
    if (jwt) {
      destination.headers.authorization = `Bearer ${jwt}`
    } else {
      LOG._warn && LOG.warn('Missing JWT token for forwardAuthToken')
    }
  }

  let requestOptions
  if (PPPD[requestConfig.method] && cds.env.features.fetch_csrf) {
    requestOptions = { fetchCsrfToken: true }
  }

  return executeHttpRequest(destination, requestConfig, requestOptions)
}

const cloudSdkCore = function () {
  return _cloudSdkCore || (_cloudSdkCore = require('@sap-cloud-sdk/core'))
}

const getDestination = (name, credentials) => {
  if (!credentials.url) {
    throw new Error(`"url" or "destination" property must be configured in "credentials" of "${name}".`)
  }

  return { name, ...credentials }
}

/**
 * @param {import('./client-types').DestinationOptions} [options]
 * @param {string} [jwt]
 * @returns {import('@sap-cloud-sdk/core').DestinationOptions}
 */
const resolveDestinationOptions = function (options, jwt) {
  if (!options && !jwt) return undefined

  const resolvedOptions = Object.assign({}, options || {})
  resolvedOptions.userJwt = jwt

  if (options && options.selectionStrategy) {
    resolvedOptions.selectionStrategy = cloudSdkCore().DestinationSelectionStrategies[options.selectionStrategy]
    if (!resolvedOptions.selectionStrategy)
      throw new Error(`Unsupported destination selection strategy "${options.selectionStrategy}".`)
  }

  return resolvedOptions
}

const getKind = options => {
  const kind = (options.credentials && options.credentials.kind) || options.kind
  if (typeof kind === 'object') {
    const k = Object.keys(kind).find(
      key => key === 'odata' || key === 'odata-v4' || key === 'odata-v2' || key === 'rest'
    )
    // odata-v4 is equivalent of odata
    return k === 'odata-v4' ? 'odata' : k
  }

  return kind
}

/**
 * Rest Client
 */
/**
 * Normalizes server path.
 *
 * Adds / in the beginning of the path if not exists.
 * Removes / in the end of the path if exists.
 *
 * @param {*} path - to be normalized
 */
const formatPath = path => {
  let formattedPath = path
  if (!path.startsWith('/')) {
    formattedPath = `/${formattedPath}`
  }

  if (path.endsWith('/')) {
    formattedPath = formattedPath.substring(0, formattedPath.length - 1)
  }

  return formattedPath
}

function _defineProperty(obj, property, value) {
  const props = {}
  if (Array.isArray(obj)) {
    const _map = obj.map
    const map = (..._) => _defineProperty(_map.call(obj, ..._), property, value)
    props.map = { value: map, enumerable: false, configurable: true, writable: true }
  }
  props[property] = { value: value, enumerable: false, configurable: true, writable: true }
  for (const prop in props) {
    Object.defineProperty(obj, prop, props[prop])
  }
  return obj
}

function _normalizeMetadata(prefix, data, results) {
  const target = results || data
  if (typeof target !== 'object') return target
  const metadataKeys = Object.keys(data).filter(k => prefix.test(k))
  for (const k of metadataKeys) {
    const $ = k.replace(prefix, '$')
    _defineProperty(target, $, data[k])
    delete target[k]
  }
  if (Array.isArray(target)) {
    return target.map(row => _normalizeMetadata(prefix, row))
  }
  // check properties for all and prop.results for odata v2
  for (const [key, value] of Object.entries(target)) {
    if (value && typeof value === 'object') {
      const nestedResults = (Array.isArray(value.results) && value.results) || value
      target[key] = _normalizeMetadata(prefix, value, nestedResults)
    }
  }
  return target
}

const _purgeODataV2 = (data, target, reqHeaders) => {
  if (typeof data !== 'object' || !data.d) return data

  data = data.d
  const contentType = reqHeaders['content-type']
  const ieee754Compatible = contentType && contentType.includes('IEEE754Compatible=true')
  const purgedResponse = data.results || data
  const convertedResponse = convertV2ResponseData(purgedResponse, target, ieee754Compatible)
  return _normalizeMetadata(/^__/, data, convertedResponse)
}

const _purgeODataV4 = data => {
  if (typeof data !== 'object') return data

  const purgedResponse = data.value || data
  return _normalizeMetadata(/^@odata\./, data, purgedResponse)
}

const TYPES_TO_REMOVE = { function: 1, object: 1 }
const PROPS_TO_IGNORE = { cause: 1, name: 1 }

const _getSanitizedError = (e, reqOptions, suppressRemoteResponseBody) => {
  e.request = {
    method: reqOptions.method,
    url: e.config ? e.config.baseURL + e.config.url : reqOptions.url,
    headers: e.config ? e.config.headers : reqOptions.headers
  }

  if (e.response) {
    const response = {
      status: e.response.status,
      statusText: e.response.statusText,
      headers: e.response.headers
    }
    if (e.response.data && !suppressRemoteResponseBody) {
      response.body = e.response.data
    }
    e.response = response
  }

  const correlationId =
    (cds.context && cds.context.id) || (reqOptions.headers && reqOptions.headers['x-correlation-id'])
  if (correlationId) e.correlationId = correlationId

  // sanitize authorization
  if (e.request.headers && e.request.headers.authorization)
    e.request.headers.authorization = e.request.headers.authorization.split(' ')[0] + ' ...'

  // delete functions and complex objects in config
  for (const k in e) if (typeof e[k] === 'function') delete e[k]
  if (e.config) for (const k in e.config) if (TYPES_TO_REMOVE[typeof e.config[k]]) delete e.config[k]

  // REVISIT: ErrorWithCause log waaay to much -> copy what we want to new object (as delete e.cause doesn't work)
  if (e.cause) {
    let msg = ''
    let cur = e.cause
    while (cur) {
      msg += ' Caused by: ' + cur.message
      cur = cur.cause
    }
    const _e = { message: e.message + msg }
    for (const k of [...Object.keys(e).filter(k => !PROPS_TO_IGNORE[k])]) _e[k] = e[k]
    e = _e
  }

  return e
}

// eslint-disable-next-line complexity
const run = async (
  requestConfig,
  { destination, jwt, kind, resolvedTarget, suppressRemoteResponseBody, destinationOptions }
) => {
  let response
  try {
    response = await _executeHttpRequest({ requestConfig, destination, destinationOptions, jwt })
  } catch (e) {
    // > axios received status >= 400 -> gateway error
    e.message = e.message ? 'Error during request to remote service: ' + e.message : 'Request to remote service failed.'

    const sanitizedError = _getSanitizedError(e, requestConfig, suppressRemoteResponseBody)

    LOG._warn && LOG.warn(sanitizedError)

    throw Object.assign(new Error(e.message), { statusCode: 502, innererror: sanitizedError })
  }

  // text/html indicates a redirect -> reject
  if (
    response.headers &&
    response.headers['content-type'] &&
    response.headers['content-type'].includes('text/html') &&
    !(
      requestConfig.headers.accept.includes('text/html') ||
      requestConfig.headers.accept.includes('text/*') ||
      requestConfig.headers.accept.includes('*/*')
    )
  ) {
    const e = new Error("Received content-type 'text/html' which is not part of accepted content types")
    e.response = response

    const sanitizedError = _getSanitizedError(e, requestConfig, suppressRemoteResponseBody)

    LOG._warn && LOG.warn(sanitizedError)

    throw Object.assign(new Error(`Error during request to remote service: ${e.message}`), {
      statusCode: 502,
      innererror: sanitizedError
    })
  }

  // get result of $batch
  // does only support read requests as of now
  if (requestConfig._autoBatch) {
    // response data splitted by empty lines
    // 1. entry contains batch id and batch headers
    // 2. entry contains request status code and request headers
    // 3. entry contains data or error
    const responseDataSplitted = response.data.split('\r\n\r\n')
    // remove closing batch id
    const [content] = responseDataSplitted[2].split('\r\n')
    const contentJSON = JSON.parse(content)

    if (responseDataSplitted[1].startsWith('HTTP/1.1 2')) {
      response.data = contentJSON
    }
    if (responseDataSplitted[1].startsWith('HTTP/1.1 4') || responseDataSplitted[1].startsWith('HTTP/1.1 5')) {
      contentJSON.message = contentJSON.message
        ? 'Error during request to remote service: ' + contentJSON.message
        : 'Request to remote service failed.'
      const sanitizedError = _getSanitizedError(contentJSON, requestConfig)
      LOG._warn && LOG.warn(sanitizedError)
      throw Object.assign(new Error(contentJSON.message), { statusCode: 502, innererror: sanitizedError })
    }
  }

  if (kind === 'odata-v4') return _purgeODataV4(response.data)
  if (kind === 'odata-v2') return _purgeODataV2(response.data, resolvedTarget, requestConfig.headers)
  if (kind === 'odata') {
    if (typeof response.data !== 'object') return response.data
    // try to guess if we need to purge v2 or v4
    if (response.data.d) {
      return _purgeODataV2(response.data, resolvedTarget, requestConfig.headers)
    }
    return _purgeODataV4(response.data)
  }
  return response.data
}

const getJwt = req => {
  const headers = req && req.context && req.context.headers
  if (headers && headers.authorization) {
    const token = headers.authorization.match(/^bearer (.+)/i)
    if (token) {
      return token[1]
    }
  }
  return null
}

const _cqnToReqOptions = (query, kind, model) => {
  const queryObject = cds.odata.urlify(query, { kind, model })
  return {
    method: queryObject.method,
    url: encodeURI(
      queryObject.path
        // ugly workaround for Okra not allowing spaces in ( x eq 1 )
        .replace(/\( /g, '(')
        .replace(/ \)/g, ')')
    ),
    data: queryObject.body
  }
}

const _stringToReqOptions = (query, data) => {
  const cleanQuery = query.trim()
  const blankIndex = cleanQuery.substring(0, 8).indexOf(' ')
  const reqOptions = {
    method: cleanQuery.substring(0, blankIndex).toUpperCase(),
    url: encodeURI(formatPath(cleanQuery.substring(blankIndex, cleanQuery.length).trim()))
  }
  if (data && reqOptions.method !== 'GET' && reqOptions.method !== 'HEAD') reqOptions.data = data
  return reqOptions
}

const _pathToReqOptions = (method, path, data) => {
  let url = path
  if (!url.startsWith('/')) {
    // extract entity name and instance identifier (either in "()" or after "/") from fully qualified path
    const parts = path.match(/([\w.]*)([\W.]*)(.*)/)
    if (!parts) url = '/' + path.match(/\w*$/)[0]
    else url = '/' + parts[1].match(/\w*$/)[0] + parts[2] + parts[3]

    // normalize in case parts[2] already starts with /
    url = url.replace(/^\/\//, '/')
  }
  const reqOptions = { method, url }
  if (data && reqOptions.method !== 'GET' && reqOptions.method !== 'HEAD') reqOptions.data = data
  return reqOptions
}

const _hasHeader = (headers, header) =>
  Object.keys(headers || [])
    .map(k => k.toLowerCase())
    .includes(header)

const getReqOptions = (req, query, service) => {
  const reqOptions =
    typeof query === 'object'
      ? _cqnToReqOptions(query, service.kind, service.model)
      : typeof query === 'string'
      ? _stringToReqOptions(query, req.data)
      : _pathToReqOptions(req.method, req.path, req.data)

  reqOptions.headers = { accept: 'application/json,text/plain' }
  reqOptions.timeout = service.requestTimeout

  if (!_hasHeader(req.headers, 'accept-language')) {
    // Forward the locale properties from the original request (including region variants or weight factors),
    // if not given, it's taken from the user's locale (normalized and simplified)
    const locale =
      (req.context && req.context._ && req.context._.req && cdsLocale.from_req(req.context._.req)) ||
      (req.user && req.user.locale)
    if (locale) reqOptions.headers['accept-language'] = locale
  }

  if (reqOptions.data && reqOptions.method !== 'GET' && reqOptions.method !== 'HEAD') {
    reqOptions.headers['content-type'] = 'application/json'
    reqOptions.headers['content-length'] = Buffer.byteLength(JSON.stringify(reqOptions.data))
  }
  reqOptions.url = formatPath(reqOptions.url)

  // batch envelope if needed
  if (
    KINDS_SUPPORTING_BATCH[service.kind] &&
    reqOptions.method === 'GET' &&
    reqOptions.url.length > ((cds.env.remote && cds.env.remote.max_get_url_length) || 1028)
  ) {
    reqOptions._autoBatch = true
    reqOptions.data = [
      '--batch1',
      'Content-Type: application/http',
      'Content-Transfer-Encoding: binary',
      '',
      `${reqOptions.method} ${reqOptions.url.replace(/^\//, '')} HTTP/1.1`,
      ...Object.keys(reqOptions.headers).map(k => `${k}: ${reqOptions.headers[k]}`),
      '',
      '',
      '--batch1--',
      ''
    ].join('\r\n')
    reqOptions.method = 'POST'
    reqOptions.headers.accept = 'multipart/mixed'
    reqOptions.headers['content-type'] = 'multipart/mixed; boundary=batch1'
    reqOptions.url = '/$batch'
  }

  if (service.path) reqOptions.url = `${encodeURI(service.path)}${reqOptions.url}`

  return reqOptions
}

const getAdditionalOptions = (req, destination, kind, resolvedTarget, destinationOptions) => {
  const jwt = getJwt(req)
  const additionalOptions = { destination, kind, resolvedTarget, destinationOptions }
  if (jwt) additionalOptions.jwt = jwt
  return additionalOptions
}

module.exports = {
  getKind,
  run,
  getReqOptions,
  getDestination,
  getAdditionalOptions
}
