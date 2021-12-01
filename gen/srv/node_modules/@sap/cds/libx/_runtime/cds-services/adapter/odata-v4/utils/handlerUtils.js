const cds = require('../../../../cds')
const { SELECT } = cds.ql
const { isCustomOperation } = require('./request')
const expandToCQN = require('../odata-to-cqn/expandToCQN')
const QueryOptions = require('../okra/odata-server').QueryOptions
const { COMPLEX_PROPERTY, PRIMITIVE_PROPERTY } = require('../okra/odata-server').uri.UriResource.ResourceKind

const _selectForFunction = (selectColumns, result, opReturnType) => {
  if (!Array.isArray(result)) result = [result]

  const keys = opReturnType.keys

  for (const row of result) {
    for (const entry in row) {
      if (keys[entry] || entry.match(/^\*/)) continue

      if (!selectColumns.includes(entry)) {
        delete row[entry]
      }
    }
  }
}

const { ensureDraftsSuffix, isDraftActivateAction } = require('../../../../fiori/utils/handler')

const _expand = (model, uriInfo, options) => {
  const expand = uriInfo.getQueryOption(QueryOptions.EXPAND)

  if (!expand || expand.length === 0) {
    return []
  }

  return expandToCQN(model, expand, uriInfo.getFinalEdmType(), options)
}

const _expandForFunction = async (uriInfo, result, req, srv, opReturnType) => {
  const results = Array.isArray(result) ? result : [result]

  const opReturnTypeName = typeof opReturnType === 'string' ? opReturnType : opReturnType.name
  const isDraft = srv.model.definitions[opReturnTypeName] && srv.model.definitions[opReturnTypeName]._isDraftEnabled

  const isDraftActivate = isDraftActivateAction(req)

  // REVISIT: what happens here exactly?
  for (const row of results) {
    const selectQuery = SELECT.from(isDraft && !isDraftActivate ? ensureDraftsSuffix(opReturnType.name) : opReturnType)

    for (const key in opReturnType.keys) {
      if ((!isDraft || isDraftActivate) && key === 'IsActiveEntity') {
        continue
      }
      selectQuery.where(key, '=', row[key])
    }

    const expandCqn = _expand(srv.model, uriInfo, { rewriteAsterisks: true })
    selectQuery.columns(expandCqn)

    const res = await cds.tx(req).run(selectQuery)
    if (res) Object.assign(row, res[0])
  }
}

const _cleanupResult = (result, opReturnType) => {
  if (!Array.isArray(result)) result = [result]

  for (const row of result) {
    for (const element in opReturnType.elements) {
      if (opReturnType.elements[element].is2many) {
        delete row[element]
      }
    }
  }
}

const getActionOrFunctionReturnType = (pathSegments, definitions) => {
  if (!isCustomOperation(pathSegments, true)) return

  const actionOrFunction =
    pathSegments[pathSegments.length - 1].getFunction() || pathSegments[pathSegments.length - 1].getAction()

  if (actionOrFunction) {
    const returnType = actionOrFunction.getReturnType()
    if (returnType) {
      return definitions[returnType.getType().getFullQualifiedName().toString()]
    }
  }
}

const actionAndFunctionQueries = async (req, odataReq, result, srv, opReturnType) => {
  _cleanupResult(result, opReturnType)

  if (odataReq.getQueryOptions().$select) {
    _selectForFunction(odataReq.getQueryOptions().$select.split(','), result, opReturnType)
  }

  // REVISIT: we need to read directly from db for this, which might not be there!
  if (odataReq.getQueryOptions().$expand && cds.db) {
    await _expandForFunction(odataReq.getUriInfo(), result, req, srv, opReturnType)
  }
}

const resolveStructuredName = (pathSegments, index, nameArr = []) => {
  if (pathSegments[index].getKind() === COMPLEX_PROPERTY) {
    const prop = pathSegments[index].getProperty()
    nameArr.unshift(prop.getName())
    return resolveStructuredName(pathSegments, --index, nameArr)
  } else if (
    pathSegments[index].getKind() === PRIMITIVE_PROPERTY &&
    pathSegments[index - 1].getKind() === COMPLEX_PROPERTY
  ) {
    return resolveStructuredName(pathSegments, --index, nameArr)
  }

  return nameArr
}

const isReturnMinimal = req => {
  if (!req.headers.prefer || !req.headers.prefer.includes('return=')) {
    return cds.env.odata.prefer && cds.env.odata.prefer.return === 'minimal'
  }

  return req.headers.prefer.includes('return=minimal')
}

module.exports = {
  _expand,
  resolveStructuredName,
  actionAndFunctionQueries,
  getActionOrFunctionReturnType,
  isReturnMinimal
}
