const { validateReturnType } = require('../../_runtime/cds-services/adapter/rest/utils/validation-checks')

const RestRequest = require('../RestRequest')

module.exports = async (_req, _res, next) => {
  const { _srv: srv, _query: query, _operation: operation, _data: data } = _req

  let result

  // unfortunately, express doesn't catch async errors -> try catch needed
  try {
    const req = query
      ? new RestRequest({ query, event: operation.name, data })
      : new RestRequest({ event: operation.name.replace(`${srv.name}.`, ''), data })
    result = await srv.dispatch(req)
  } catch (e) {
    return next(e)
  }

  if (!operation.returns) {
    _req._result = { status: 204 }
  } else {
    // unfortunately, express doesn't catch async errors -> try catch needed
    try {
      // REVISIT: do not use from old rest adapter
      // REVISIT: new impl should return instead of throwing to avoid try catch
      validateReturnType(operation, result)
    } catch (e) {
      return next(e)
    }

    // REVISIT: still needed?
    if (!operation.returns.items && Array.isArray(result)) result = result[0]

    _req._result = { result }
  }

  next()
}
