const cds = require('../../../../cds')

const ODataRequest = require('../ODataRequest')

const {
  Components: { ACTION_EXECUTE_HANDLER }
} = require('../okra/odata-server')

const { getSapMessages } = require('../../../../common/error/frontend')
const { actionAndFunctionQueries, getActionOrFunctionReturnType } = require('../utils/handlerUtils')
const { validateResourcePath } = require('../utils/request')
const readAfterWrite = require('../utils/readAfterWrite')
const { setStatusCodeAndHeader, getKeyProperty } = require('../../../../fiori/utils/handler')
const { toODataResult, postProcess } = require('../utils/result')
const { mergeJson } = require('../../../services/utils/compareJson')

/*
 * Get the returns object for the (un)bound action from CSN.
 */
const _getTypeReturns = (definitions, req, service) => {
  if (req.event === 'draftPrepare' || req.event === 'EDIT' || req.event === 'draftActivate') {
    return 'Other'
  }

  if (req.target && req._.odataReq.getUriInfo().getLastSegment().getKind() === 'BOUND.ACTION') {
    return definitions[req.target.name].actions[req.event].returns
  }

  // Also support correct req.event without service prefix
  return (definitions[req.event] || definitions[`${service.name}.${req.event}`]).returns
}

/*
 * Check if the return is an array or any other.
 */
const _getActionReturnType = (service, req) => {
  const returns = _getTypeReturns(service.model.definitions, req, service)

  return returns && returns.items ? 'Array' : 'Other'
}

const _postProcessDraftActivate = async (req, result, service) => {
  // update req.data (keys needed in readAfterWrite)
  req.data = result
  const dataInDb = await readAfterWrite(req, service)
  if (dataInDb.length) result = mergeJson(dataInDb[0], result, req.target)

  // add static draft columns
  result.IsActiveEntity = true
  result.HasActiveEntity = false
  result.HasDraftEntity = false

  // REVISIT: should not be necessary
  // remove composition and association stubs
  if (!cds.env.effective.odata.structs) {
    for (const k in req.target.elements) if (req.target.elements[k].isAssociation) delete result[k]
  }

  return result
}

const _postProcess = async (req, odataReq, odataRes, tx, result) => {
  postProcess(req, odataRes, tx, result)

  // REVISIT: harmonize getactionreturntype functions
  const actionReturnType = getActionOrFunctionReturnType(odataReq.getUriInfo().getPathSegments(), tx.model.definitions)
  if (actionReturnType && actionReturnType.kind === 'entity' && odataReq.getQueryOptions()) {
    await actionAndFunctionQueries(req, odataReq, result, tx, actionReturnType)
  }
}

/**
 * The handler that will be registered with odata-v4.
 *
 * @param {import('../../../services/Service')} service
 * @returns {function}
 */
const action = service => {
  return async (odataReq, odataRes, next) => {
    let req
    try {
      validateResourcePath(odataReq, service)
      req = new ODataRequest(ACTION_EXECUTE_HANDLER, service, odataReq, odataRes)
    } catch (e) {
      return next(e)
    }

    const changeset = odataReq.getAtomicityGroupId()
    const tx = changeset ? odataReq.getBatchApplicationData().txs[changeset] : service.tx(req)
    cds.context = tx

    let result, err, commit
    try {
      result = await tx.dispatch(req)

      // post processing for draftActivate
      if (req.event === 'draftActivate') {
        result = await _postProcessDraftActivate(req, result, service)

        const k = getKeyProperty(req.target.keys)
        setStatusCodeAndHeader(odataRes, { [k]: result[k] }, req.target.name.replace(`${service.name}.`, ''), true)
      }

      await _postProcess(req, odataReq, odataRes, tx, result)

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
      else next(null, toODataResult(result, _getActionReturnType(service, req)))
    }
  }
}

module.exports = action
