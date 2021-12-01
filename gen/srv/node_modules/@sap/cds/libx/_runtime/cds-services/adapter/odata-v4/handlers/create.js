const cds = require('../../../../cds')
const ODataRequest = require('../ODataRequest')

const {
  Components: { DATA_CREATE_HANDLER }
} = require('../okra/odata-server')

const { getSapMessages } = require('../../../../common/error/frontend')
const { validateResourcePath } = require('../utils/request')
const { isReturnMinimal } = require('../utils/handlerUtils')
const readAfterWrite = require('../utils/readAfterWrite')
const { toODataResult, postProcess, postProcessMinimal } = require('../utils/result')
const { mergeJson } = require('../../../services/utils/compareJson')

/**
 * The handler that will be registered with odata-v4.
 *
 * @param {import('../../../services/Service')} service
 * @returns {function}
 */
const create = service => {
  return async (odataReq, odataRes, next) => {
    let req
    try {
      validateResourcePath(odataReq, service)
      req = new ODataRequest(DATA_CREATE_HANDLER, service, odataReq, odataRes)
    } catch (e) {
      return next(e)
    }

    const changeset = odataReq.getAtomicityGroupId()
    const tx = changeset ? odataReq.getBatchApplicationData().txs[changeset] : service.tx(req)
    cds.context = tx

    let result, err, commit
    try {
      result = await tx.dispatch(req)

      if (!isReturnMinimal(req)) {
        // REVISIT: find better solution
        if (req._.readAfterWrite) {
          const dataInDb = await readAfterWrite(req, service)
          if (dataInDb.length) result = mergeJson(dataInDb[0], result, req.target)
        }

        postProcess(req, odataRes, service, result)
      } else {
        postProcessMinimal(req, result)
      }

      if (changeset) {
        // for passing into commit
        odataReq.getBatchApplicationData().results[changeset].push({ result, req })
      } else {
        commit = true
        await tx.commit(result)
      }

      if (isReturnMinimal(req)) {
        odataRes.setStatusCode(204)
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
      else next(null, toODataResult(result))
    }
  }
}

module.exports = create
