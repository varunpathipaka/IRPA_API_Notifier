const cds = require('../../../../cds')
const ODataRequest = require('../ODataRequest')

const {
  Components: { DATA_DELETE_HANDLER }
} = require('../okra/odata-server')

const { getSapMessages } = require('../../../../common/error/frontend')
const { validateResourcePath } = require('../utils/request')

/**
 * The handler that will be registered with odata-v4.
 *
 * @param {import('../../../services/Service')} service
 * @returns {function}
 */
const del = service => {
  return async (odataReq, odataRes, next) => {
    let req
    try {
      validateResourcePath(odataReq, service)
      req = new ODataRequest(DATA_DELETE_HANDLER, service, odataReq, odataRes)
    } catch (e) {
      return next(e)
    }

    const changeset = odataReq.getAtomicityGroupId()
    const tx = changeset ? odataReq.getBatchApplicationData().txs[changeset] : service.tx(req)
    cds.context = tx

    let err, commit
    try {
      await tx.dispatch(req)
      const result = null

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
      else next(null, null)
    }
  }
}

module.exports = del
