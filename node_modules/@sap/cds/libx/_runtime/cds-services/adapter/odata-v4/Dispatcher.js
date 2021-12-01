const cds = require('../../../cds')
const LOG = cds.log('odata')

const OData = require('./OData')

const { alias2ref } = require('../../../common/utils/csn')

function _createNewService(name, csn, defaultOptions) {
  const reflectedModel = cds.linked(cds.compile.for.odata(csn))
  const options = Object.assign({}, defaultOptions, { reflectedModel })

  const service = new cds.ApplicationService(name, csn, options)
  service.init()
  if (options.impl) service.impl(options.impl)
  service._isExtended = true

  const edm = cds.compile.to.edm(csn, { service: name })
  alias2ref(service, edm)

  const odataService = new OData(edm, csn, options)
  odataService.addCDSServiceToChannel(service)

  return odataService
}

class Dispatcher {
  /**
   * Constructs an Dispatcher for OData service.
   * New OData services will be created in case of extensibility.
   *
   * @param odata
   */
  constructor(odata) {
    this._odata = odata
  }

  _getService4Tenant(req) {
    const {
      user: { tenant }
    } = req

    // eslint-disable-next-line no-async-promise-executor
    return new Promise(async (resolve, reject) => {
      try {
        const isExtended = await cds.mtx.isExtended(tenant)
        if (!isExtended) return resolve(false)

        const csn = await cds.mtx.getCsn(tenant)

        resolve(_createNewService(this._odata._cdsService.definition.name, csn, this._odata._options))
      } catch (e) {
        reject(e)
      }
    })
  }

  _getService4Toggles(req) {
    const {
      user: { tenant }
    } = req

    // eslint-disable-next-line no-async-promise-executor
    return new Promise(async (resolve, reject) => {
      try {
        if (!this._mps) this._mps = await cds.connect.to('ModelProviderService')

        /*
         * ModelProviderService:
         *   action csn(tenant:TenantID, version:String, toggles: array of String) returns CSN;
         *   action edmx(tenant:TenantID, version:String, toggles: array of String, service:String, locale:Locale, odataVersion:String) returns XML;
         */
        const toggles = (req.features && Object.keys(req.features)) || []
        const csn = await this._mps.csn(tenant, 'dummy', toggles)

        resolve(_createNewService(this._odata._cdsService.definition.name, csn, this._odata._options))
      } catch (e) {
        reject(e)
      }
    })
  }

  /**
   * Dispatch request in case of extensibility to other odata adapters.
   *
   * @param req
   * @param res
   * @private
   * @returns {Promise}
   */
  async dispatch(req, res) {
    // here, req is express' req -> req.tenant not available
    if (cds._mtxEnabled && req.user && req.user.tenant) {
      // enable mtx, if not done yet
      if (!this._extMap) {
        this._extMap = new Map()
        cds.mtx.eventEmitter.on(cds.mtx.events.TENANT_UPDATED, async hash => {
          this._extMap.delete(hash)
        })
      }

      const { alpha_toggles: alphaToggles } = cds.env.features

      // here, req is express' req -> req.tenant not available
      const hash = alphaToggles ? cds.mtx._getHash(req) : req.user.tenant

      // add hash to map, if not done yet
      if (!this._extMap.has(hash)) {
        this._extMap.set(hash, alphaToggles ? this._getService4Toggles(req) : this._getService4Tenant(req))
      }

      // await extended service promise
      let service
      try {
        service = await this._extMap.get(hash)
      } catch (e) {
        if (LOG._error) {
          e.message = 'Unable to get service from service map due to error: ' + e.message
          LOG.error(e)
        }
        // REVISIT: use i18n
        return res.status(500).send({ error: { code: 'null', message: 'Internal Server Error' } })
      }

      // invoke extended service, if exists
      if (service) return service.process(req, res)
    }

    this._odata.process(req, res)
  }

  /**
   * Return service middleware, which can be used by node server, express, connect, ...
   *
   * @returns {Function}
   */
  getService() {
    return (req, res) => {
      this.dispatch(req, res)
    }
  }
}

module.exports = Dispatcher
