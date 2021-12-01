const cds = require('../../../../cds')

const { toODataResult } = require('../utils/result')

let _mps

const _get4Tenant = async (tenant, locale, service) => {
  if (cds._mtxEnabled && service._isExtended) {
    const edmx = await cds.mtx.getEdmx(tenant, service.name, locale)
    return edmx
  }
}

const _get4Toggles = async (tenant, locale, service, req) => {
  if (!_mps) _mps = await cds.connect.to('ModelProviderService')

  /*
   * ModelProviderService:
   *   action csn(tenant:TenantID, version:String, toggles: array of String) returns CSN;
   *   action edmx(tenant:TenantID, version:String, toggles: array of String, service:String, locale:Locale, odataVersion:String) returns XML;
   */
  const toggles = (req.features && Object.keys(req.features)) || []
  const edmx = await _mps.edmx(tenant, 'dummy', toggles, service.name, locale, 'v4')

  return edmx
}

/**
 * Provide localized metadata handler.
 *
 * @param {object} service
 * @returns {Function}
 */
const metadata = service => {
  return async (odataReq, odataRes, next) => {
    try {
      const req = odataReq.getIncomingRequest()

      const tenant = req.tenant
      // REVISIT: can we take locale from user, or is there some odata special wrt metadata?
      const locale = odataRes.getContract().getLocale()

      let edmx

      if (tenant) {
        const { alpha_toggles: alphaToggles } = cds.env.features
        edmx = alphaToggles
          ? await _get4Toggles(tenant, locale, service, req)
          : await _get4Tenant(tenant, locale, service)
      }

      if (!edmx) {
        edmx = cds.localize(
          service.model,
          locale,
          // REVISIT: we could cache this in a weak map
          cds.compile.to.edmx(service.model, { service: service.definition.name })
        )
      }

      return next(null, toODataResult(edmx))
    } catch (e) {
      return next(e)
    }
  }
}

module.exports = metadata
