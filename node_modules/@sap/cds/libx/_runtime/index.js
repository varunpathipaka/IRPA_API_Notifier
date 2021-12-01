module.exports = {
  to: {
    /** @type {import('./cds-services/adapter/odata-v4/to')} */
    get odata_v4() {
      return this._odatav4 || (this._odatav4 = require('./cds-services/adapter/odata-v4/to'))
    },

    get rest() {
      if (!this._rest) {
        if (global.cds.env.features.rest_new_adapter) this._rest = require('../rest')
        else this._rest = require('./cds-services/adapter/rest/to')
      }
      return this._rest
    }
  },

  /** @type {import('./common/auth')} */
  get auth() {
    return this._auth || (this._auth = require('./common/auth'))
  },

  // REVISIT: remove once not needed anymore
  get performanceMeasurement() {
    return this._perf || (this._perf = require('./cds-services/adapter/perf/performanceMeasurement'))
  }
}
