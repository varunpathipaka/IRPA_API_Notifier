const cds = require('../../../cds')

const OData = require('./OData')
const Dispatcher = require('./Dispatcher')

const { alias2ref } = require('../../../common/utils/csn')

const to = service => {
  const edm = cds.compile.to.edm(service.model, { service: service.definition.name })
  alias2ref(service, edm)

  const odata = new OData(edm, service.model, service.options)
  odata.addCDSServiceToChannel(service)

  return new Dispatcher(odata).getService()
}

module.exports = to
