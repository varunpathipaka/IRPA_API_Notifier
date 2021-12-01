const cds = require('../../../cds')

const getTemplate = require('../../../common/utils/template')
const templateProcessor = require('../../../common/utils/templateProcessor')

const {
  getRootEntity,
  getPick,
  createLogEntry,
  addObjectID,
  addDataSubject,
  addDataSubjectForDetailsEntity,
  resolveDataSubjectPromises
} = require('./utils')

let als

const _processorFnAccess = (accessLogs, model, req) => {
  return ({ row, key, element, plain }) => {
    const entity = getRootEntity(element)

    // create or augment log entry
    const accessLog = createLogEntry(accessLogs, entity, row)

    // process categories
    for (const category of plain.categories) {
      if (category === 'ObjectID') {
        addObjectID(accessLog, row, key)
      } else if (category === 'DataSubjectID') {
        addDataSubject(accessLog, row, key, entity)
      } else if (category === 'IsPotentiallySensitive') {
        // add attribute
        if (!accessLog.attributes.find(ele => ele.name === key)) accessLog.attributes.push({ name: key })
        // REVISIT: attribute vs. attachment?
      }
    }

    // add promise to determine data subject if a DataSubjectDetails entity
    if (
      element.parent['@PersonalData.EntitySemantics'] === 'DataSubjectDetails' &&
      accessLog.dataSubject.id.length === 0 // > id still an array -> promise not yet set
    ) {
      addDataSubjectForDetailsEntity(row, accessLog, req, entity, model, element)
    }
  }
}

const _getDataAccessLogs = (data, req, tx) => {
  const template = getTemplate(
    'personal_read',
    Object.assign({ name: req.target._service.name, model: tx.model }),
    req.target,
    { pick: getPick('READ') }
  )

  const accessLogs = {}

  const processFn = _processorFnAccess(accessLogs, tx.model, req)
  const data_ = Array.isArray(data) ? data : [data]
  data_.forEach(row => {
    templateProcessor({ processFn, row, template })
  })

  return accessLogs
}

const auditAccessHandler = async function (data, req) {
  als = als || (await cds.connect.to('audit-log'))
  if (!als.ready) return

  const accessLogs = _getDataAccessLogs(data, req, this)
  const accesses = Object.keys(accessLogs).map(k => accessLogs[k])

  await resolveDataSubjectPromises(accesses)

  if (accesses.length) await als.emit('dataAccessLog', { accesses })
}

module.exports = {
  auditAccessHandler
}
