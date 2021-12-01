const cds = require('../../../cds')

const { getDataSubject } = require('../../../common/utils/csn')

const WRITE = { CREATE: 1, UPDATE: 1, DELETE: 1 }
const ASPECTS = { CREATE: '_auditCreate', READ: '_auditRead', UPDATE: '_auditUpdate', DELETE: '_auditDelete' }

const getMapKeyForCurrentRequest = req => {
  // running in srv or db layer? -> srv's req.query used as key of diff and logs maps at req.context
  return req._tx.constructor.name.match(/Database$/i) ? req._.query : req.query
}

const getRootEntity = element => {
  let entity = element.parent
  while (entity.kind !== 'entity') entity = entity.parent
  return entity
}

const getPick = event => {
  return (element, target) => {
    if (!target[ASPECTS[event]]) return
    const categories = []
    if (!element.isAssociation && element.key) categories.push('ObjectID')
    if (
      element['@PersonalData.FieldSemantics'] === 'DataSubjectID' &&
      // item element of arrayed element has no parent, but
      // at the moment annotation on item level is not supported
      element.parent &&
      element.parent['@PersonalData.EntitySemantics'] === 'DataSubject'
    )
      categories.push('DataSubjectID')
    if (event in WRITE && element['@PersonalData.IsPotentiallyPersonal']) categories.push('IsPotentiallyPersonal')
    if (element['@PersonalData.IsPotentiallySensitive']) categories.push('IsPotentiallySensitive')
    if (categories.length) return { categories }
  }
}

const _getHash = (entity, row) => {
  return `${entity.name}(${Object.keys(entity.keys)
    .map(k => `${k}=${row[k]}`)
    .join(',')})`
}

const createLogEntry = (logs, entity, row) => {
  const hash = _getHash(entity, row)
  let log = logs[hash]
  if (!log) {
    logs[hash] = {
      dataObject: { type: entity.name, id: [] },
      dataSubject: { id: [], role: entity['@PersonalData.DataSubjectRole'] },
      attributes: [],
      attachments: []
    }
    log = logs[hash]
  }
  return log
}

const addObjectID = (log, row, key) => {
  if (!log.dataObject.id.find(ele => ele.keyName === key))
    log.dataObject.id.push({ keyName: key, value: String(row[key]) })
}

const addDataSubject = (log, row, key, entity) => {
  if (!log.dataSubject.type) log.dataSubject.type = entity.name
  if (!log.dataSubject.id.find(ele => ele.key === key)) {
    const value = row[key] || (row._old && row._old[key])
    log.dataSubject.id.push({ keyName: key, value: String(value) })
  }
}

const _addKeysToWhere = (child, row) => {
  const keysWithValue = []
  Object.keys(child.keys).forEach(el => {
    if (keysWithValue.length > 0) keysWithValue.push('and')
    keysWithValue.push({ ref: [child.name, el] }, '=', {
      val: row[el]
    })
  })
  return keysWithValue
}

const _buildSubSelect = (model, child, { element, up }, row, previousCqn) => {
  const entity = element.parent
  const childCqn = SELECT.from(child.name)
    .columns(Object.keys(child.keys))
    .where(entity._relations[element.name].join(child.name, entity.name))
  if (previousCqn) {
    childCqn.where('exists', previousCqn)
  } else {
    childCqn.where(_addKeysToWhere(child, row))
  }
  if (up) return _buildSubSelect(model, entity, up, {}, childCqn)
  return childCqn
}

const _getDataSubjectIdPromise = (child, dataSubjectInfo, row, req, model) => {
  const root = dataSubjectInfo.entity
  const cqn = SELECT.from(root.name)
    .columns(Object.keys(root.keys))
    .where(['exists', _buildSubSelect(model, child, dataSubjectInfo.up, row)])
  return cds
    .tx(req)
    .run(cqn)
    .then(res => {
      const id = []
      for (const k in res[0]) id.push({ keyName: k, value: String(res[0][k]) })
      return id
    })
}

const addDataSubjectForDetailsEntity = (row, log, req, entity, model, element) => {
  const role = entity['@PersonalData.DataSubjectRole']

  const dataSubjectInfo = getDataSubject(entity, model, role, element)

  log.dataSubject.type = dataSubjectInfo.entity.name

  /*
   * for each req (cf. $batch with atomicity) and data subject role (e.g., customer vs supplier),
   * store (in audit data structure at context) and reuse a single promise to look up the respective data subject
   */
  const mapKey = getMapKeyForCurrentRequest(req)
  if (!req.context._audit.dataSubjects) req.context._audit.dataSubjects = new Map()
  if (!req.context._audit.dataSubjects.has(mapKey)) req.context._audit.dataSubjects.set(mapKey, new Map())
  const map = req.context._audit.dataSubjects.get(mapKey)
  if (map.has(role)) log.dataSubject.id = map.get(role)
  else map.set(role, _getDataSubjectIdPromise(entity, dataSubjectInfo, row, req, model))
}

const resolveDataSubjectPromises = async logs => {
  const dataSubjPromise = logs.filter(el => el.dataSubject.id instanceof Promise)
  const res = await Promise.all(dataSubjPromise.map(el => el.dataSubject.id))
  dataSubjPromise.forEach((el, i) => (el.dataSubject.id = res[i]))
}

module.exports = {
  getMapKeyForCurrentRequest,
  getRootEntity,
  getPick,
  createLogEntry,
  addObjectID,
  addDataSubject,
  addDataSubjectForDetailsEntity,
  resolveDataSubjectPromises
}
