const getSafeNumber = str => {
  const n = Number(str)
  return Number.isSafeInteger(n) || String(n) === str ? n : str
}

const V4UUIDREGEXP = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
const isV4UUID = val => V4UUIDREGEXP.test(val)

const _odataV2Val = (val, type) => {
  switch (type) {
    case 'cds.Binary':
    case 'cds.LargeBinary':
      return `binary'${val}'`
    case 'cds.Date':
      return `datetime'${val}T00:00:00'`
    case 'cds.DateTime':
      return `datetime'${val}'`
    case 'cds.Time':
      // eslint-disable-next-line no-case-declarations
      const [hh, mm, ss] = val.split(':')
      return `time'PT${hh}H${mm}M${ss}S'`
    case 'cds.Timestamp':
      return `datetimeoffset'${val}'`
    case 'cds.UUID':
      return `guid'${val}'`
    default:
      return `'${val}'`
  }
}

const _isTimestamp = val =>
  /^\d+-\d\d-\d\d(T\d\d:\d\d(:\d\d(\.\d+)?)?(Z|([+-]{1}\d\d:\d\d))?)?$/.test(val) && !isNaN(Date.parse(val))

const _val = (val, type) => {
  switch (type) {
    case 'cds.Decimal':
    case 'cds.Integer64':
      return getSafeNumber(val)
    case 'cds.Boolean':
    case 'cds.DateTime':
    case 'cds.Date':
    case 'cds.Timestamp':
    case 'cds.Time':
    case 'cds.UUID':
      return val
    default:
      return _isTimestamp(val) ? val : `'${val}'`
  }
}

const formatVal = (val, element, csnTarget, kind) => {
  if (val === null || val === 'null') return 'null'
  if (typeof val === 'boolean') return val
  if (typeof val === 'number') return getSafeNumber(val)
  if (!csnTarget && typeof val === 'string' && isV4UUID(val)) return kind === 'odata-v2' ? `guid'${val}'` : val

  const csnElement = (csnTarget && csnTarget.elements && csnTarget.elements[element]) || { type: undefined }
  return kind === 'odata-v2' ? _odataV2Val(val, csnElement.type) : _val(val, csnElement.type)
}

module.exports = {
  getSafeNumber,
  formatVal
}
