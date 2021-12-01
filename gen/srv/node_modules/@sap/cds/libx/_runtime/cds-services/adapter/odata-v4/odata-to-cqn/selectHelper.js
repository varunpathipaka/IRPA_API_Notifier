const { getFeatureNotSupportedError } = require('../../../util/errors')
const { deepCopyArray } = require('../../../../common/utils/copy')
const cds = require('../../../../cds')
const { cqn2cqn4sql } = require('../../../../common/utils/cqn2cqn4sql')

const isNavigation = pathSegments => {
  return pathSegments.length > 1 && pathSegments[1].getKind().startsWith('NAVIGATION')
}

const isViewWithParams = target => {
  return target.params && Object.keys(target.params).length > 0
}

const getValidationQuery = (ref, model) => {
  const refQuery = deepCopyArray(ref.slice(0, ref.length - 1))
  const cqn = cds.ql.SELECT.from({ ref: refQuery }).columns({
    val: 1,
    as: 'validationQuery'
  })

  return cqn2cqn4sql(cqn, model)
}

const isPathSupported = (supported, pathSegments) => {
  for (const segment of pathSegments) {
    if (!supported[segment.getKind()]) {
      throw getFeatureNotSupportedError(`Request parameter "${segment.getKind()}"`)
    }
  }
}

module.exports = {
  isNavigation,
  isViewWithParams,
  isPathSupported,
  getValidationQuery
}
