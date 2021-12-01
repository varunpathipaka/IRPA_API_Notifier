const cds = require('../../cds')

const { SELECT } = cds.ql

const { isNavigationToMany } = require('../utils/req')
const { getKeysCondition } = require('../utils/where')
const { isDraftActivateAction, ensureNoDraftsSuffix, ensureDraftsSuffix, draftIsLocked } = require('../utils/handler')

const { isCustomOperation } = require('../../cds-services/adapter/odata-v4/utils/request')

const { DRAFT_COLUMNS_ADMIN } = require('../../common/constants/draft')

// copied from adapter/odata-v4/utils/context-object
const _getTargetEntityName = (service, pathSegments) => {
  if (isCustomOperation(pathSegments, false)) {
    return undefined
  }

  let navSegmentName
  let entityName = `${service.name}.${pathSegments[0].getEntitySet().getName()}`

  for (const navSegment of pathSegments.filter(segment => segment.getNavigationProperty() !== null)) {
    navSegmentName = navSegment.getNavigationProperty().getName()
    entityName = service.model.definitions[entityName].elements[navSegmentName].target
  }

  return entityName
}

/**
 * Provide information about the parent entity, i.e. the entity that has the to-many composition element.
 * Limitation: only works for one key (besides IsActiveEntity)
 *
 * @param service
 * @param req
 * @returns {object}
 * @private
 */
const _getParent = (service, req) => {
  // REVISIT: get rid of getUriInfo
  if (!req.getUriInfo) return

  const segments = req.getUriInfo().getPathSegments()

  if (segments.length === 1) return

  const parent = {
    entityName: _getTargetEntityName(service, segments.slice(0, segments.length - 1))
  }

  const parentKeyPredicates = segments[segments.length - 2].getKeyPredicates()
  let keyPredicateName, keyPredicateText
  for (const keyPredicate of parentKeyPredicates) {
    keyPredicateName = keyPredicate.getEdmRef().getName()
    keyPredicateText = keyPredicate.getText()

    if (keyPredicateName === 'IsActiveEntity') {
      parent.IsActiveEntity = keyPredicateText
    } else {
      parent.keyName = keyPredicateName
      parent.keyValue = keyPredicateText
    }
  }

  return parent
}

const _validateDraft = (draftResult, req) => {
  if (!draftResult || draftResult.length === 0) {
    req.reject(404)
  }

  const draftAdminData = draftResult[0]

  // the same user that locked the entity can always delete it
  if (draftAdminData.InProcessByUser === req.user.id) {
    return
  }

  // proceed with the delete action only if it was initiated by a different user
  // than the one who locked the entity and the configured drafts cancellation
  // timeout timer has expired
  if (draftIsLocked(draftAdminData.LastChangeDateTime)) {
    req.reject(403, 'DRAFT_LOCKED_BY_ANOTHER_USER')
  }
}

const _addDraftDataToContext = (req, result) => {
  _validateDraft(result, req)

  if (req.rejected) {
    return
  }

  if (!req._draftMetadata) {
    req._draftMetadata = {}
  }

  DRAFT_COLUMNS_ADMIN.forEach(column => {
    if (column in result[0]) req._draftMetadata[column] = result[0][column]
  })

  req.data.DraftAdministrativeData_DraftUUID = result[0].DraftUUID
}

const _prefixDraftColumns = () => {
  return DRAFT_COLUMNS_ADMIN.map(col => {
    return { ref: ['DRAFT_DraftAdministrativeData', col] }
  })
}

const _getSelectDraftDataCqn = (entityName, where) => {
  return SELECT.from(ensureDraftsSuffix(entityName), _prefixDraftColumns())
    .join('DRAFT.DraftAdministrativeData')
    .on('DraftAdministrativeData_DraftUUID =', { ref: ['DRAFT.DraftAdministrativeData', 'DraftUUID'] })
    .where(where)
}

const _addDraftDataFromExistingDraft = async (req, service) => {
  const parent = _getParent(service, req)
  let result

  if (parent && parent.IsActiveEntity === 'false') {
    const parentWhere = [{ ref: [parent.keyName] }, '=', { val: parent.keyValue }]
    result = await cds.tx(req).run(_getSelectDraftDataCqn(parent.entityName, parentWhere))
    _addDraftDataToContext(req, result)
    return result
  }

  if (!parent) {
    const rootWhere = getKeysCondition(req.target, req.data)
    result = await cds.tx(req).run(_getSelectDraftDataCqn(ensureNoDraftsSuffix(req.target.name), rootWhere))
    if (result && result.length > 0) {
      _addDraftDataToContext(req, result)
    }
    return result
  }

  return []
}

const _addGeneratedDraftUUID = async req => {
  req._draftMetadata = req._draftMetadata || {}
  req.data.DraftAdministrativeData_DraftUUID = cds.utils.uuid()
  req._draftMetadata.DraftUUID = req.data.DraftAdministrativeData_DraftUUID
}

/**
 * Generic Handler for before NEW requests.
 *
 * @param req
 */
const _new = async function (req) {
  if (isDraftActivateAction(req)) return

  if (isNavigationToMany(req)) {
    const result = await _addDraftDataFromExistingDraft(req, this)

    // in order to fix strange case where active subitems are created in draft case
    if (result.length === 0) req.reject(404)
  } else {
    _addGeneratedDraftUUID(req)
  }
}

/**
 * Generic Handler for before PATCH and UPDATE requests.
 *
 * @param req
 */
const _patchUpdate = async function (req) {
  if (isDraftActivateAction(req)) return

  const result = await _addDraftDataFromExistingDraft(req, this)

  // means that draft not exists
  if (result.length === 0) req.reject(404)
}

/**
 * Generic Handler for before DELETE and CANCEL requests.
 *
 * @param req
 */
const _deleteCancel = async function (req) {
  await _addDraftDataFromExistingDraft(req, this)
}

module.exports = cds.service.impl(function () {
  _new._initial = true
  _patchUpdate._initial = true
  _deleteCancel._initial = true

  for (const entity of Object.values(this.entities).filter(e => e._isDraftEnabled)) {
    this.before('NEW', entity, _new)
    this.before(['PATCH', 'UPDATE'], entity, _patchUpdate)
    this.before(['DELETE', 'CANCEL'], entity, _deleteCancel)
  }
})
