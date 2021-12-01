const cds = require('../../../../cds')
const { SELECT } = cds.ql
const { isActiveEntityRequested } = require('../../../../fiori/utils/where')
const { ensureDraftsSuffix } = require('../../../../fiori/utils/handler')
const { cqn2cqn4sql } = require('../../../../common/utils/cqn2cqn4sql')
const { findCsnTargetFor } = require('../../../../common/utils/csn')

const isStreaming = segments => {
  const lastSegment = segments[segments.length - 1]
  return (
    segments.length > 1 &&
    lastSegment.getKind() === 'PRIMITIVE.PROPERTY' &&
    lastSegment.getProperty().getType().getName() === 'Stream'
  )
}

const _getProperties = async (properties, req) => {
  const isActiveRequested = isActiveEntityRequested(req.query.SELECT.from.ref[0].where)

  // REVISIT DRAFT HANDLING: cqn2cqn4sql should not happen here, but adaptStreamCQN relies on exists clause
  const cqn = cqn2cqn4sql(SELECT.one(req.query.SELECT.from), req._model).columns(properties)

  // REVISIT: renaming of media type property (e.g., mimeType as MimeType in AFC) not reflected in @Core.MediaType
  if (req.target.query && req.target.query._target && !req.target.drafts) {
    cqn.SELECT.from.ref[0] = req.target.query._target.name
  }

  if (!isActiveRequested) {
    cqn.SELECT.from.ref[0] = ensureDraftsSuffix(cqn.SELECT.from.ref[0])
  }

  try {
    return await cds.tx(req).run(cqn)
  } catch (e) {
    // REVISIT: why ignore?
  }
}

const _getDynamicProperties = (contentType, contentDisposition) => {
  const properties = {}
  if (contentType && typeof contentType === 'object') {
    properties.contentType = Object.values(contentType)[0]
  }
  if (contentDisposition) {
    properties.contentDisposition = Object.values(contentDisposition)[0]
  }

  return properties
}

const getStreamProperties = async (segments, srv, req) => {
  // REVISIT: we need to read directly from db, which might not be there!
  if (!cds.db) return {}

  let contentType, entityName, namespace, contentDisposition
  const previous = segments[segments.length - 2]
  if (previous.getKind() === 'ENTITY') {
    entityName = previous.getEntitySet().getName()
    namespace = previous.getEdmType().getFullQualifiedName().namespace
  } else if (previous.getKind() === 'NAVIGATION.TO.ONE' && previous.getTarget()) {
    entityName = previous.getTarget().getName()
    namespace = previous.getTarget().getEntityType().getFullQualifiedName().namespace
  }

  if (entityName) {
    const entityDefinition = findCsnTargetFor(entityName, srv.model, namespace)
    if (entityDefinition._hasPersistenceSkip) return {}

    const mediaProperty = Object.values(entityDefinition.elements).find(ele => ele['@Core.MediaType'])
    contentType = mediaProperty['@Core.MediaType']
    // @Core.ContentDisposition.Filename is correct, but we released with @Core.ContentDisposition, so we should stay compatible
    contentDisposition = mediaProperty['@Core.ContentDisposition.Filename'] || mediaProperty['@Core.ContentDisposition']
    // contentDisposition can be only dynamic
    if (typeof contentType === 'object' || contentDisposition) {
      const properties = _getDynamicProperties(contentType, contentDisposition)
      const result = await _getProperties(Object.values(properties), req)
      if (properties.contentDisposition && result[properties.contentDisposition] !== undefined) {
        contentDisposition = result[properties.contentDisposition]
      }
      // REVISIT: renaming of media type property (e.g., mimeType as MimeType in AFC) not reflected in @Core.MediaType
      if (properties.contentType) {
        if (result[properties.contentType] !== undefined) contentType = result[properties.contentType]
        else {
          let ct
          for (const k in entityDefinition.elements) {
            if (entityDefinition.elements[k]['@Core.IsMediaType']) {
              ct = k
              break
            }
          }
          contentType = result[ct]
        }
      }
    }
  }

  return { contentType, contentDisposition }
}

module.exports = {
  isStreaming,
  getStreamProperties
}
