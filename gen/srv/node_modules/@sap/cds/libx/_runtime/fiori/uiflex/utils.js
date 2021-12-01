const { ensureNoDraftsSuffix } = require('../../common/utils/draft')
const { ensureUnlocalized } = require('../../fiori/utils/handler')

const EXT_BACK_PACK = 'extensions__'

const getTargetRead = req => {
  let name = ''
  if (req.query.SELECT.from.join) {
    // join
    name = req.query.SELECT.from.args.find(arg => arg.ref && arg.ref[0] !== 'DRAFT.DraftAdministativeData').ref[0]
  } else if (req.target.name.SET) {
    // union
    name = req.target.name.SET.args[0]._target.name
  } else {
    // simple select
    name = req.target.name
  }

  return { name: ensureUnlocalized(ensureNoDraftsSuffix(name)) }
}

const getTargetWrite = (target, model) => {
  return model.definitions[ensureUnlocalized(ensureNoDraftsSuffix(target.name))]
}

const isExtendedEntity = (entityName, model) => {
  // REVISIT: Dass alle unsere und auch custom handlers immer die ensureUnlocalized + ensureNoDraftsSuffix schleife drehen müssen, kann nicht sein
  const entity = model.definitions[ensureUnlocalized(ensureNoDraftsSuffix(entityName))]
  return entity.elements[EXT_BACK_PACK] || Object.values(entity.elements).some(el => el['@cds.extension'])
}

const _hasExtendedEntityArgs = (args, model) => {
  return args.find(arg => {
    if (arg.ref) {
      return arg.ref[0] !== 'DRAFT.DraftAdministativeData' && isExtendedEntity(arg.ref[0], model)
    }

    if (arg.join) {
      return _hasExtendedEntityArgs(arg.args, model)
    }
  })
}

const hasExtendedEntity = (req, model) => {
  if (!req.query.SELECT) return false

  if (req.query.SELECT.from.join) {
    // join
    return _hasExtendedEntityArgs(req.query.SELECT.from.args, model)
  } else if (req.target.name.SET) {
    // union
    return isExtendedEntity(req.target.name.SET.args[0]._target.name, model)
  } else {
    // simple select
    return isExtendedEntity(req.target.name, model)
  }
}

const getExtendedFields = (entityName, model) => {
  const elements = model.definitions[ensureUnlocalized(ensureNoDraftsSuffix(entityName))].elements

  return Object.values(elements)
    .filter(element => {
      return element['@cds.extension']
    })
    .map(element => {
      return element.name
    })
}

module.exports = {
  EXT_BACK_PACK,
  getTargetRead,
  getTargetWrite,
  isExtendedEntity,
  hasExtendedEntity,
  getExtendedFields
}
