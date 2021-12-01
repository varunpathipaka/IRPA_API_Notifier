const { ensureNoDraftsSuffix } = require('../../common/utils/draft')

const getEntityNameFromCQN = cqn => {
  while (cqn.SELECT) cqn = cqn.SELECT.from

  // Do the most likely first -> {ref}
  if (cqn.ref) {
    return cqn.ref[0].id || cqn.ref[0]
  }

  // TODO cleanup
  // REVISIT infer should do this for req.target
  // REVISIT2 No, req.target doesn't make sense for joins
  if (cqn.SET) {
    return cqn.SET.args.map(getEntityNameFromCQN).find(n => n !== 'DRAFT.DraftAdministrativeData')
  }
  if (cqn.join) {
    return cqn.args.map(getEntityNameFromCQN).find(n => n !== 'DRAFT.DraftAdministrativeData')
  }
}

// Note: This also works for the common draft scenarios
const getEntityFromCQN = (req, service) => {
  if (!req.target || req.target._unresolved) {
    const entity = getEntityNameFromCQN(req.query)
    if (!entity) return
    return service.model.definitions[ensureNoDraftsSuffix(entity)]
  }
  return req.target
}

module.exports = {
  getEntityFromCQN,
  getEntityNameFromCQN
}
