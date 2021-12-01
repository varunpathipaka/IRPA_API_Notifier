const { UNAUTHORIZED, FORBIDDEN } = require('../../_runtime/common/utils/auth')

const { getRequiresAsArray } = require('../../_runtime/common/utils/auth')

module.exports = (req, res, next) => {
  if (!req._srv._requires) req._srv._requires = getRequiresAsArray(req._srv.definition)

  const { _requires: requires } = req._srv

  if (req.path !== '/' && requires.length > 0 && !requires.some(r => req.user.is(r))) {
    // > unauthorized or forbidden?
    if (req.user._is_anonymous) {
      if (req.user._challenges) res.set('WWW-Authenticate', req.user._challenges.join(';'))
      // REVISIT: security log in else case?
      throw UNAUTHORIZED
    }
    // REVISIT: security log?
    throw FORBIDDEN
  }

  next()
}
