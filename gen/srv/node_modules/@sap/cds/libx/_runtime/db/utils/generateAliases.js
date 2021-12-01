const { ensureUnlocalized } = require('../../common/utils/draft')

const ALIAS_PREFIX = 'ALIAS_'

const cleanUpName = name => {
  return ensureUnlocalized(name).replace(/\./g, '_')
}

const _redirectXpr = (xpr, aliasMap) => {
  if (!xpr) return

  xpr.forEach(element => {
    if (element.ref) {
      if (element.ref.length > 1) {
        const view = cleanUpName(element.ref[0])
        if (aliasMap.has(view)) {
          element.ref[0] = aliasMap.get(view)
        }
      }

      return
    }

    if (element.func) {
      _redirectXpr(element.args, aliasMap)
      return
    }

    if (element.list) {
      _redirectXpr(element.list, aliasMap)
      return
    }

    _generateAliases(element, aliasMap)
  })
}

const _redirectRef = (ref, aliasMap) => {
  if (ref.as) {
    aliasMap.set(cleanUpName(ref.ref[0]), ref.as)
  } else {
    ref.as = `${ALIAS_PREFIX}${aliasMap.size + 1}`
    aliasMap.set(cleanUpName(ref.ref[0].id || ref.ref[0]), ref.as)
  }
}

const _generateAliases = (partialCqn, aliasMap = new Map()) => {
  if (partialCqn.SELECT) {
    const selectMap = new Map(aliasMap)
    _generateAliases(partialCqn.SELECT, selectMap)

    _redirectXpr(partialCqn.SELECT.where, selectMap)
    _redirectXpr(partialCqn.SELECT.having, selectMap)
    _redirectXpr(partialCqn.SELECT.columns, selectMap)
    _redirectXpr(partialCqn.SELECT.groupBy, selectMap)
    return
  }

  if (partialCqn.from) {
    if (partialCqn.from.ref) {
      _redirectRef(partialCqn.from, aliasMap)
    } else {
      _generateAliases(partialCqn.from, aliasMap)
    }

    return
  }

  if (Object.prototype.hasOwnProperty.call(partialCqn, 'join')) {
    partialCqn.args.forEach(arg => {
      if (arg.ref) {
        _redirectRef(arg, aliasMap)
      } else {
        _generateAliases(arg, aliasMap)
      }
    })

    _redirectXpr(partialCqn.on, aliasMap)
    return
  }

  if (partialCqn.SET && partialCqn.SET.op === 'union') {
    partialCqn.SET.args.forEach(arg => {
      _generateAliases(arg, new Map(aliasMap))
    })

    return
  }

  if (partialCqn.xpr) {
    _redirectXpr(partialCqn.xpr, new Map(aliasMap))
  }
}

const generateAliases = query => {
  if (!query.SELECT) return
  _generateAliases(query)
}

module.exports = generateAliases
