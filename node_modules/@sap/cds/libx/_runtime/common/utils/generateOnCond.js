const _toRef = (alias, column) => {
  if (Array.isArray(column)) column = column.join('_')
  return { ref: alias ? [alias, column] : [column] }
}

const _adaptRefs = (onCond, path, { select, join }) => {
  const adaptedOnCondition = onCond.map(el => {
    const ref = el.ref

    if (ref) {
      if (ref[0] === path.join('_') && ref[1]) {
        return _toRef(select, ref.slice(1))
      }

      // no alias for special $user of canonical localized association
      if (ref[0] === '$user' && path[0] === 'localized') {
        return _toRef(undefined, ref.slice(0))
      }

      return _toRef(join, ref.slice(0))
    }

    return el
  })

  return adaptedOnCondition
}

const _args = (csnElement, path, aliases) => {
  const onCond = csnElement.on

  if (!onCond || !onCond.length) {
    return []
  }

  if (onCond.length < 3) {
    return onCond
  }

  if (!csnElement._isSelfManaged) return _adaptRefs(onCond, path, aliases)

  // revert join and select aliases because of backlink
  const oc = _newOnConditions(csnElement._backlink, [csnElement._backlink.name], {
    select: aliases.join,
    join: aliases.select
  })

  if (onCond.some(e => e === 'and')) {
    // managed with ON-conditions must contain `$self`, which we replace with `oc`
    const onCondWithouSelf = _adaptRefs(_onCondWithout$self(onCond), path, aliases)
    oc.push('and', ...onCondWithouSelf)
  }

  return oc
}

const _isSelfRef = e => e && e.ref && e.ref[0] === '$self'

const _onCondWithout$self = onCond => {
  const onCondWithoutSelf = [...onCond]
  const selfIndex = onCondWithoutSelf.findIndex((e, i, on) => {
    if (e === 'and') return _isSelfRef(on[i + 1]) || _isSelfRef(on[i + 3])
    return on[i + 1] === '=' && (_isSelfRef(e) || _isSelfRef(on[i + 2]))
  })
  onCondWithoutSelf.splice(selfIndex, 4)
  return onCondWithoutSelf
}

const _foreignToOn = (csnElement, path, { select, join }) => {
  // this is only for 2one managed w/o ON-conditions i.e. no static values are possible
  const on = []
  for (const key of csnElement._foreignKeys) {
    if (on.length !== 0) {
      on.push('and')
    }
    const ref1 = _toRef(select, key.prefix ? `${key.prefix}_${key.childElement.name}` : key.childElement.name)
    const structPrefix = path.length > 1 ? path.slice(0, -1) : []
    const ref2 = _toRef(join, [...structPrefix, key.parentElement.name])
    on.push(ref1, '=', ref2)
  }
  return on
}

const _newOnConditions = (csnElement, path, aliases) => {
  if (csnElement.keys) {
    return _foreignToOn(csnElement, path, aliases)
  }

  return _args(csnElement, path, aliases)
}

const getOnCond = (csnElement, path = [], aliases = { select: '', join: '' }) => {
  return ['(', ..._newOnConditions(csnElement, path, aliases), ')']
}

module.exports = {
  getOnCond
}
