/* istanbul ignore file */
/* eslint-disable max-len */
/* eslint-disable no-new-wrappers */

const cds = require('../../cds')
const { SELECT } = cds.ql

const { getRequiresAsArray } = require('../utils/auth')
const { cqn2cqn4sql } = require('../utils/cqn2cqn4sql')
const { isActiveEntityRequested, removeIsActiveEntityRecursively } = require('../../fiori/utils/where')
const { ensureDraftsSuffix } = require('../../fiori/utils/handler')

const WRITE = ['CREATE', 'UPDATE', 'DELETE']
const MOD = { UPDATE: 1, DELETE: 1, EDIT: 1 }
const WRITE_EVENTS = { CREATE: 1, NEW: 1, UPDATE: 1, PATCH: 1, DELETE: 1, CANCEL: 1, EDIT: 1 }
const DRAFT_EVENTS = { PATCH: 1, CANCEL: 1, draftActivate: 1, draftPrepare: 1 }
const DRAFT2CRUD = { NEW: 'CREATE', EDIT: 'UPDATE' }
const ODATA_DRAFT_ENABLED = '@odata.draft.enabled'
const FIORI_DRAFT_ENABLED = '@fiori.draft.enabled'

const RESTRICTIONS = {
  READABLE: 'ReadRestrictions.Readable',
  READABLE_BY_KEY: 'ReadRestrictions.ReadByKeyRestrictions.Readable',
  INSERTABLE: 'InsertRestrictions.Insertable',
  UPDATABLE: 'UpdateRestrictions.Updatable',
  DELETABLE: 'DeleteRestrictions.Deletable'
}

const _reject = req => {
  // unauthorized or forbidden?
  if (req.user._is_anonymous) {
    // REVISIT: improve `req._.req` check if this is an HTTP request
    if (req._.req && req.user._challenges && req.user._challenges.length > 0) {
      req._.res.set('WWW-Authenticate', req.user._challenges.join(';'))
    }
    // REVISIT: security log in else case?
    return req.reject(401)
  } else {
    // REVISIT: security log?
    return req.reject(403)
  }
}

const _getCurrentSubClause = (next, restrict) => {
  const escaped = next[0].replace(/\$/g, '\\$').replace(/\./g, '\\.')
  const re1 = new RegExp(`([\\w\\.']*)\\s*=\\s*(${escaped})|(${escaped})\\s*=\\s*([\\w\\.']*)`)
  const re2 = new RegExp(`([\\w\\.']*)\\s*in\\s*(${escaped})|(${escaped})\\s*in\\s*([\\w\\.']*)`)
  const clause = restrict.where.match(re1) || restrict.where.match(re2)
  if (!clause) {
    // NOTE: arrayed attr with "=" as operator is some kind of legacy case
    throw new Error('user attribute array must be used with operator "=" or "in"')
  }
  return clause
}

const _processUserAttr = (next, restrict, user, attr) => {
  const clause = _getCurrentSubClause(next, restrict)
  const valOrRef = clause[1] || clause[4]
  if (clause[0].match(/ in /)) {
    if (!user[attr] || user[attr].length === 0) {
      restrict.where = restrict.where.replace(clause[0], '1 = 2')
    } else if (user[attr].length === 1) {
      restrict.where = restrict.where.replace(clause[0], `${valOrRef} = '${user[attr][0]}'`)
    } else {
      restrict.where = restrict.where.replace(
        clause[0],
        `${valOrRef} in (${user[attr].map(ele => `'${ele}'`).join(', ')})`
      )
    }
  } else if (valOrRef.startsWith("'") && user[attr].includes(valOrRef.split("'")[1])) {
    restrict.where = restrict.where.replace(clause[0], `${valOrRef} = ${valOrRef}`)
  } else {
    restrict.where = restrict.where.replace(
      clause[0],
      `(${user[attr].map(ele => `${valOrRef} = '${ele}'`).join(' or ')})`
    )
  }
}

const _getShortcut = (attrs, attr) => {
  // undefined
  if (attrs[attr] === undefined) {
    return '1 = 2'
  }

  // $UNRESTRICTED
  if (
    (typeof attrs[attr] === 'string' && attrs[attr].match(/\$UNRESTRICTED/i)) ||
    (Array.isArray(attrs[attr]) && attrs[attr].some(a => a.match(/\$UNRESTRICTED/i)))
  ) {
    return '1 = 1'
  }

  return null
}

/*
 * for supporting xssec v3
 */
const _getAttrsAsProxy = (attrs, additional = {}) => {
  return new Proxy(
    {},
    {
      get: function (_, attr) {
        if (attr in additional) return additional[attr]
        return attrs[attr]
      }
    }
  )
}

/*
 * resolves user attributes deeply, even though nested attributes are officially not supported
 */
const _resolveUserAttrs = (restrict, req) => {
  const _getNext = where => where.match(/\$user\.([\w.]*)/)

  let next = _getNext(restrict.where)
  while (next !== null) {
    const parts = next[1].split('.')

    let skip
    let val
    let attrs = _getAttrsAsProxy(req.user.attr, { id: req.user.id })
    let attr = parts.shift()
    while (attr) {
      const shortcut = _getShortcut(attrs, attr)
      if (shortcut) {
        const clause = _getCurrentSubClause(next, restrict)
        restrict.where = restrict.where.replace(clause[0], shortcut)
        skip = true
        break
      }

      if (Array.isArray(attrs[attr])) {
        _processUserAttr(next, restrict, attrs, attr)
        skip = true
        break
      }

      val = !Number.isNaN(Number(attrs[attr])) && attr !== 'id' ? attrs[attr] : `'${attrs[attr]}'`
      if (val === null || val === undefined) break

      attrs = _getAttrsAsProxy(attrs[attr])
      attr = parts.shift()
    }
    if (!skip) restrict.where = restrict.where.replace(next[0], val === undefined ? null : val)

    next = _getNext(restrict.where)
  }

  return restrict
}

const _evalStatic = (op, vals) => {
  vals[0] = Number.isNaN(Number(vals[0])) ? vals[0] : Number(vals[0])
  vals[1] = Number.isNaN(Number(vals[1])) ? vals[1] : Number(vals[1])

  switch (op) {
    case '=':
      return vals[0] === vals[1]
    case '!=':
      return vals[0] !== vals[1]
    case '<':
      return vals[0] < vals[1]
    case '<=':
      return vals[0] <= vals[1]
    case '>':
      return vals[0] > vals[1]
    case '>=':
      return vals[0] >= vals[1]
    default:
      throw new Error(`Operator "${op}" is not supported in @restrict.where`)
  }
}

const _getMergedWhere = restricts => {
  const xprs = []
  restricts.forEach(ele => {
    xprs.push('(', ...ele._xpr, ')', 'or')
  })
  xprs.pop()
  return xprs
}

const _findTableName = (ref, aliases) => {
  const maxLength = Math.max(...aliases.map(alias => alias.length))
  let name = ''
  for (let i = 0; i < ref.length; i++) {
    name += name.length !== 0 ? `.${ref[i]}` : ref[i]

    if (name >= maxLength) {
      break
    }

    const aliasIndex = aliases.indexOf(name)
    if (aliasIndex !== -1) {
      return { refIndex: i, aliasIndex: aliasIndex, name: name }
    }
  }

  return { refIndex: -1 }
}

const _getTableForColumn = (col, aliases, model) => {
  for (let i = 0; i < aliases.length; i++) {
    const index = aliases.length - i - 1
    const alias = aliases[index]
    if (Object.keys(model.definitions[alias].elements).includes(col)) {
      return { index, table: alias.replace(/\./g, '_') }
    }
  }

  return { index: -1 }
}

const _adaptTableName = (ref, index, name) => {
  const tableName = name.replace(/\./g, '_')
  ref.splice(0, index + 1, tableName)
}

const _ensureTableAlias = (ref, aliases, targetFrom, model, hasExpand) => {
  const nameObj = _findTableName(ref, aliases)
  if (nameObj.refIndex === -1) {
    const { index, table } = _getTableForColumn(ref[0], aliases, model)
    if (index !== -1) {
      nameObj.aliasIndex = index
      if (table === targetFrom.name && targetFrom.as) {
        ref.unshift(targetFrom.as)
      } else {
        ref.unshift(table)
      }
    }
  } else {
    _adaptTableName(ref, nameObj.refIndex, nameObj.name)
  }
}

const _enhanceAnnotationSubSelect = (select, model, targetName, targetFrom, hasExpand) => {
  if (select.where) {
    for (const v of select.where) {
      if (v.ref && select.from.ref) {
        _ensureTableAlias(v.ref, [targetName, select.from.ref[0]], targetFrom, model, hasExpand)
      }
    }
  }
}

// Add alias symbols to refs if needed and mark ref (for expand) and SELECT.from (for draft)
const _enhanceAnnotationWhere = (query, where, model) => {
  const cqn2cqn4sqlOptions = { suppressSearch: true }
  query = cqn2cqn4sql(query, model, cqn2cqn4sqlOptions)
  const hasExpand = query.SELECT && query.SELECT.columns && query.SELECT.columns.some(col => col.expand)
  const targetFrom = query.SELECT
    ? { name: query.SELECT.from.ref[0].replace(/\./g, '_'), as: query.SELECT.from.as }
    : {}
  for (const w of where) {
    if (w.ref) {
      // REVISIT: can this case be removed permanently?
      // _ensureTableAlias(w.ref, [query._target.name], targetFrom, model, hasExpand)
    } else if (w.SELECT) {
      _enhanceAnnotationSubSelect(w.SELECT, model, query._target.name, targetFrom, hasExpand)
      w.SELECT.__targetFrom = targetFrom
    }
  }
}

const _getApplicables = (restricts, req) => {
  return restricts.filter(restrict => {
    const event = DRAFT2CRUD[req.event] || req.event
    return (restrict.grant === '*' || restrict.grant === event) && restrict.to.some(role => req.user.is(role))
  })
}

const _getResolvedApplicables = (applicables, req) => {
  const resolvedApplicables = []

  // REVISIT: the static portion of "mixed wheres" could already grant access -> optimization potential
  for (const restrict of applicables) {
    // replace $user.x with respective values
    const resolved = _resolveUserAttrs({ grant: restrict.grant, target: restrict.target, where: restrict.where }, req)

    // check for duplicates
    if (
      !resolvedApplicables.find(
        restrict =>
          resolved.grant === restrict.grant &&
          (!resolved.target || resolved.target === restrict.target) &&
          (!resolved.where || resolved.where === restrict.where)
      )
    ) {
      if (resolved.where) resolved._xpr = cds.parse.expr(resolved.where).xpr
      resolvedApplicables.push(resolved)
    }
  }

  return resolvedApplicables
}

const _isStaticAuth = resolvedApplicables => {
  return (
    resolvedApplicables.length === 1 &&
    resolvedApplicables[0]._xpr.length === 3 &&
    resolvedApplicables[0]._xpr.every(ele => typeof ele !== 'object' || ele.val)
  )
}

const _handleStaticAuth = (resolvedApplicables, req) => {
  const op = resolvedApplicables[0]._xpr.find(ele => typeof ele === 'string')
  const vals = resolvedApplicables[0]._xpr.filter(ele => typeof ele === 'object' && ele.val).map(ele => ele.val)
  if (!_evalStatic(op, vals)) {
    // static clause forbids access => forbidden
    return _reject(req)
  }
  // static clause grants access => done
}

const _getFromWithIsActiveEntityRemoved = from => {
  for (const element of from.ref) {
    if (element.where && isActiveEntityRequested(element.where)) {
      element.where = removeIsActiveEntityRecursively(element.where)
    }
  }
  return from
}

const _addWheresToRef = (ref, model, resolvedApplicables) => {
  const newRef = []
  let lastEntity = model.definitions[ref[0].id || ref[0]]
  ref.forEach((identifier, idx) => {
    if (idx === ref.length - 1) {
      newRef.push(identifier)
      return // determine last one separately
    }
    const entity = idx === 0 ? lastEntity : lastEntity.elements[identifier.id || identifier]._target
    lastEntity = entity
    const applicablesForEntity = resolvedApplicables.filter(
      restrict => restrict.target && restrict.target.name === entity.name
    )
    let newIdentifier = identifier
    if (applicablesForEntity.length) {
      if (typeof newIdentifier === 'string') {
        newIdentifier = { id: identifier, where: [] }
      }
      if (!newIdentifier.where) newIdentifier.where = []
      if (newIdentifier.where && newIdentifier.where.length) {
        newIdentifier.where.unshift('(')
        newIdentifier.where.push(')')
        newIdentifier.where.push('and')
      }
      newIdentifier.where.push(..._getMergedWhere(applicablesForEntity))
    }
    newRef.push(newIdentifier)
  })
  return newRef
}

const _getRestrictionForTarget = (resolvedApplicables, target) => {
  const reqTarget = target && (target[ODATA_DRAFT_ENABLED] ? target.name.replace(/_drafts$/, '') : target.name)
  const applicablesForTarget = resolvedApplicables.filter(
    restrict => restrict.target && restrict.target.name === reqTarget
  )
  if (applicablesForTarget.length) {
    return _getMergedWhere(applicablesForTarget)
  }
}

const _addRestrictionsToRead = async (req, model, resolvedApplicables) => {
  if (req.target._isDraftEnabled) {
    req.query._draftRestrictions = resolvedApplicables.map(ra => ra._xpr)
    return
  }

  if (typeof req.query.SELECT.from === 'object')
    req.query.SELECT.from.ref = _addWheresToRef(req.query.SELECT.from.ref, model, resolvedApplicables)

  const restrictionForTarget = _getRestrictionForTarget(resolvedApplicables, req.target)
  if (restrictionForTarget) {
    req.query.where(restrictionForTarget)
    // REVISIT: remove with cds^6
    _enhanceAnnotationWhere(req.query, restrictionForTarget, model)
  }
}

const _getUnrestrictedCount = async req => {
  const dbtx = cds.tx(req)

  const target =
    (req.query.UPDATE && req.query.UPDATE.entity) ||
    (req.query.DELETE && req.query.DELETE.from) ||
    (req.query.SELECT && req.query.SELECT.from)
  const selectUnrestricted = SELECT.one(['count(*) as n']).from(target)

  const whereUnrestricted = (req.query.UPDATE && req.query.UPDATE.where) || (req.query.DELETE && req.query.DELETE.where)
  if (whereUnrestricted) selectUnrestricted.where(whereUnrestricted)

  // Because of side effects, the statements have to be fired sequentially.
  const { n } = await dbtx.run(selectUnrestricted)
  return n
}

const _getRestrictedCount = async (req, model, resolvedApplicables) => {
  const dbtx = cds.tx(req)

  let target =
    (req.query.UPDATE && req.query.UPDATE.entity) ||
    (req.query.DELETE && req.query.DELETE.from) ||
    (req.query.SELECT && req.query.SELECT.from)
  // REVISIT: req._ gets set in onDraftActivate to original req
  if (req._ && req._.event === 'draftActivate') target = ensureDraftsSuffix(target)
  const selectRestricted = SELECT.one(['count(*) as n']).from(target)

  const whereRestricted = (req.query.UPDATE && req.query.UPDATE.where) || (req.query.DELETE && req.query.DELETE.where)
  if (whereRestricted) selectRestricted.where(whereRestricted)

  if (typeof selectRestricted.SELECT === 'object')
    selectRestricted.SELECT.from.ref = _addWheresToRef(selectRestricted.SELECT.from.ref, model, resolvedApplicables)

  const restrictionForTarget = _getRestrictionForTarget(resolvedApplicables, req.target)
  if (restrictionForTarget) selectRestricted.where(restrictionForTarget)

  const { n } = await dbtx.run(cqn2cqn4sql(selectRestricted, model, { suppressSearch: true }))
  return n
}

const _getRestrictsHandler = (restricts, definition, model) => {
  const bounds = Object.keys(definition.actions || {})
  const onlyBoundsAreRestricted = restricts.every(restrict => bounds.includes(restrict.grant))

  const handler = async function (req) {
    if (req.user._is_privileged || DRAFT_EVENTS[req.event]) {
      // > skip checks (events in DRAFT_EVENTS are checked in draft handlers via InProcessByUser)
      return
    }

    if (!bounds.includes(req.event) && onlyBoundsAreRestricted) {
      // no @restrict on entity level => done
      return
    }

    const applicables = _getApplicables(restricts, req)

    if (applicables.length === 0) {
      // no @restrict for req.event with the user's roles => forbidden
      return _reject(req)
    }

    if (applicables.some(restrict => !restrict.where)) {
      // at least one if the user's roles grants unrestricted access => done
      return
    }

    const resolvedApplicables = _getResolvedApplicables(applicables, req)

    // REVISIT: support more complex statics
    if (_isStaticAuth(resolvedApplicables)) {
      return _handleStaticAuth(resolvedApplicables, req)
    }

    // REVISIT: remove feature flag skip_restrict_where after grace period of at least two months (> April release)
    if (cds.env.features.skip_restrict_where === false) {
      if (req.event !== 'READ' && !MOD[req.event]) {
        // REVISIT: security log?
        req.reject({
          code: 403,
          internal: {
            reason: `Only static @restrict.where allowed for event "${req.event}"`,
            source: `@restrict.where of ${definition.name}`
          }
        })
      }
    }

    if (req.event === 'READ') {
      _addRestrictionsToRead(req, model, resolvedApplicables)
      return
    }

    if (!MOD[req.event]) {
      // no modification -> nothing more to do
      return
    }

    if (req.query.DELETE) req.query.DELETE.from = _getFromWithIsActiveEntityRemoved(req.query.DELETE.from)
    if (req.query.SELECT) req.query.SELECT.from = _getFromWithIsActiveEntityRemoved(req.query.SELECT.from)

    // REVISIT: selected data could be used for etag check, diff, etc.

    /*
     * Here we check if UPDATE/DELETE requests add additional restrictions
     * Note: Needs to happen sequentially because of side effects
     */
    const unrestrictedCount = await _getUnrestrictedCount(req)
    if (unrestrictedCount === 0) req.reject(404)

    const restrictedCount = await _getRestrictedCount(req, model, resolvedApplicables)
    if (restrictedCount < unrestrictedCount) {
      // REVISIT: security log?
      req.reject({
        code: 403,
        internal: {
          reason: `@restrict results in ${restrictedCount} affected rows out of ${unrestrictedCount}`,
          source: `@restrict.where of ${definition.name}`
        }
      })
    }

    // for minor optimization in generic crud handler
    req._authChecked = true
  }

  handler._initial = true

  return handler
}

const _getLocalName = definition => {
  return definition._service ? definition.name.replace(`${definition._service.name}.`, '') : definition.name
}

const _getRestrictWithEventRewrite = (grant, to, where, target) => {
  // REVISIT: req.event should be 'SAVE' and 'PREPARE'
  if (grant === 'SAVE') grant = 'draftActivate'
  else if (grant === 'PREPARE') grant = 'draftPrepare'
  return { grant, to, where, target }
}

const _addNormalizedRestrictPerGrant = (grant, where, restrict, restricts, definition) => {
  const to = restrict.to ? (Array.isArray(restrict.to) ? restrict.to : [restrict.to]) : ['any']
  if (definition.kind === 'entity') {
    if (grant === 'WRITE') {
      WRITE.forEach(g => {
        restricts.push(_getRestrictWithEventRewrite(g, to, where, definition))
      })
    } else {
      restricts.push(_getRestrictWithEventRewrite(grant, to, where, definition))
    }
  } else {
    restricts.push({ grant: _getLocalName(definition), to, where, target: definition.parent })
  }
}

const _addNormalizedRestrict = (restrict, restricts, definition, definitions) => {
  let where = restrict.where
    ? restrict.where.replace(/\$user/g, '$user.id').replace(/\$user\.id\./g, '$user.')
    : undefined

  // NOTE: "exists toMany.toOne[prop = $user]" -> "exists toMany[exists toOne[prop = $user]]"
  try {
    if (where) {
      // operate on a copy
      let _where = where
      // find all path expressions in order to normalize shorthand (i.e., inject "[exists ...]")
      const paths = (where.match(/ (\w\.*)*/g) || []).filter(m => m.match(/\./) && m !== ' ')
      for (let i = 0; i < paths.length; i++) {
        const parts = paths[i].trim().split('.')
        let current = definition
        while (parts.length) {
          current = current.elements[parts.shift()]
          if (current.isAssociation && _where.includes(current.name + '.')) {
            const matches = _where.match(new RegExp(`(${current.name}).(.*)]`))
            _where = _where.replace(`${matches[1]}.`, `${current.name}[exists `)
            _where = _where.replace(matches[2], `${matches[2]}]`)
          }
          if (current.target) current = definitions[current.target]
        }
      }
      where = _where
    }
  } catch (e) {
    // ignore
  }

  restrict.grant = Array.isArray(restrict.grant) ? restrict.grant : [restrict.grant || '*']
  restrict.grant.forEach(grant => _addNormalizedRestrictPerGrant(grant, where, restrict, restricts, definition))
}

const _getNormalizedRestricts = (definition, definitions) => {
  const restricts = []

  // own
  definition['@restrict'] &&
    definition['@restrict'].forEach(restrict => _addNormalizedRestrict(restrict, restricts, definition, definitions))

  // bounds
  if (definition.actions && Object.keys(definition.actions).some(k => definition.actions[k]['@restrict'])) {
    for (const k in definition.actions) {
      const action = definition.actions[k]
      if (action['@restrict']) {
        restricts.push(..._getNormalizedRestricts(action, definitions))
      } else if (!definition['@restrict']) {
        // > no entity-level restrictions => unrestricted action
        restricts.push({ grant: action.name, to: ['any'], target: action.parent })
      }
    }
  }

  return restricts
}

const _cqnFrom = req => {
  const { query } = req
  if (!query) return
  if (query.SELECT) return query.SELECT.from
  if (query.INSERT) return query.INSERT.into
  if (query.UPDATE) return query.UPDATE.entity
  if (query.DELETE) return query.DELETE.from
}

const _forPath = ({ model }, mainEntity, intermediateEntities, handler) => {
  // eslint-disable-next-line complexity
  const _isEntityRequested = cqn => {
    if (!cqn) return
    if (!cqn.ref || !Array.isArray(cqn.ref) || cqn.name)
      return cqn.ref === mainEntity || cqn.name === mainEntity || cqn === mainEntity
    // Special case for drafts, as compositions are directly accessed
    if (cqn.ref.length === 1) return true
    let targetName = cqn.ref[0].id || cqn.ref[0]
    if (targetName === mainEntity) return true
    let element
    // no need to look at first and last segments
    for (const seg of cqn.ref.slice(1, -1)) {
      const csn = targetName ? model.definitions[targetName] : element && (element.items || element)
      if (csn) {
        element = csn.elements && csn.elements[seg.id || seg]
        targetName = element && (element.target || element.type || (element.items && element.items.type))
        if (!targetName && !element) return
        if (targetName === mainEntity) return true
        if (!intermediateEntities.includes(targetName)) return
      }
    }
  }
  return req => _isEntityRequested(_cqnFrom(req)) && handler(req)
}

const _getRequiresHandler = requires => {
  const handler = function (req) {
    return !requires.some(role => req.user.is(role)) && _reject(req)
  }
  handler._initial = true
  return handler
}

const _registerEntityRequiresHandlers = (entity, srv, { dependentEntity, intermediateEntities } = {}) => {
  // own
  const requires = getRequiresAsArray(entity)
  if (requires.length > 0) {
    if (dependentEntity)
      srv.before('*', dependentEntity, _forPath(srv, entity.name, intermediateEntities, _getRequiresHandler(requires)))
    else srv.before('*', entity, _getRequiresHandler(requires))
  }

  // bounds
  if (!dependentEntity && entity.actions && Object.keys(entity.actions).some(k => entity.actions[k]['@requires'])) {
    for (const k in entity.actions) {
      const requires = getRequiresAsArray(entity.actions[k])
      if (requires.length > 0) {
        srv.before(k, entity, _getRequiresHandler(requires))
      }
    }
  }
}

const _registerEntityRestrictHandlers = (entity, srv, { dependentEntity, intermediateEntities } = {}) => {
  if (entity['@restrict'] || entity.actions) {
    const restricts = _getNormalizedRestricts(entity, srv.model.definitions)
    if (restricts.length > 0) {
      if (dependentEntity)
        srv.before(
          '*',
          dependentEntity,
          _forPath(srv, entity.name, intermediateEntities, _getRestrictsHandler(restricts, entity, srv.model))
        )
      else srv.before('*', entity, _getRestrictsHandler(restricts, entity, srv.model))
    }
  }
}

const _registerOperationRequiresHandlers = (operation, srv) => {
  const requires = getRequiresAsArray(operation)
  if (requires.length > 0) {
    srv.before(_getLocalName(operation), _getRequiresHandler(requires))
  }
}

const _registerOperationRestrictHandlers = (operation, srv) => {
  if (operation['@restrict']) {
    const restricts = _getNormalizedRestricts(operation, srv.model.definitions)
    if (restricts.length > 0) {
      srv.before(_getLocalName(operation), _getRestrictsHandler(restricts, operation, srv.model))
    }
  }
}

const _registerRejectsForReadonly = (entity, srv, { dependentEntity, intermediateEntities } = {}) => {
  const handler = function (req) {
    // @read-only (-> C_UD events not allowed but actions and functions are)
    if (entity._isReadOnly) {
      if (WRITE_EVENTS[req.event]) req.reject(405, 'ENTITY_IS_READ_ONLY', [entity.name])
      return
    }
    // autoexposed
    if (req.event !== 'READ') req.reject(405, 'ENTITY_IS_AUTOEXPOSED', [entity.name])
  }
  handler._initial = true

  // According to documentation, @cds.autoexposed + @cds.autoexpose entities are readonly.
  if (
    entity._isReadOnly ||
    (entity['@cds.autoexpose'] && entity['@cds.autoexposed']) ||
    entity.name.match(/\.DraftAdministrativeData$/)
  ) {
    // registering check for '*' makes the check future proof
    if (dependentEntity) srv.before('*', dependentEntity, _forPath(srv, entity.name, intermediateEntities, handler))
    else srv.before('*', entity, handler)
  }
}

const _registerRejectsForInsertonly = (entity, srv, { dependentEntity, intermediateEntities } = {}) => {
  const allowed = entity[ODATA_DRAFT_ENABLED] ? ['NEW', 'PATCH'] : ['CREATE']
  const handler = function (req) {
    return !allowed.includes(req.event) && req.reject(405, 'ENTITY_IS_INSERT_ONLY', [entity.name])
  }
  handler._initial = true

  if (entity['@insertonly']) {
    // registering check for '*' makes the check future proof
    if (dependentEntity) srv.before('*', dependentEntity, _forPath(srv, entity.name, intermediateEntities, handler))
    else srv.before('*', entity, handler)
  }
}

const _getCapabilitiesHandler = (entity, annotation, srv) => {
  const action = annotation.split('.').pop().toUpperCase()
  const _localName = entity => entity.name.replace(entity._service.name + '.', '')

  const _isRestricted = (req, capability, capabilityReadByKey) => {
    if (capabilityReadByKey !== undefined && req.query.SELECT.one) {
      return capabilityReadByKey === false
    }
    return capability === false
  }

  const _isNavigationRestricted = (target, path, req) => {
    if (!target) return
    const parts = annotation.split('.')
    if (target && Array.isArray(target['@Capabilities.NavigationRestrictions.RestrictedProperties'])) {
      for (const r of target['@Capabilities.NavigationRestrictions.RestrictedProperties']) {
        if (r.NavigationProperty['='] === path && r[parts[0]]) {
          return _isRestricted(
            req,
            r[parts[0]][parts[1]],
            r.ReadRestrictions && r.ReadRestrictions['ReadByKeyRestrictions.Readable']
          )
        }
      }
    }
  }

  const handler = function (req) {
    const from = _cqnFrom(req)
    const nav = (from && from.ref && from.ref.map(el => el.id || el)) || []

    if (nav.length > 1) {
      const path = nav.slice(1).join('.')
      const target = srv.model.definitions[nav[0]]
      if (_isNavigationRestricted(target, path, req)) {
        // REVISIT: rework exception with using target
        const trgt = `${_localName(target)}.${path}`
        req.reject(405, 'ENTITY_IS_NOT_CRUD_VIA_NAVIGATION', [_localName(entity), action, trgt])
      }
    } else if (
      _isRestricted(req, entity['@Capabilities.' + annotation], entity['@Capabilities.' + RESTRICTIONS.READABLE_BY_KEY])
    ) {
      req.reject(405, 'ENTITY_IS_NOT_CRUD', [_localName(entity), action])
    }
  }

  handler._initial = true

  return handler
}

const _authDependsOnParents = entity => {
  return entity['@cds.autoexposed'] && !entity['@cds.autoexpose']
}

const _traverseChildren = (srv, parentEntityDef, traversedEntities = []) => {
  if (traversedEntities.includes(parentEntityDef.name)) return // recursive compositions are handled in path filter
  traversedEntities.push(parentEntityDef.name)

  // We only need to look at compositions as only those can be autoexposed (without autoexpose)
  const children = Object.keys(parentEntityDef.compositions || {}).map(c => parentEntityDef.compositions[c])

  children
    .map(c => srv.model.definitions[c.target])
    .filter(t => _authDependsOnParents(t))
    .forEach(t => _traverseChildren(srv, t, traversedEntities))

  return traversedEntities
}

const _registerRejectsForCapabilities = (entity, srv, { dependentEntity, intermediateEntities } = {}) => {
  if (dependentEntity) {
    srv.before(
      'CREATE',
      dependentEntity,
      _forPath(srv, entity.name, intermediateEntities, _getCapabilitiesHandler(entity, RESTRICTIONS.INSERTABLE, srv))
    )
    srv.before(
      'READ',
      dependentEntity,
      _forPath(srv, entity.name, intermediateEntities, _getCapabilitiesHandler(entity, RESTRICTIONS.READABLE, srv))
    )
    srv.before(
      'UPDATE',
      dependentEntity,
      _forPath(srv, entity.name, intermediateEntities, _getCapabilitiesHandler(entity, RESTRICTIONS.UPDATABLE, srv))
    )
    srv.before(
      'DELETE',
      dependentEntity,
      _forPath(srv, entity.name, intermediateEntities, _getCapabilitiesHandler(entity, RESTRICTIONS.DELETABLE, srv))
    )
  } else {
    srv.before('CREATE', entity, _getCapabilitiesHandler(entity, RESTRICTIONS.INSERTABLE, srv))
    srv.before('READ', entity, _getCapabilitiesHandler(entity, RESTRICTIONS.READABLE, srv))
    srv.before('UPDATE', entity, _getCapabilitiesHandler(entity, RESTRICTIONS.UPDATABLE, srv))
    srv.before('DELETE', entity, _getCapabilitiesHandler(entity, RESTRICTIONS.DELETABLE, srv))
  }
}

const _registerAuthHandlers = (entity, srv, opts) => {
  // REVISIT: switch order? access control checks should be cheaper than authorization checks...

  // @requires (own and bounds)
  _registerEntityRequiresHandlers(entity, srv, opts)

  // @restrict (own and bounds)
  _registerEntityRestrictHandlers(entity, srv, opts)

  // @readonly (incl. DraftAdministrativeData by default)
  _registerRejectsForReadonly(entity, srv, opts)

  // @insertonly
  _registerRejectsForInsertonly(entity, srv, opts)

  // @Capabilities
  _registerRejectsForCapabilities(entity, srv, opts)
}

// REVISIT: What's missing here is the special draft case.
// Fiori Elements accesses draft compositions via top level access, e.g.
// PATCH SalesOrdersHeaders(ID=....,IsActiveEntity=false)
// as opposed to
// PATCH SalesOrders(ID=...,IsActiveEntity=false)/SalesOrdersHeaders
// therefore we must make sure to restrict direct access.
// Note: As the parent information is lost, we cannot support
// authorization checks in case one entity has several parents (in draft)

/*
 * Algorithm as follows:
 * 1) Determine traversedEntities, these are the auth-dependent entities which
 *    can be accessed through the auth root (without having auth entities in-between).
 *    Example: Foo/bar/baz -> traversedEntities = [bar, baz]
 *             ^^^
 *          auth root
 * 2) Register auth handlers for every traversed entity (with settings from auth root)
 *    Each of those auth handlers has an additional path restriction (`_forPath`).
 *    The path restriction checks that the segments always either include the
 *    traversed entities or the auth root, beginning from the last segment until the auth root
 *    (or only the traversed entities in case auf direct access).
 */
const _secureDependentEntities = srv => {
  const entities = Object.keys(srv.model.definitions)
    .map(n => srv.model.definitions[n])
    .filter(d => d.kind === 'entity' && !_authDependsOnParents(d))

  for (const e of entities) {
    const traversedEntities = _traverseChildren(srv, e)
    const [, ...intermediateEntities] = traversedEntities
    for (const entity of traversedEntities) {
      if (entity === e.name) continue // no need to secure auth root
      _registerAuthHandlers(e, srv, { dependentEntity: entity, intermediateEntities })
    }
    if (e[ODATA_DRAFT_ENABLED] || e[FIORI_DRAFT_ENABLED])
      _registerAuthHandlers(e, srv, { dependentEntity: 'DraftAdministrativeData', intermediateEntities })
  }
}

module.exports = cds.service.impl(function () {
  // @restrict, @requires, @readonly, @insertonly, and @Capabilities for entities
  _secureDependentEntities(this)
  for (const k in this.entities) {
    const entity = this.entities[k]
    if (!_authDependsOnParents(entity)) _registerAuthHandlers(entity, this)
  }

  // @restrict and @requires for operations
  for (const k in this.operations) {
    const operation = this.operations[k]

    // @requires
    _registerOperationRequiresHandlers(operation, this)

    // @restrict
    _registerOperationRestrictHandlers(operation, this)
  }
})
