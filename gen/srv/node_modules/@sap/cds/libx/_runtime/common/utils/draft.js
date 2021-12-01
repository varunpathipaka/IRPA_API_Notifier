const cds = require('../../cds')

const _4sqlite = cds.env.i18n && Array.isArray(cds.env.i18n.for_sqlite) ? cds.env.i18n.for_sqlite : []
// compiler reserves 'localized' and raises a corresponding exception if used in models
const LOCALIZED = 'localized'
const _tableExists = table => {
  if (!cds.db || !cds.db.model) return false
  return !!cds.db.model.definitions[table]
}
const ensureUnlocalized = table => {
  if (!table.startsWith(LOCALIZED)) return table
  const _table = table.substring(LOCALIZED.length + 1)
  const languagePrefix = _4sqlite.find(lang => _table.startsWith(lang))
  // for langu-like namespace 'de' and sqlite => _table === 'de.de.Books'
  if (languagePrefix && !_tableExists(_table)) {
    return _table.substring(languagePrefix.length + 1)
  }
  return _table
}

const ensureDraftsSuffix = name => {
  if (name.endsWith('_drafts')) {
    return name
  }

  return `${ensureUnlocalized(name)}_drafts`
}

const ensureNoDraftsSuffix = name => name.replace(/_drafts$/g, '')

module.exports = {
  ensureUnlocalized,
  ensureDraftsSuffix,
  ensureNoDraftsSuffix
}
