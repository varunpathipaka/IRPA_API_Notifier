/*
// global.cds is used on purpose here!
const cds = global.cds
*/

const { getETag, hasPersonalData, hasSensitiveData } = require('./utils')

let getSearchableColumns

module.exports = class {
  get _isSingleton() {
    return (
      this.own('__isSingleton') ||
      this.set(
        '__isSingleton',
        this['@odata.singleton'] || (this['@odata.singleton.nullable'] && this['@odata.singleton'] !== false)
      )
    )
  }

  get _hasPersistenceSkip() {
    return (
      this.own('__hasPersistenceSkip') ||
      this.set(
        '__hasPersistenceSkip',
        this.own('@cds.persistence.skip') && this.own('@cds.persistence.skip') !== 'if-unused'
      )
    )
  }

  get _isDraftEnabled() {
    return (
      this.own('__isDraftEnabled') ||
      this.set(
        '__isDraftEnabled',
        (this.associations && this.associations.DraftAdministrativeData) ||
          this.name.match(/\.DraftAdministrativeData$/) ||
          this.own('@odata.draft.enabled') // > case: entity not in service (tests only?)
      )
    )
  }

  get _searchableColumns() {
    // lazily require on first use
    getSearchableColumns =
      getSearchableColumns || require('../../cds-services/services/utils/columns').getSearchableColumns
    return this.own('__searchableColumns') || this.set('__searchableColumns', getSearchableColumns(this))
  }

  get _etag() {
    return this.own('__etag') || this.set('__etag', getETag(this))
  }

  /*
   * audit logging
   */

  get _hasPersonalData() {
    return this.own('__hasPersonalData') || this.set('__hasPersonalData', hasPersonalData(this))
  }

  get _hasSensitiveData() {
    return this.own('__hasSensitiveData') || this.set('__hasSensitiveData', hasSensitiveData(this))
  }

  get _auditCreate() {
    return (
      this.own('__auditCreate') ||
      this.set('__auditCreate', this._hasPersonalData && this['@AuditLog.Operation.Insert'])
    )
  }

  get _auditRead() {
    return this.own('__auditRead') || this.set('__auditRead', this._hasPersonalData && this['@AuditLog.Operation.Read'])
  }

  get _auditUpdate() {
    return (
      this.own('__auditUpdate') ||
      this.set('__auditUpdate', this._hasPersonalData && this['@AuditLog.Operation.Update'])
    )
  }

  get _auditDelete() {
    return (
      this.own('__auditDelete') ||
      this.set('__auditDelete', this._hasPersonalData && this['@AuditLog.Operation.Delete'])
    )
  }
}
