// global.cds is used on purpose here!
const cds = global.cds

const ODATA_CONTAINED = '@odata.contained'

const { isSelfManaged, isBacklink, getAnchor, getBacklink } = require('./utils')
const { foreignKeyPropagations } = require('../utils/foreignKeyPropagations')

module.exports = class {
  get _isAssociationStrict() {
    return (
      this.own('__isAssociationStrict') ||
      this.set('__isAssociationStrict', !!(this.isAssociation && !this.isComposition))
    )
  }

  get _isAssociationEffective() {
    return (
      this.own('__isAssociationEffective') ||
      this.set(
        '__isAssociationEffective',
        this._isAssociationStrict && (!this[ODATA_CONTAINED] || this.name === 'DraftAdministrativeData')
      )
    )
  }

  get _isCompositionEffective() {
    return (
      this.own('__isCompositionEffective') ||
      this.set(
        '__isCompositionEffective',
        this.isComposition ||
          (this._isAssociationStrict && this[ODATA_CONTAINED] && this.name !== 'DraftAdministrativeData')
      )
    )
  }

  get _isContained() {
    return (
      this.own('__isContained') ||
      this.set(
        '__isContained',
        this.name !== 'DraftAdministrativeData_DraftUUID' &&
          ((this.isAssociation && this[ODATA_CONTAINED]) || (this.isComposition && cds.env.effective.odata.containment))
      )
    )
  }

  get _isSelfManaged() {
    return this.own('__isSelfManaged') || this.set('__isSelfManaged', isSelfManaged(this))
  }

  get _isBacklink() {
    return this.own('__isBacklink') || this.set('__isBacklink', isBacklink(this))
  }

  get _isCompositionBacklink() {
    return this.own('__isCompositionBacklink') || this.set('__isCompositionBacklink', isBacklink(this, true))
  }

  get _anchor() {
    return this.own('__anchor') || this.set('__anchor', getAnchor(this))
  }

  get _backlink() {
    return this.own('__backlink') || this.set('__backlink', getBacklink(this))
  }

  get _foreignKeys() {
    return this.own('__foreignKeys') || this.set('__foreignKeys', foreignKeyPropagations(this))
  }
}
