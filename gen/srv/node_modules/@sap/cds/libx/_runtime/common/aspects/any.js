const { getRelations, isMandatory, isReadOnly } = require('./utils')

// NOTE: Please only add things which are relevant to _any_ type,
// use specialized types otherwise (entity, Association, ...).
module.exports = class {
  get _isStructured() {
    return this.own('__isStructured') || this.set('__isStructured', !!this.elements && this.kind !== 'entity')
  }

  get _isMandatory() {
    return this.own('__isMandatory') || this.set('__isMandatory', !this.isAssociation && isMandatory(this))
  }

  get _isReadOnly() {
    return this.own('__isReadOnly') || this.set('__isReadOnly', !this.key && isReadOnly(this))
  }

  // REVISIT: Where to put?
  get _relations() {
    return this.own('__relations') || this.set('__relations', getRelations(this))
  }
}
