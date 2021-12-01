/** @type {import('../../lib')} */
const cds = global.cds || require('../../lib')
module.exports = cds

/*
 * csn aspects
 */
const { any, entity, Association } = cds.builtin.classes
cds.extend(any).with(require('./common/aspects/any'))
cds.extend(Association).with(require('./common/aspects/Association'))
cds.extend(entity).with(require('./common/aspects/entity'))

/*
 * mtx?
 */
Object.defineProperty(cds, '_mtxEnabled', {
  get: () => cds.mtx && typeof cds.mtx.in === 'function',
  configurable: true
})

/*
 * (lazy) feature flags
 */
// referential integrity
Object.defineProperty(cds.env.features, '_foreign_key_constraints', {
  get: () => cds.env.cdsc.beta && cds.env.cdsc.beta.foreignKeyConstraints,
  configurable: true
})
let assertIntegrity = cds.env.features.assert_integrity
Object.defineProperty(cds.env.features, 'assert_integrity', {
  get: () => (assertIntegrity != null ? assertIntegrity : !cds.env.features._foreign_key_constraints),
  set: val => {
    assertIntegrity = val
  },
  configurable: true
})
