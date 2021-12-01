module.exports = async () => {
  const cds = require('../../cds')
  if (!cds.requires.db) return

  const db = await cds.connect.to({ ...cds.requires.db, model: null, silent: true })
  const rs = await db.read('cds_r.Extensions')
  if (rs.length !== 0) {
    const extensions = []
    rs.forEach(row => extensions.push(...JSON.parse(row.csn).extensions))
    cds.once('loaded', csn => {
      if (cds.model) return // extend cds.model only
      const extended = cds.compile({
        'base.csn': cds.compile.to.json(csn),
        'ext.csn': cds.compile.to.json({ extensions })
      })
      csn.definitions = extended.definitions
    })
  }
  await db.disconnect()

  if (cds.db) return // because of tests
  cds.once('served', () => {
    const { transformExtendedFieldsCREATE, transformExtendedFieldsUPDATE } = require('./handler/transformWRITE')
    const { transformExtendedFieldsREAD } = require('./handler/transformREAD')
    const { transformExtendedFieldsRESULT } = require('./handler/transformRESULT')
    cds.db
      .before('CREATE', transformExtendedFieldsCREATE)
      .before('UPDATE', transformExtendedFieldsUPDATE)
      .before('READ', transformExtendedFieldsREAD)
      .after('READ', transformExtendedFieldsRESULT)
    if ('cds_r.ExtensibilityService' in cds.services) return
    const model = require('path').join(__dirname, 'extensibility')
    return cds.serve(model, { silent: true }).to('odata').in(cds.app)
  })
}
