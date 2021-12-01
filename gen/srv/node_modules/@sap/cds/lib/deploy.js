const cds = require('./index'), { local } = cds.utils
const DEBUG = cds.debug('deploy')
let LOG

/**
 * Implementation of `cds.deploy` common to all databases.
 * It uses the database-specific `db.deploy` to prepare the database, e.g.
 * deploy create tables and views in case of a SQL database, then fills
 * in initial data, if present.
 */
exports = module.exports = function cds_deploy (model,options) { return {

  /** @param {cds.Service} db */
  async to (db, o=options||{}) { // NOSONAR
    LOG = cds.log('deploy')

    if (model && !model.definitions)  model = await cds.load (model)
    if (o.mocked) exports.include_external_entities_in (model)
    else exports.exclude_external_entities_in (model)

    if (!db.run) db = await cds.connect.to(db)
    if (!cds.db) cds.db = cds.services.db = db
    if (!db.model) db.model = model

    if (o.ddl !== false) {
      const any = await db.deploy (model,o) //> CREATE TABLE, ...
      if (!any)  return
    }

    // fill in initial data...
    const SILENT = o.silent || !LOG._info
    await init_from_code (db,model,SILENT)
    await init_from_csv (db,model,SILENT)
    await init_from_json (db,model,SILENT)

    const {credentials} = db.options, file = credentials && (credentials.database || credentials.url)
    if (!SILENT) {
      if (file !== ':memory:')  console.log (`/> successfully deployed to ./${file}\n`)
      else console.log (`/> successfully deployed to sqlite in-memory db\n`)
    }

    return db
  },
  // continue to support cds.deploy() as well...
  then(n,e) { return this.to (cds.db||'db') .then (n,e) },
  catch(e) { return this.to (cds.db||'db') .catch (e) },
}}




const { fs, path, isdir, isfile, read } = cds.utils
const { readdir } = fs.promises

exports.include_external_entities_in = function (model) {
  if (model._mocked) return model; else Object.defineProperty(model,'_mocked',{value:true})
  for (let each in model.definitions) {
    const def = model.definitions[each]
    if (def['@cds.persistence.mock'] === false) continue
    if (def['@cds.persistence.skip'] === true) {
      DEBUG && DEBUG ('including mocked', each)
      delete def['@cds.persistence.skip']
    }
  }
  exports.exclude_external_entities_in (model)
  return model
}

exports.exclude_external_entities_in = function (csn) { // NOSONAR
  // IMPORTANT to use cds.env.requires below, not cds.requires !!
  for (let [each,{service=each,model,credentials}] of Object.entries (cds.env.requires)) {
    if (!model) continue //> not for internal services like cds.requires.odata
    if (!credentials && csn._mocked) continue //> not for mocked unbound services
    DEBUG && DEBUG ('excluding external entities for', service, '...')
    const prefix = service+'.'
    for (let each in csn.definitions) if (each.startsWith(prefix)) _exclude (each)
  }
  return csn

  function _exclude (each) {
    const def = csn.definitions[each]; if (def.kind !== 'entity') return
    if (def['@cds.persistence.table'] === true) return // do not exclude replica table
    DEBUG && DEBUG ('excluding external entity', each)
    def['@cds.persistence.skip'] = true
    // propagate to all views on top...
    for (let other in csn.definitions) {
      const d = csn.definitions[other]
      const p = d.query && d.query.SELECT || d.projection
      if (p && p.from.ref && p.from.ref[0] === each) _exclude (other)
    }
  }
}


function init_from_code (db, csn, SILENT) {

  if (!csn.$sources)  return
  const folders = new Set([ path.resolve(cds.root,'db'), ...csn.$sources.map (path.dirname)])
  const inits = []

  for (let each of folders) {
    let file;
    if (process.env.CDS_TYPESCRIPT === 'true') file = isfile (path.resolve(each,'init.ts'));
    if (!file) file = isfile (path.resolve(each,'init.js'));
    if (!file)  continue
    SILENT || console.log (` > initializing database from ${local(file)}`)  // eslint-disable-line
    let init = require(file);  if (!init)  continue
    if (!init.then && typeof init === 'function')  inits.push (init(db))
    else  inits.push (init)
  }

  return Promise.all (inits)
}

function init_from_csv (db, csn, SILENT) {
  return init_from_ (['data','csv'], _csvs, db, csn, SILENT, (entity, src) => {
    let [ cols, ...rows ] = cds.parse.csv (src)
    return rows && rows[0] && INSERT.into (entity) .columns (cols) .rows (rows)
  });

  function _csvs (filename,_,allFiles) {
    if (filename[0] === '-' || !filename.endsWith ('.csv'))  return false
    if (/[._]texts\.csv$/.test (filename) && check_lang_file(filename, allFiles)) {
      return false
    }
    return true
  }
}

function init_from_json (db, csn, SILENT) {
  return init_from_ (['data'], _jsons, db, csn, SILENT, (entity, src) => {
      let json = JSON.parse (src)
      return json[0] && INSERT.into (entity) .entries (json)
  });

  function _jsons (filename,_,allFiles) {
    if (filename[0] === '-' || !filename.endsWith ('.json'))  return false
    if (/[._]texts\.json$/.test (filename) && check_lang_file(filename, allFiles)) {
      return false
    }
    return true
  }
}

function check_lang_file(filename, allFiles) {
  // ignores 'Books_texts.csv/json' if there is any 'Books_texts_LANG.csv/json'
  const basename = path.parse(filename).name
  const monoLangFiles = allFiles.filter (file => new RegExp('^'+basename+'_').test (file))
  if (monoLangFiles.length > 0) {
    DEBUG && DEBUG (`ignoring '${filename}' in favor of [${monoLangFiles}]`)  // eslint-disable-line
    return true
  }
  return false
}

async function init_from_ (locations, filter, db, csn, SILENT, INSERT_into) { // NOSONAR

  const folders = new Set
  let roots = Object.values(cds.env.folders); if (cds.env.features.test_data) roots.push('test/')
  for (let root of roots) {
    for (let data of locations) {
      let each = path.resolve(root,data)
      if (isdir (each))  folders.add(each)
    }
  }
  if (csn.$sources) for (let model of csn.$sources) {
    for (let data of locations) {
      let each = path.resolve(model,'..',data)
      if (isdir (each))  folders.add(each)
    }
  }

  if (folders.size === 0) return

  const {local} = cds.utils, inits = [], err = new Error
  await db.tx (async tx => {
    for (let folder of folders) {
      const files = await readdir (folder)
      for (let each of files.filter (filter)) {
        let name = each.replace(/-/g,'.').slice(0, -path.extname(each).length)
        let entity = _entity4 (name)
        if (!entity) { DEBUG && DEBUG (`warning: ${name} not in model`); continue }
        if (entity['@cds.persistence.skip'] === true) continue
        const file = path.join(folder,each)
        const src = await read (file,'utf8'); if (!src) continue
        const q = INSERT_into (entity,src)
        if (!q)  { DEBUG && DEBUG (`skipping empty ${local(file)}`); continue }
        SILENT || console.log (`\x1b[2m > filling ${entity.name} from ${local(file)} \x1b[0m`) // eslint-disable-line
        inits.push (tx.run(q).catch(e=>{ const ex = new Error
          e.stack = e.message +'\n'+ require('util').inspect(q) + err.stack.slice(5)
            .replace (/deploy\.js:\d+:/, ex.stack.slice(5).match(/deploy\.js:\d+:/)[0])
          throw e
        }))
      }
    }
    await Promise.all(inits)
  })

  function _entity4 (name) {
    let entity = csn.definitions [name]
    if (!entity) {
      if (/(.+)[._]texts_?/.test (name)) { // 'Books.texts', 'Books.texts_de'
        const base = csn.definitions [RegExp.$1]
        return base && _entity4 (base.elements.texts.target)
      }
      else return
    }
    // We also support insert into simple views if they have no projection
    const p = entity.query && entity.query.SELECT || entity.projection
    if (p && !p.columns && p.from.ref && p.from.ref.length === 1) {
      if (csn.definitions [p.from.ref[0]])  return entity
    }
    return entity.name ? entity : { name, __proto__:entity }
  }

}
/* eslint no-console: off */
