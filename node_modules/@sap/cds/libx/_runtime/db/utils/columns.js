const cds = require('../../cds')
const resolveStructured = require('../../common/utils/resolveStructured')

const { DRAFT_COLUMNS } = require('../../common/constants/draft')

/**
 * This method gets all columns for an entity.
 * It includes the generated foreign keys from managed associations, structured elements and complex and custom types.
 * As well, it provides the annotations starting with '@' for each column.
 *
 * @param entity - the csn entity
 * @returns {Array} - array of columns
 */
const getColumns = (entity, { db, onlyKeys } = { db: true, onlyKeys: false }) => {
  // REVISIT is this correct or just a problem that occurs because of new structure we do not deal with yet?
  if (!(entity && entity.elements)) return []
  const columnNames = []
  // REVISIT!!!
  const elements = Object.getPrototypeOf(entity.elements) || entity.elements
  for (const elementName in elements) {
    const element = elements[elementName]
    if (onlyKeys && !element.key) continue
    if (element.isAssociation) continue
    if (db && entity._isDraftEnabled && DRAFT_COLUMNS.includes(elementName)) continue
    if (cds.env.effective.odata.structs && element.elements) {
      columnNames.push(...resolveStructured({ structName: elementName, structProperties: [] }, element.elements, false))
      continue
    }
    columnNames.push(elementName)
  }
  return columnNames.map(name => elements[name] || { name })
}

module.exports = getColumns
