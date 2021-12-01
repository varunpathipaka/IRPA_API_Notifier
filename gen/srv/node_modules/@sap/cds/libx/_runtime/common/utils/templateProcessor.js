const DELIMITER = require('./templateDelimiter')

const _formatRowContext = (tKey, keyNames, row) => {
  const keyValuePairs = keyNames.map(key => `${key}=${row[key]}`)
  const keyValuePairsSerialized = keyValuePairs.join(',')
  return `${tKey}(${keyValuePairsSerialized})`
}

const _processElement = (processFn, row, key, elements, picked = {}, complex = false, isRoot, pathOptions) => {
  const { segments: pathSegments } = pathOptions
  const element = elements[key]
  const { plain } = picked

  if (plain) {
    if (!complex && pathSegments) pathSegments.push(key)

    /**
     * @type import('../../types/api').templateProcessorProcessFnArgs
     */
    const elementInfo = { row, key, element, plain, isRoot, pathSegments }
    processFn(elementInfo)
  }
}

const _processRow = (processFn, row, template, tKey, tValue, isRoot, pathOptions) => {
  const { template: subTemplate, picked } = tValue
  const key = tKey.split(DELIMITER).pop()

  _processElement(processFn, row, key, template.target.elements, picked, !!subTemplate, isRoot, pathOptions)

  // process deep
  if (subTemplate) {
    let subRows = row && row[key]
    subRows = Array.isArray(subRows) ? subRows : [subRows]
    _processComplex(processFn, subRows, subTemplate, key, pathOptions)
  }
}

const _processComplex = (processFn, rows, template, tKey, pathOptions) => {
  if (rows.length === 0) return

  const segments = pathOptions.segments
  let keyNames

  for (const row of rows) {
    if (row == null) continue
    const args = { processFn, row, template, isRoot: false, pathOptions }

    if (pathOptions.includeKeyValues) {
      keyNames = keyNames || (template.target.keys && Object.keys(template.target.keys)) || []
      pathOptions.rowKeysGenerator(keyNames, row, template)
      const pathSegment = _formatRowContext(tKey, keyNames, { ...row, ...pathOptions.extraKeys })
      args.pathOptions.segments = segments ? [...segments, pathSegment] : [pathSegment]
    }

    templateProcessor(args)
  }
}

/**
 * @param {import("../../types/api").TemplateProcessor} args
 */
const templateProcessor = ({ processFn, row, template, isRoot = true, pathOptions = {} }) => {
  const segments = pathOptions.segments && [...pathOptions.segments]

  for (const [tKey, tValue] of template.elements) {
    if (segments) pathOptions.segments = [...segments]
    _processRow(processFn, row, template, tKey, tValue, isRoot, pathOptions)
  }
}

module.exports = templateProcessor
