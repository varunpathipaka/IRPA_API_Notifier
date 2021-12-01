const EXPAND = Symbol.for('sap.cds.expand')
const GET_KEY_VALUE = Symbol.for('sap.cds.getKeyValue')
const TO_MANY = Symbol.for('sap.cds.toMany')
const TO_ACTIVE = Symbol.for('sap.cds.toActive')
const { DRAFT_COLUMNS } = require('../../common/constants/draft')

class RawToExpanded {
  constructor(configs, queries, one, rootEntity) {
    this._one = one
    this._toManyResults = { expand: {} }
    this._result = []
    this._configs = configs
    this._queries = queries
    this._rootEntity = rootEntity
  }

  /**
   * Parses and converts the raw result set(s) into one single expanded result set.
   *
   * @returns {Promise<Array>}
   */
  async toExpanded() {
    const { queries, mappings } = this._configs

    for (let i = 0, length = this._queries.length; i < length; i++) {
      const { _conversionMapper: conversionMapper = new Map(), _toManyTree: toManyTree = [] } = queries[i]
      const result = await this._queries[i]
      if (toManyTree.length === 0) {
        this._parseMainResult(result, mappings, conversionMapper, toManyTree)
      } else {
        this._parseExpandResult(result, mappings, conversionMapper, toManyTree)
      }
    }

    return this._one ? this._result[0] || null : this._result
  }

  _parseMainResult(result, mappings, conversionMapper, toManyTree) {
    for (const entry of result) {
      const parsed = this._parseRaw({ mappings, toManyTree, conversionMapper, entry })

      if (parsed) {
        this._result.push(parsed)
      }
    }
  }

  _isExpandEmpty(mapping, result) {
    for (const key in mapping) {
      const el = mapping[key]
      // if we expand automatically in groupby we have no other columns than objects
      if (typeof el !== 'string') return false
      if (result[el] !== null && !DRAFT_COLUMNS.some(col => el.endsWith(`_${col}`))) {
        return false
      }
    }
    return true
  }

  /**
   * Takes one result row (entry) and converts it into expanded row according to the config at the structureMap.
   *
   * @param {object} config
   * @param {Map} config.mappings - Config how to structure the entry. Can be nested which will lead to recursion.
   * @param {Map} config.conversionMapper - Post processing of values like 1/0 to true/false.
   * @param {object} config.entry - One row (entry) of the result set.
   * @param {object} config.toManyTree - Tree of 'to many' associations.
   * @returns {object}
   * @private
   */
  // eslint-disable-next-line complexity
  _parseRaw({ mappings, toManyTree, conversionMapper, entry }) {
    let isEntityNull

    const row = {}

    const rootIsActiveEntity = this._rootEntity._isDraftEnabled
      ? mappings.IsActiveEntity
        ? entry[mappings.IsActiveEntity] === null
          ? null
          : !!entry[mappings.IsActiveEntity]
        : 'IsActiveEntity' in entry
        ? entry.IsActiveEntity === null
          ? null
          : !!entry.IsActiveEntity
        : null
      : null

    // A raw row contains more elements than the config. Iterating over config is faster.
    for (const key in mappings) {
      // To many entries have been already processed and cached
      const mapping = mappings[key]
      if (mapping[TO_MANY]) {
        let expandedItems = this._getResultCache(toManyTree.concat(key))[mapping[GET_KEY_VALUE](false, entry)] || []

        // the expanded items may include the actives of the deleted drafts -> filter out
        if (rootIsActiveEntity !== null) {
          if (mapping[TO_ACTIVE]) expandedItems = expandedItems.filter(ele => ele.IsActiveEntity !== false)
          else expandedItems = expandedItems.filter(ele => ele.IsActiveEntity === rootIsActiveEntity)
        }

        row[key] = expandedItems
      } else if (typeof mapping === 'object') {
        // > Will be true in case of 1:1 expands
        // check if the expanded entry doesn't exists
        if (this._isExpandEmpty(mapping, entry)) {
          row[key] = null
          continue
        }
        const parsed = this._parseRaw({
          mappings: mapping,
          toManyTree: toManyTree.concat(key),
          conversionMapper: conversionMapper,
          entry: entry
        })

        // the expanded items may include the actives of the deleted drafts -> filter out
        if (rootIsActiveEntity === null || key === 'DraftAdministrativeData') {
          row[key] = parsed || null
        } else if (rootIsActiveEntity !== null) {
          if (mapping[TO_ACTIVE]) row[key] = parsed && parsed.IsActiveEntity !== false ? parsed : null
          else if (rootIsActiveEntity) row[key] = parsed && parsed.IsActiveEntity !== false ? parsed : null
          else row[key] = parsed && parsed.IsActiveEntity === rootIsActiveEntity ? parsed : null
        }
      } else {
        // > No expand convert the result directly.
        const rawValue = entry[mapping]
        // Assume a DB will not return undefined, but always null
        row[key] = this._convertValue(rawValue, conversionMapper.get(mapping))

        isEntityNull = this._isNull(isEntityNull, rawValue)
      }
    }

    // No property holds any value. A to null must have failed.
    if (isEntityNull) {
      return
    }

    return row
  }

  /**
   * Check if row entry is null.
   *
   * @param {boolean} isEntityNull - previous state
   * @param {*} value - Value of row entry
   * @returns {boolean}
   * @private
   */
  _isNull(isEntityNull, value) {
    if (isEntityNull === undefined) {
      return value === null || value === undefined
    }

    return isEntityNull === true && (value === null || value === undefined)
  }

  /**
   * Helper to check if value needs to be converted and if yes, apply the conversion function.
   *
   * @param {string|number} value - As received from DB
   * @param {Function} converter - To something (Boolean, String, ...) conversion
   * @returns {*}
   * @private
   */
  _convertValue(value, converter) {
    if (converter) {
      return converter(value)
    }

    return value
  }

  _parseExpandResult(result, mappings, conversionMapper, toManyTree) {
    const resultCache = this._getResultCache(toManyTree)
    const expandMapping = this._getExpandMapping(mappings, toManyTree)

    for (const entry of result) {
      const parsed = this._parseRaw({ mappings: expandMapping, toManyTree, conversionMapper, entry })

      if (parsed) {
        const key = expandMapping[GET_KEY_VALUE](true, entry)

        if (!resultCache[key]) {
          resultCache[key] = []
        }

        resultCache[key].push(parsed)
      }
    }
  }

  _getResultCache(toManyTree) {
    let target = this._toManyResults

    for (const expand of toManyTree) {
      if (!target[expand]) {
        target[expand] = {
          [EXPAND]: {}
        }
      }

      target = target[expand]
    }

    return target
  }

  _getExpandMapping(mappings, toManyTree) {
    let mapping = mappings

    for (const key of toManyTree) {
      mapping = mapping[key]
    }

    return mapping
  }
}

/**
 * Convert N results into one expanded according to configurations.
 *
 * @param {Array} configs - Array of instructions how to combine and expand the 1 to N results
 * @param {Array} queries - Same amount of queries as configs. Contains one query for each to many expand.
 * @param {boolean} one - SELECT.one has been used
 * @param {object} rootEntity - the root entity
 * @returns {Promise<Array>} The complete expanded result set.
 */
const rawToExpanded = (configs, queries, one, rootEntity) => {
  return new RawToExpanded(configs, queries, one, rootEntity).toExpanded().catch(err => {
    Promise.all(queries).catch(() => {})
    throw err
  })
}

module.exports = rawToExpanded
