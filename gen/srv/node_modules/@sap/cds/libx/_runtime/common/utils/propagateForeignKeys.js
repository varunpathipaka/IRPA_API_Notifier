const cds = require('../../cds')

const _generateParentField = ({ parentElement }, row) => {
  if (_autoGenerate(parentElement) && !row[parentElement.name]) {
    row[parentElement.name] = cds.utils.uuid()
  }
}

const _generateChildField = ({ deep, childElement }, childRow) => {
  if (deep) {
    _generateChildField(deep.propagation, childRow[deep.targetName])
  } else if (_autoGenerate(childElement) && childRow && !childRow[childElement.name]) {
    childRow[childElement.name] = cds.utils.uuid()
  }
}

const _autoGenerate = e => e && e.type === 'cds.UUID' && e.key

const _getNestedVal = (row, prefix) => {
  let val = row
  const splitted = prefix.split('_')
  let k = ''

  while (splitted.length > 0) {
    k += splitted.shift()
    if (k in val) {
      val = val[k]
      k = ''
    } else {
      k += '_'
    }
  }

  return val
}

const _propagateToChid = ({ parentElement, childElement, prefix, parentFieldValue }, row, childRow) => {
  if (!childElement) return
  if (parentElement) {
    if (prefix) {
      const nested = _getNestedVal(row, prefix)
      childRow[childElement.name] = nested[parentElement.name]
    } else {
      childRow[childElement.name] = row[parentElement.name]
    }
  } else if (parentFieldValue !== undefined) {
    childRow[childElement.name] = parentFieldValue
  }
}

const _propagateToParent = ({ parentElement, childElement, deep }, childRow, row) => {
  if (deep) {
    _propagateToParent(deep.propagation, childRow[deep.targetName], childRow)
  }
  if (parentElement && childElement && childRow && Object.prototype.hasOwnProperty.call(childRow, childElement.name)) {
    row[parentElement.name] = childRow[childElement.name]
  }
}

const propagateForeignKeys = (tKey, row, foreignKeyPropagations, isCompositionEffective) => {
  const childRows = Array.isArray(row[tKey]) ? row[tKey] : [row[tKey]]

  for (const childRow of childRows) {
    if (!childRow) return

    for (const foreignKeyPropagation of foreignKeyPropagations) {
      if (foreignKeyPropagation.fillChild) {
        _generateParentField(foreignKeyPropagation, row)
        if (!isCompositionEffective) {
          delete row[tKey]
        } else {
          _propagateToChid(foreignKeyPropagation, row, childRow)
        }
      } else {
        _generateChildField(foreignKeyPropagation, childRow)
        _propagateToParent(foreignKeyPropagation, childRow, row)
      }
    }
  }
}

module.exports = {
  propagateForeignKeys
}
