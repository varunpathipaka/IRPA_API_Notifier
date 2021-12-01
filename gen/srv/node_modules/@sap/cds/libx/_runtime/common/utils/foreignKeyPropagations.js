const _getSubOns = element => {
  // this only works for on conds with `and`, once we support `or` this needs to be adjusted
  const newOn = element.on ? element.on.filter(e => e !== '(' && e !== ')') : []
  const subOns = []
  let currArr = []

  for (const onEl of newOn) {
    if (currArr.length === 0) subOns.push(currArr)
    if (onEl !== 'and') currArr.push(onEl)
    else {
      currArr = []
    }
  }

  for (const subOn of subOns) {
    // We don't support anything else than
    // A = B AND C = D AND ...
    if (subOn.length !== 3) return []
  }

  return subOns
}

const _parentFieldsFromSimpleOnCond = (element, subOn) => {
  const idxChildField = subOn.findIndex(o => o.ref && o.ref[0] === element.name)
  if (idxChildField === -1 || subOn[1] !== '=') return

  const childFieldName = subOn[idxChildField].ref && subOn[idxChildField].ref.slice(1).join('_')
  const childElement = element._target.elements[childFieldName]
  const idxParentField = idxChildField === 2 ? 0 : 2
  let parentRef = Array.isArray(subOn[idxParentField].ref) && [...subOn[idxParentField].ref]

  if (parentRef && parentRef.length > 1) {
    const idxChildInParent = parentRef.findIndex(e => e === element.name)
    if (idxChildInParent > -1) parentRef.splice(idxChildInParent, 1)
    parentRef = [parentRef.join('_')]
  }
  const parentElement = parentRef && element.parent.elements[parentRef[0]]

  if (!childElement) {
    // update on view with key in parent
    return [{ fillChild: false, parentElement, childElement }]
  }

  if (!childElement.on) {
    if (element._isSelfManaged) return _foreignKeyPropagationsFromToManyOn(element, childFieldName)
    if (parentElement) return [{ fillChild: true, parentElement, childElement }]
  }

  if ('val' in subOn[idxParentField]) {
    return [{ fillChild: true, parentFieldValue: subOn[idxParentField].val, childElement }]
  }

  if (childElement._isAssociationStrict && childElement.on) {
    return _foreignKeyPropagationsFromCustomBacklink(element, childElement)
  }
}

const _foreignKeyPropagationsFromToManyOn = (element, childFieldName) => {
  const foreignKeys = _foreignKeysForTarget(element, childFieldName)
  // REVISIT foreignKeys is empty if we have deep operations where a sub element is annotated with persistence skip
  if (foreignKeys && foreignKeys.length) {
    const parentKeys = _parentKeys(element)
    return _resolvedKeys(parentKeys, foreignKeys, true)
  }
  return []
}

const _foreignKeyPropagationsFromCustomBacklink = (element, childElement) => {
  const foreignKeyPropagations = []
  const subOns = _getSubOns(childElement)

  for (const subOn of subOns) {
    if (subOn[1] === '=') {
      const parentFieldIdx = subOn.findIndex(o => o.ref && o.ref[0] === childElement.name)
      const otherFieldIdx = parentFieldIdx === 0 ? 2 : 0
      const otherField = subOn[otherFieldIdx]

      if (parentFieldIdx === -1 && subOn[otherFieldIdx === 0 ? 2 : 0].val !== undefined) {
        const parentField = subOn[otherFieldIdx === 0 ? 2 : 0]
        foreignKeyPropagations.push({
          fillChild: false,
          parentFieldValue: parentField.val,
          childElement: element._target.elements[otherField.ref[0]]
        })
      } else if (otherField.ref && otherField.ref.length === 1) {
        const parentFieldName = subOn[parentFieldIdx].ref[1]
        foreignKeyPropagations.push({
          fillChild: true,
          parentElement: element.parent.elements[parentFieldName],
          childElement: element._target.elements[otherField.ref[0]]
        })
      } else if (otherField.val !== undefined) {
        const parentFieldName = subOn[parentFieldIdx] && subOn[parentFieldIdx].ref[1]
        const parentField = subOn[otherFieldIdx === 2 ? 0 : 2]
        foreignKeyPropagations.push({
          fillChild: true,
          parentElement: element.parent.elements[parentFieldName],
          parentFieldValue: parentField.val,
          childFieldValue: otherField.val
        })
      }
    }
  }

  return foreignKeyPropagations
}

const _foreignKeyPropagationsFromOn = element => {
  const subOns = _getSubOns(element)
  const foreignKeyPropagations = []

  for (const subOn of subOns) {
    const subParentFields = _parentFieldsFromSimpleOnCond(element, subOn)
    if (subParentFields) foreignKeyPropagations.push(...subParentFields)
  }

  return foreignKeyPropagations
}

/*
 * recursive resolvedKeys for a structured element
 * returns how many indexes can be skipped in the outer loop
 * example:
 *   foo: {
 *     bar: {
 *       moo: Integer;
 *       shu: Integer;
 *     };
 *     baz: Integer;
 *   };
 *   -> foo_bar_moo, foo_bar_shu, foo_baz
 *   -> processed three instead of one from outer loop perspective
 */
const _resolve4struct = (others, struct, fkps, fillChild, childIsStruct, i) => {
  let j = 0

  for (const k in struct.elements) {
    const other = others[i + j]
    const current = struct.elements[k]

    if (current._isStructured) {
      // call recursive and increment skip
      j += _resolve4struct(others, current, fkps, fillChild, childIsStruct, i + j)
    } else if (current.isAssociation) {
      continue
    } else {
      // calc prefix
      let prefix = struct.name
      let cur = struct.parent
      while (cur._isStructured) {
        prefix = cur.name + '_' + prefix
        cur = cur.parent
      }

      // push propagation
      fkps.push({
        childElement: childIsStruct ? current : other,
        parentElement: childIsStruct ? other : current,
        fillChild,
        prefix,
        deep: !fillChild && _resolveTargetForeignKey(childIsStruct ? current : other)
      })

      // increment skip
      j++
    }
  }

  return j
}

const _resolveTargetForeignKey = targetKey => {
  const targetName = targetKey['@odata.foreignKey4']
  if (!targetName) return
  const _foreignKeyProps = foreignKeyPropagations(targetKey.parent.elements[targetName])
  const propagation = _foreignKeyProps.find(_fkp => _fkp.parentElement && targetKey.name === _fkp.parentElement.name)
  return { targetName, propagation }
}

const _resolvedKeys = (foreignKeys, targetKeys, fillChild) => {
  const foreignKeyPropagations = []

  for (let i = 0; i < foreignKeys.length; i++) {
    const fk = foreignKeys[i]
    const tk = targetKeys[i]
    if (fk._isStructured) {
      i += _resolve4struct(targetKeys, fk, foreignKeyPropagations, fillChild, false, i)
    } else if (tk._isStructured) {
      i += _resolve4struct(foreignKeys, tk, foreignKeyPropagations, fillChild, true, i)
    } else {
      foreignKeyPropagations.push({
        fillChild,
        parentElement: fk,
        childElement: tk,
        // needed only for child -> parent propagation since template loops in other direction
        deep: !fillChild && _resolveTargetForeignKey(tk)
      })
    }
  }

  return foreignKeyPropagations
}

const foreignKeyPropagations = element => {
  if (element.is2many && element.on) {
    return _foreignKeyPropagationsFromOn(element)
  }
  if (element.is2one) {
    if (!element.on) {
      const foreignKeys = _foreignKeys(element)
      if (foreignKeys) {
        const targetKeys = _targetKeys(element)
        return _resolvedKeys(foreignKeys, targetKeys)
      }
    } else {
      // It's a link through a backlink
      return _foreignKeyPropagationsFromOn(element)
    }
  }
  return []
}

const _foreignKeys = csnElement => {
  return Object.values(csnElement.parent.elements).filter(element => element['@odata.foreignKey4'] === csnElement.name)
}

const _foreignKeysForTarget = (csnElement, name) => {
  return Object.values(csnElement._target.elements).filter(
    element =>
      !element.isAssociation && !element.isComposition && element['@odata.foreignKey4'] === (name || csnElement.name)
  )
}

const _targetKeys = csnElement => {
  return Object.values(csnElement._target.keys).filter(
    element => !element.isAssociation && !element.isComposition && element.name !== 'IsActiveEntity'
  )
}

const _parentKeys = csnElement => {
  return Object.values(csnElement.parent.keys).filter(
    element => !element.isAssociation && !element.isComposition && element.name !== 'IsActiveEntity'
  )
}

module.exports = {
  foreignKeyPropagations
}
