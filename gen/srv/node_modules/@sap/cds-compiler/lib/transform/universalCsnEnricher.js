'use strict';

const { forEachDefinition } = require('../base/model');
const {
  applyTransformations,
  cloneCsn,
  getUtils,
  isBuiltinType,
} = require('../model/csnUtils');

/**
 * Loop through a universal CSN and enrich it with the properties
 * from the source definition - modifies the input model in-place
 * 
 * @param {CSN.Model} csn
 * @param {CSN.Options} options
 */
module.exports = function(csn, options) {
  let { getOrigin, getFinalType, getFinalTypeDef } = getUtils(csn);
  // User-defined structured types do not have the elements propagated any longer
  // if there is no association among the elements. For that reason,
  // as a first step propagate the elements of these
  forEachDefinition(csn, (def) => {
    if (def.kind === 'type' && def.type && !def.elements) {
      const finalType = getFinalType(def.type);
      if (isBuiltinType(finalType)) return;
      const finalTypeDef = getFinalTypeDef(def.type);
      if (finalTypeDef.elements)
        def.elements = cloneCsn(finalTypeDef.elements, options);
    }
  });

  // as a second step, loop through all the $origin properties in the model
  // and propagate the properties from the origin definition
  applyTransformations(csn, {
    '$origin': (node, _$orign, $originValue, _path, parent, propName) => {
      if (!node.kind) { // we do not want to replace whole definitions
        if (Array.isArray($originValue))
          propagatePropsFromOrigin(node, propName, parent);
        else if ($originValue.$origin && Array.isArray($originValue.$origin)) {
          // cover the case of query entity elements where we have own and ihnerited attributes/annotations
          propagatePropsFromOrigin($originValue, propName, parent);
        }

      }
    }
  }, undefined, undefined, options);

  function propagatePropsFromOrigin(member, memberName, construct) {
    // TODO: shall the $origin be kept as part of the element?
    const origin = getOrigin(member);
    if (origin.kind) return;
    if (member.elements && origin.type) {
      delete member.$origin;
      member.type = origin.type;
      return;
    }
    let newMember = cloneCsn(origin, options);
    // keep targets and keys of assoc, if it was redirected
    if (origin.type === 'cds.Association') {
      newMember.target = member.target || newMember.target;
      newMember.keys = member.keys || newMember.keys;
    }
    // TODO: check if this works fine for items/returns/actions
    construct[memberName] = newMember;
  }
}
