'use strict';

const { forEachManagedAssociation } = require('./utils');
const { attachPath, attachPathOnPartialCSN } = require('./attachPath');

/**
 * This module runs through the model and for each managed association in it,
 * in case the foreign keys are structured, it is expanding them. Example:
 * entity A {
 *  toB: association to B { stru };
 * } // -> CSN: keys:[ { ref:['stru'] } ]
 *
 * entity B {
 *  stru: {
 *    subid: Integer;
 *  }
 * }
 * after expand -> keys:[ { ref: ['stru_subid'] } ]
 */
module.exports = function (csn, referenceFlattener, csnUtils, isExternalServiceMember) {

  forEachManagedAssociation(csn, (element) => {
    if (element.keys) {
      expandStructuredKeysForAssociation(element, referenceFlattener);
    }
  }, isExternalServiceMember);

  // update paths and resolve references
  attachPath(csn);
  referenceFlattener.resolveAllReferences(csn, csnUtils.inspectRef, csnUtils.isStructured);

  function expandStructuredKeysForAssociation(assoc, referenceFlattener) {
    let newKeys = [];
    for (let key of assoc.keys) {
      // when are assigned $paths and when not???
      let paths = key.$paths;
      if (paths) {
        let lastPath = paths[paths.length - 1];
        let generatedElements = referenceFlattener.getGeneratedElementsForPath(lastPath);
        if (generatedElements) {
          generatedElements.forEach(elementName => {
            let newRef = { ref: [elementName] };
            if (key.as) {
              newRef.as = elementName.replace(key.ref[0], key.as);
            }
            newKeys.push(newRef);
          })
          continue;
        }
      }
      newKeys.push(key);
    }

    if (newKeys.length) {
      attachPathOnPartialCSN(newKeys, assoc.$path.concat('keys'));
      assoc.keys = newKeys;
    }
  }
}
