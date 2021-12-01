'use strict';

const { copyAnnotations } = require('../../model/csnUtils');
const { cloneCsn, forEachDefinition } = require('../../model/csnUtils');
const { attachPathOnPartialCSN } = require('./attachPath');

// These functions are used for propagation of the annotations, virtual, key,
// notNull attributes collected along the path during flattening.
const { addPropsForPropagationFromElement, propagatePropsToElement, resetPropsForPropagation } = function () {
  let toBePropagated = Object.create(null);
  return {
    addPropsForPropagationFromElement: function (element) {
      copyAnnotations(element, toBePropagated);
      if (element.virtual) toBePropagated.virtual = element.virtual;
      if (element.key) toBePropagated.key = element.key;
    },
    propagatePropsToElement: function (element) {
      copyAnnotations(toBePropagated, element);
      if (toBePropagated.virtual) element.virtual = toBePropagated.virtual;
      if (toBePropagated.key) element.key = toBePropagated.key;
    },
    resetPropsForPropagation: function () {
      toBePropagated = Object.create(null);
    }
  }
}();

// keep here the state of the 'notNull' attribute
// this is needed because during flattening all the elements
// along the chain need to be assigned with not null so
// the resulting element to be not null as well
const { isNotNull, setNotNull, setUpNotNull } = function () {
  let notNull = undefined;
  return {
    isNotNull: function () {
      return notNull;
    },
    setNotNull: function (value) {
      notNull = value;
    },
    setUpNotNull: function (element, isParentNotNull) {
      if (isParentNotNull && element.notNull) setNotNull(element.notNull);
      else if (isNotNull() && !element.notNull || (isNotNull() === false && element.notNull !== false)) setNotNull(undefined);
    }
  }
}();

/**
 * During the OData transformations in flat-mode, all structured elements will be flattened.
 * This module performs the complete flattening.
 * It also provides information to the reference flattener: elements produced for specific path in the CSN structure.
 * @param {CSN.Model} csn CSN-object to flatten
 * @param {*} csnUtils instances of utility functions
 * @param {*} options
 * @param {*} referenceFlattener
 * @param {Function} error
 * @param {Function} isExternalServiceMember returns true for an artifact that is part of an external service
 */
function flattenCSN(csn, csnUtils, options, referenceFlattener, error, isExternalServiceMember) {
  forEachDefinition(csn, (def, defName, propertyName, path) =>
    flattenDefinition(def, path, csnUtils, options, referenceFlattener, error), { skipArtifact: isExternalServiceMember });
}

/**
 * Flattens one single definition and all structures in it. Modifies the definition in place.
 * @param {CSN.Definition} definition definition object to flatten
 * @param {CSN.Path} definitionPath path in CSN object
 * @param {*} csnUtils utility functions
 * @param {*} options
 * @param {*} referenceFlattener
 * @param {Function} error
 */
function flattenDefinition(definition, definitionPath, csnUtils, options, referenceFlattener, error) {
  if (definition.kind !== 'entity' && definition.kind !== 'view')
    return;

  let { newFlatElements } = flattenStructure(definition.elements, definitionPath, csnUtils, options, error, referenceFlattener);

  attachPathOnPartialCSN(newFlatElements, definitionPath.concat('elements'));
  definition.elements = newFlatElements;

  if (definition.params) {
    let { newFlatElements } = flattenStructure(definition.params, definitionPath, csnUtils, options, error, referenceFlattener);
    attachPathOnPartialCSN(newFlatElements, definitionPath.concat('params'));
    definition.params = newFlatElements;
  }
} // flattenDefinition

/**
 * Flattens structured element by calling element flattener for each structured child.
 * Returns a dictionary containing all the new elements for the given structure.
 * @param {*} dictionary to flatten
 * @param {CSN.Path} path the path of the structure in the CSN tree
 * @param {*} csnUtils
 * @param {Function} error Error message function
 * @param {*} [referenceFlattener]
 * @param {string[]} [elementPathInStructure] list of parent element names
 * @param {*} [newFlatElements]
 * @param {boolean} [isTopLevelElement] states if this is a top level element
 */
function flattenStructure(dictionary, path, csnUtils, options, error, referenceFlattener = undefined, elementPathInStructure = [],
  newFlatElements = Object.create(null), isTopLevelElement = true, isParentNotNull = false) {

  if (!isTopLevelElement) addPropsForPropagationFromElement(dictionary);

  let generatedNewFlatElementsNames = []; // holds the names of all new child elements of the structure

  dictionary && Object.entries(dictionary).forEach(([elementName, element]) => {
    let currPath = path.concat('elements', elementName);

    if (isTopLevelElement) {
      resetPropsForPropagation();
      setNotNull(element.notNull)
    } else {
      setUpNotNull(element, isParentNotNull);
    }

    // flat elements when structured and NOT empty (allow incomplete structures - cds-compiler#4337)
    if (csnUtils.isStructured(element) && !(element.elements && Object.keys(element.elements).length === 0)) {

      if (referenceFlattener) referenceFlattener.registerFlattenedElement(currPath, element.$path);

      addPropsForPropagationFromElement(element);

      // if the child element is structured itself -> needs to be flattened
      const elements = element.elements || csnUtils.getFinalBaseType(element.type).elements;
      let result = flattenStructure(elements, currPath, csnUtils, options, error, referenceFlattener, elementPathInStructure.concat(elementName), newFlatElements, false, isNotNull());
      generatedNewFlatElementsNames.push(...result.generatedNewFlatElementsNames); // accomulate names of produced elements

    } else { // when we do not need to flat, this is scalar or empty (cds-compiler#4337) -> needs to be registered in referenceFlattener
      let newElementName = elementPathInStructure.concat(elementName).join('_');
      let elementNameWithDots = elementPathInStructure.concat(elementName).join('.');
      addNewElementToResult(element, newElementName, elementNameWithDots, currPath);
    }
  });

  if (referenceFlattener) {
    referenceFlattener.registerGeneratedElementsForPath(path, generatedNewFlatElementsNames);
  }
  return { newFlatElements, generatedNewFlatElementsNames };

  // adds newly created element into the final dictionary of elements
  function addNewElementToResult(element, elementName, elementNameWithDots, path) {
    if (newFlatElements[elementName]) {
      error(null, path, `Generated element ${elementName} conflicts with other generated element`);
    } else {
      let newPath = path.slice(0, 2).concat('elements', elementName);
      let newElement = createNewElement(element, elementNameWithDots, newPath);
      newFlatElements[elementName] = newElement;
      generatedNewFlatElementsNames.push(elementName);

      if (referenceFlattener) {
        referenceFlattener.registerElementTransition(path, newPath);
      }
    }
  } // addNewElementToResult

  // creates new element by copying the properties of the originating element
  function createNewElement(element, elementNameWithDots, path) {
    let newElement = cloneCsn(element, options);
    if (!isTopLevelElement) propagatePropsToElement(newElement);
    if (isNotNull() === undefined) delete newElement.notNull;
    if (!isTopLevelElement && referenceFlattener) {
      referenceFlattener.setElementNameWithDots(path, elementNameWithDots);
    }
    return newElement;
  } // createNewElement

} // flattenStructure

module.exports = { flattenCSN, flattenStructure };
