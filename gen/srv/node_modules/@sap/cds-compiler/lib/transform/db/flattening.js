'use strict';

const {
  forEachDefinition, getUtils,
  applyTransformations, forAllElements, isBuiltinType,
} = require('../../model/csnUtils');
const transformUtils = require('../transformUtilsNew');
const { csnRefs } = require('../../model/csnRefs');
const { setProp } = require('../../base/model');

/**
 * Strip off leading $self from refs where applicable
 *
 * @param {CSN.Model} csn
 */
function removeLeadingSelf(csn) {
  const magicVars = [ '$now' ];
  forEachDefinition(csn, (artifact, artifactName) => {
    if (artifact.kind === 'entity' || artifact.kind === 'view') {
      forAllElements(artifact, artifactName, (parent, elements) => {
        for (const [ elementName, element ] of Object.entries(elements)) {
          if (element.on) {
            // applyTransformations expects the first thing to have a "definitions"
            const fakeDefinitions = { definitions: {} };
            fakeDefinitions.definitions[elementName] = element;
            applyTransformations( fakeDefinitions, {
              ref: (root, name, ref) => {
                // Renderers seem to expect it to not be there...
                if (ref[0] === '$self' && ref.length > 1 && !magicVars.includes(ref[1]))
                  root.ref = ref.slice(1);
              },
            });
          }
        }
      });
    }
  });
}

/**
 * Resolve type references and turn things with `.items` into elements of type `LargeString`.
 *
 * Also, replace actions, events and functions with simply dummy artifacts.
 *
 * @param {CSN.Model} csn
 * @param {CSN.Options} options
 * @param {WeakMap} resolved Cache for resolved refs
 * @param {string} pathDelimiter
 */
function resolveTypeReferences(csn, options, resolved, pathDelimiter) {
  /**
   * Remove .localized from the element and any sub-elements
   *
   * Only direct .localized usage should produce "localized things".
   * If we don't remove it here, the second compile step adds localized stuff again.
   *
   * @param {object} obj
   */
  function removeLocalized(obj) {
    const stack = [ obj ];
    while (stack.length > 0) {
      const current = stack.pop();
      if (current.localized)
        delete current.localized;


      if (current.elements)
        stack.push(...Object.values(current.elements));
    }
  }
  const { toFinalBaseType } = transformUtils.getTransformers(csn, options, pathDelimiter);
  applyTransformations(csn, {
    cast: (parent) => {
      // Resolve cast already - we otherwise lose .localized
      if (parent.cast.type && !isBuiltinType(parent.cast.type))
        toFinalBaseType(parent.cast, resolved, true);
    },
    type: (parent, prop, type) => {
      if (!isBuiltinType(type)) {
        const directLocalized = parent.localized || false;
        toFinalBaseType(parent, resolved);
        if (!directLocalized)
          removeLocalized(parent);
      }
      // HANA/SQLite do not support array-of - turn into CLOB/Text
      if (parent.items) {
        parent.type = 'cds.LargeString';
        delete parent.items;
      }
    },
    // HANA/SQLite do not support array-of - turn into CLOB/Text
    items: (parent) => {
      parent.type = 'cds.LargeString';
      delete parent.items;
    },
  }, [ (definitions, artifactName, artifact) => {
    // Replace events, actions and functions with simple dummies - they don't have effect on forHanaNew stuff
    // and that way they contain no references and don't hurt.
    if (artifact.kind === 'action' || artifact.kind === 'function' || artifact.kind === 'event') {
      const dummy = { kind: artifact.kind };
      if (artifact.$location)
        setProp(dummy, '$location', artifact.$location);

      definitions[artifactName] = dummy;
    }
  } ], true, { skipDict: { actions: true } });
}

/**
 * @param {CSN.Model} csn
 * @param {CSN.Options} options
 * @param {WeakMap} resolved Cache for resolved refs
 * @param {string} pathDelimiter
 */
function flattenAllStructStepsInRefs(csn, options, resolved, pathDelimiter) {
  const { inspectRef, effectiveType } = csnRefs(csn);
  const { flattenStructStepsInRef } = transformUtils.getTransformers(csn, options, pathDelimiter);
  const adaptRefs = [];

  /**
   * For each step of the links, check if there is a type reference.
   * If there is, resolve it and store the result in a WeakMap.
   *
   * @param {Array} [links=[]]
   * @todo seems too hacky
   * @returns {WeakMap} A WeakMap where a link is the key and the type is the value
   */
  function resolveLinkTypes(links = []) {
    const resolvedLinkTypes = new WeakMap();
    links.forEach((link) => {
      const { art } = link;
      if (art && art.type)
        resolvedLinkTypes.set(link, effectiveType(art));
    });

    return resolvedLinkTypes;
  }

  applyTransformations(csn, {
    ref: (parent, prop, ref, path) => {
      const { links, art, scope } = inspectRef(path);
      const resolvedLinkTypes = resolveLinkTypes(links);
      setProp(parent, '$path', [ ...path ]);
      const lastRef = ref[ref.length - 1];
      const fn = () => {
        const scopedPath = [ ...parent.$path ];

        parent.ref = flattenStructStepsInRef(ref, scopedPath, links, scope, resolvedLinkTypes );
        resolved.set(parent, { links, art, scope });
        // Explicitly set implicit alias for things that are now flattened - but only in columns
        // TODO: Can this be done elegantly during expand phase already?
        if (parent.$implicitAlias) { // an expanded s -> s.a is marked with this - do not add implicit alias "a" there, we want s_a
          if (parent.ref[parent.ref.length - 1] === parent.as) // for a simple s that was expanded - for s.substructure this would not apply
            delete parent.as;
          delete parent.$implicitAlias;
        }
        // To handle explicitly written s.a - add implicit alias a, since after flattening it would otherwise be s_a
        else if (parent.ref[parent.ref.length - 1] !== lastRef && (insideColumns(scopedPath) || insideKeys(scopedPath)) && !parent.as) {
          parent.as = lastRef;
        }
      };
      // adapt queries later
      adaptRefs.push(fn);
    },
  });

  adaptRefs.forEach(fn => fn());

  /**
   * Return true if the path points inside columns
   *
   * @param {CSN.Path} path
   * @returns {boolean}
   */
  function insideColumns(path) {
    return path.length >= 3 && (path[path.length - 3] === 'SELECT' || path[path.length - 3] === 'projection') && path[path.length - 2] === 'columns';
  }
  /**
   * Return true if the path points inside keys
   *
   * @param {CSN.Path} path
   * @returns {boolean}
   */
  function insideKeys(path) {
    return path.length >= 3 && path[path.length - 2] === 'keys' && typeof path[path.length - 1] === 'number';
  }
}

/**
 * @param {CSN.Model} csn
 * @param {CSN.Options} options
 * @param {string} pathDelimiter
 * @param {Function} error
 */
function flattenElements(csn, options, pathDelimiter, error) {
  const { isAssocOrComposition } = getUtils(csn);
  const { flattenStructuredElement } = transformUtils.getTransformers(csn, options, pathDelimiter);
  const { effectiveType } = csnRefs(csn);
  forEachDefinition(csn, flattenStructuredElements);
  /**
   * Flatten structures
   *
   * @param {CSN.Artifact} art Artifact
   * @param {string} artName Artifact Name
   */
  function flattenStructuredElements(art, artName) {
    forAllElements(art, artName, (parent, elements, pathToElements) => {
      const elementsArray = [];
      for (const elemName in elements) {
        const pathToElement = pathToElements.concat([ elemName ]);
        const elem = parent.elements[elemName];
        elementsArray.push([ elemName, elem ]);
        if (elem.elements) {
          elementsArray.pop();
          // Ignore the structured element, replace it by its flattened form
          // TODO: use $ignore - _ is for links
          elem._ignore = true;

          const branches = getBranches(elem, elemName);
          const flatElems = flattenStructuredElement(elem, elemName, [], pathToElement);

          for (const flatElemName in flatElems) {
            if (parent.elements[flatElemName])
              error(null, pathToElement, `"${artName}.${elemName}": Flattened struct element name conflicts with existing element: "${flatElemName}"`);

            const flatElement = flatElems[flatElemName];

            // Check if we have a valid notNull chain
            const branch = branches[flatElemName];
            if (flatElement.notNull !== false && !branch.some(s => !s.notNull))
              flatElement.notNull = true;


            if (flatElement.type && isAssocOrComposition(flatElement.type) && flatElement.on) {
              // Make refs resolvable by fixing the first ref step
              for (let i = 0; i < flatElement.on.length; i++) {
                const onPart = flatElement.on[i];
                if (onPart.ref) {
                  const firstRef = flatElement.on[i].ref[0];

                  /*
                      when element is defined in the current name resolution scope, like
                        entity E {
                          key x: Integer;
                              s : {
                              y : Integer;
                              a3 : association to E on a3.x = y;
                              }
                        }
                        We need to replace y with s_y and a3 with s_a3 - we must take care to not escape our local scope
                    */
                  const prefix = flatElement._flatElementNameWithDots.split('.').slice(0, -1).join(pathDelimiter);
                  const possibleFlatName = prefix + pathDelimiter + firstRef;

                  if (flatElems[possibleFlatName])
                    flatElement.on[i].ref[0] = possibleFlatName;
                }
              }
            }
            elementsArray.push([ flatElemName, flatElement ]);
            // Still add them - otherwise we might not detect collisions between generated elements.
            parent.elements[flatElemName] = flatElement;
          }
        }
      }
      //  Don't fake consistency of the model by adding empty elements {}
      if (elementsArray.length === 0)
        return;

      parent.elements = elementsArray.reduce((previous, [ name, element ]) => {
        previous[name] = element;
        return previous;
      }, Object.create(null));
    });
  }

  /**
   * Get not just the leafs, but all the branches of a structured element
   *
   * @param {object} element Structured element
   * @param {string} elementName Name of the structured element
   * @returns {object} Returns a dictionary, where the key is the flat name of the branch and the value is an array of element-steps.
   */
  function getBranches(element, elementName) {
    const branches = {};
    const subbranchNames = [];
    const subbranchElements = [];
    walkElements(element, elementName);
    /**
     * Walk the element chain
     *
     * @param {CSN.Element} e
     * @param {string} name
     */
    function walkElements(e, name) {
      if (isBuiltinType(e)) {
        branches[subbranchNames.concat(name).join(pathDelimiter)] = subbranchElements.concat(e);
      }
      else {
        const eType = effectiveType(e);
        const subelements = e.elements || eType.elements;
        if (subelements) {
          subbranchElements.push(e);
          subbranchNames.push(name);
          for (const [ subelementName, subelement ] of Object.entries(subelements))
            walkElements(subelement, subelementName);

          subbranchNames.pop();
          subbranchElements.pop();
        }
        else {
          branches[subbranchNames.concat(name).join(pathDelimiter)] = subbranchElements.concat(e);
        }
      }
    }
    return branches;
  }
}

module.exports = {
  resolveTypeReferences,
  flattenAllStructStepsInRefs,
  flattenElements,
  removeLeadingSelf,
};
