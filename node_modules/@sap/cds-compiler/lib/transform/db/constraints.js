'use strict';

const { forEachDefinition } = require('../../base/model');
const { forAllElements, hasAnnotationValue, getResultingName } = require('../../model/csnUtils');
const { csnRefs } = require('../../model/csnRefs');

const COMPOSITION = 'cds.Composition';
const ASSOCIATION = 'cds.Association';
/**
 * Create referential constraints for foreign keys mentioned in on-conditions of associations and compositions.
 * The referential constraints will be attached to the csn.Artifacts.
 *
 * @param {CSN.Model} csn
 * @param {CSN.Options} options are used to modify the validate / enforced flag on the constraints
 */
function createReferentialConstraints(csn, options) {
  let validated = true;
  let enforced = true;
  if (options.constraintsNotValidated)
    validated = false;

  if (options.constraintsNotEnforced)
    enforced = false;

  const { inspectRef } = csnRefs(csn);
  // prepare the functions with the compositions and associations across all entities first
  // and execute it afterwards because compositions must be processed first
  const compositions = [];
  const associations = [];
  forEachDefinition(csn, (artifact, artifactName) => {
    if (!artifact.query && artifact.kind === 'entity' ) {
      forAllElements(artifact, artifactName, (parent, elements, path) => {
        // Step I: iterate compositions, enrich dependent keys (in target entity)
        for (const elementName in elements) {
          const element = elements[elementName];
          if (element.type === COMPOSITION && !treatCompositionLikeAssociation(element)) {
            compositions.push(() => {
              foreignKeyConstraintForComposition(element, parent, path.concat([ elementName ]));
            });
          }
        }
        // Step II: iterate associations, enrich dependent keys (in entity containing the association)
        for (const elementName in elements) {
          const element = elements[elementName];
          if (element.type === ASSOCIATION ||
             element.type === COMPOSITION && treatCompositionLikeAssociation(element)) {
            associations.push(() => {
              foreignKeyConstraintForAssociation(element, elements, path.concat([ elementName ]), elementName);
            });
          }
        }
      });
    }
  });
  // create constraints on foreign keys
  compositions.forEach(fn => fn());
  associations.forEach(fn => fn());
  // Step III: Create the final referential constraints from all dependent key <-> parent key pairs stemming from the same $sourceAssociation
  forEachDefinition(csn, collectAndAttachReferentialConstraints);


  /**
   * Calculate referential constraints for dependent keys in target entity of cds.Composition.
   * The DELETE rule for a referential constraint stemming from a composition will be CASCADE.
   * A managed composition with a target cardinality of one, will be treated like a regular Association.
   *
   * @param {CSN.Element} composition for that a constraint should be generated
   * @param {CSN.Artifact} parent artifact containing the composition
   * @param {CSN.Path} path
   */
  function foreignKeyConstraintForComposition(composition, parent, path) {
    if (skipConstraintGeneration(parent, composition))
      return;

    const { elements } = parent;
    const onCondition = composition.on;
    if (onCondition) {
      if (hasConstraintCompliantOnCondition(composition, elements, path)) {
        // 1. cds.Composition has constraint compliant on-condition
        // mark each dependent key referenced in the on-condition (in target entity)
        const dependentKeys = Array.from(elementsOfTargetSide(onCondition, csn.definitions[composition.target].elements));
        const parentKeys = Array.from(elementsOfSourceSide(onCondition, elements));
        const { backlinkName } = composition.$selfOnCondition || {};
        // sanity check; do not generate constraints for on-conditions like "dependent.idOne = id AND dependent.idTwo = id"
        // also: no constraints for compositions of many w/o backlink
        if (dependentKeys.length === parentKeys.length && backlinkName)
          attachConstraintsToDependentKeys(dependentKeys, parentKeys, path[path.length - 3], path, backlinkName, 'CASCADE');
      }
    }
    else if (!onCondition && composition.keys.length > 0) {
      throw new Error('Please debug me, an on-condition was expected here, but only found keys');
    }
  }

  /**
   * Calculate referential constraints for dependent keys in the entity where the cds.Associations is defined.
   * The DELETE rule for a referential constraint stemming from a cds.Association will be 'RESTRICT'
   *
   * @param {CSN.Association} association for that a constraint should be generated
   * @param {CSN.Elements} elements of parent entity.
   * @param {CSN.Path} path
   * @param {string} assocName passed through as proper constraint suffix
   */
  function foreignKeyConstraintForAssociation(association, elements, path, assocName) {
    const associationTarget = csn.definitions[association.target];
    if (skipConstraintGeneration(associationTarget, association))
      return;

    const onCondition = association.on;
    if (onCondition && hasConstraintCompliantOnCondition(association, elements, path)) {
      // 1. cds.Association has constraint compliant on-condition
      // mark each dependent key - in the entity containing the association - referenced in the on-condition
      const dependentKeys = Array.from(elementsOfSourceSide(onCondition, elements));
      const parentKeys = Array.from(elementsOfTargetSide(onCondition, associationTarget.elements));
      // sanity check; do not generate constraints for on-conditions like "dependent.idOne = id AND dependent.idTwo = id"
      if (dependentKeys.length === parentKeys.length)
        attachConstraintsToDependentKeys(dependentKeys, parentKeys, association.target, path, assocName);
    }
    else if (!onCondition && association.keys.length > 0) {
      throw new Error('Please debug me, an on-condition was expected here, but only found keys');
    }
  }

  /**
   * Attach constraints to individual foreign key elements
   * The $foreignKeyConstraint property will later be collected from the foreign keys
   * and attached to the $tableConstraints property of the respective entity.
   *
   * @param {Array} dependentKeys array holding dependent keys in the format [['key1', 'value1'], [...], ...]
   * @param {Array} parentKeys array holding parent keys in the format [['key1', 'value1'], [...], ...]
   * @param {CSN.PathSegment} parentTable the sql-table where the foreign key constraints will be pointing to
   * @param {CSN.Path} path
   * @param {string | null} constraintIdentifierSuffix name of the association / the backlink association
   * @param {string} onDelete the on delete rule which should be applied. Default for associations is 'RESTRICT'
   */
  function attachConstraintsToDependentKeys(dependentKeys, parentKeys, parentTable, path, constraintIdentifierSuffix, onDelete = 'RESTRICT') {
    while (dependentKeys.length > 0) {
      const dependentKeyValuePair = dependentKeys.pop();
      const dependentKey = dependentKeyValuePair[1];
      if (Object.prototype.hasOwnProperty.call(dependentKey, '$foreignKeyConstraint'))
        return;

      const parentKeyValuePair = parentKeys.pop();
      const parentKeyName = parentKeyValuePair[0];

      const constraint = {
        parentKey: parentKeyName,
        parentTable,
        sourceAssociation: path[path.length - 1],
        nameSuffix: constraintIdentifierSuffix || 'up_',
        onDelete,
        validated,
        enforced,
      };
      dependentKey.$foreignKeyConstraint = constraint;
    }
  }

  /**
   * Constraints can only be generated if the full primary key of the target is referenced by the foreign key in an on-condition.
   * 1. on-condition only contains AND as logical operator
   * 2. each part of the on-condition must either:
   *    - reference a valid field in the dependent entity:
   *      a) for cds.Composition this is in the target entity
   *      b) for cds.Association this is the entity, where the association is defined
   *    - reference a key element in the parent entity:
   *      a) for cds.Composition this is the entity, where the composition itself is defined
   *      b) for cds.Association this is the target entity
   * 3. parent keys must be the full primary key tuple
   *
   * @param {CSN.Association | CSN.Composition} element
   * @param {CSN.Elements} siblingElements
   * @param {CSN.Path} path the path to the element
   * @returns {boolean} indicating whether the association / composition is a constraint candidate
   */
  function hasConstraintCompliantOnCondition(element, siblingElements, path) {
    const onCondition = element.on;
    const allowedTokens = [ '=', 'and', '(', ')' ];
    // on condition must only contain logical operator 'AND'
    if (onCondition.some(step => typeof step === 'string' && !allowedTokens.includes(step)))
      return false;

    // on-condition like ... TemplateAuthGroupAssignments.isTemplate = true; is not allowed
    if (onCondition.some(step => typeof step === 'object' && Object.prototype.hasOwnProperty.call(step, 'val')))
      return false;

    // no magic vars in on-condition
    // e.g. for localized: ... and localized.locale = $user.locale; -> not a valid on-condition
    if (onCondition.some((step, index) => typeof step === 'object' && inspectRef(path.concat([ 'on', index ])).scope === '$magic'))
      return false;

    // managed composition with target cardinality of one is treated like an association
    const isComposition = element.type === COMPOSITION && !treatCompositionLikeAssociation(element);
    // for cds.Associations the parent keys are in the associations target entity
    // for cds.Composition the parent keys are in the entity, where the composition is defined
    const parentElements = isComposition ? siblingElements : csn.definitions[element.target].elements;
    const parentKeys = isComposition ? elementsOfSourceSide(onCondition, parentElements) : elementsOfTargetSide(onCondition, parentElements);
    // returns true if the parentKeys found in the on-condition are covering the full primary key tuple in the parent entity
    return Array.from(parentKeys.entries())
    // check if primary key found in on-condition is present in association target / composition source
      .filter(([ keyName, pk ]) => pk.key && parentElements[keyName].key).length === Object.keys(parentElements)
    // compare that with the length of the primary key tuple found in association target / composition source
      .filter(key => parentElements[key].key &&
      parentElements[key].type !== ASSOCIATION &&
      parentElements[key].type !== COMPOSITION)
      .length;
  }

  /**
   *  Skip referential constraint if the parent table (association target, or artifact where composition is defined)
   *  of the relation is:
   *    - a query
   *    - annotated with '@cds.persistence.skip:true'
   *    - annotated with '@cds.persistence.exists:true'
   *
   *  Skip referential constraint as well if:
   *    - global option 'skipDbConstraints' is set and if the element is not annotated with
   *       '@cds.persistency.assert.integrity: true'.
   *    - the element is annotated either with '@cds.persistency.assert.integrity: false' or '@assert.integrity: false'
   *
   * @param {CSN.Element} parent of association / composition
   * @param {CSN.Element} element the composition or association
   * @returns {boolean}
   */
  function skipConstraintGeneration(parent, element) {
    if (hasAnnotationValue(element, '@assert.integrity', false) ||
     hasAnnotationValue(element, '@cds.persistency.assert.integrity', false)) {
    // in case of managed composition, the 'up_' link should not result in a constraint
      const target = csn.definitions[element.target];
      const { up_ } = target.elements;
      if (up_)
        up_.$skipReferentialConstraintForUp_ = true;
      return true;
    }

    if (element.$skipReferentialConstraintForUp_) {
      delete element.$skipReferentialConstraintForUp_;
      return true;
    }

    if (hasAnnotationValue(parent, '@cds.persistence.skip', true) ||
     hasAnnotationValue(parent, '@cds.persistence.exists', true) ||
     parent.query)
      return true;

    // '@cds.persistency.assert.integrity: true' supersedes global switch
    if (!hasAnnotationValue(element, '@cds.persistency.assert.integrity', true) && options.forHana.skipDbConstraints)
      return true;

    return false;
  }

  /**
   * Finds and returns elementNames and elements of target side mentioned in on-condition.
   *
   * @param {CSN.OnCondition} on
   * @param {CSN.Elements} targetElements elements of association/composition target entity
   * @returns {Map} of target elements with their name as key
   */
  function elementsOfTargetSide(on, targetElements) {
    const elements = new Map();
    on.filter(element => typeof element === 'object' &&
        element.ref.length > 1 &&
        targetElements[element.ref[element.ref.length - 1]])
      .forEach((element) => {
        elements.set(element.ref[element.ref.length - 1], targetElements[element.ref[element.ref.length - 1]]);
      });

    return elements;
  }

  /**
   * Finds and return elementNames and elements of source side mentioned in on-condition.
   *
   * @param {CSN.Association.on} on the on-condition
   * @param {CSN.Elements} sourceElements elements of source entity where the association/composition is defined.
   * @returns {Map} of source elements with their name as key
   */
  function elementsOfSourceSide(on, sourceElements) {
    const elements = new Map();
    on.filter(element => typeof element === 'object' &&
          element.ref.length === 1 &&
          sourceElements[element.ref[0]])
      .forEach((element) => {
        elements.set(element.ref[0], sourceElements[element.ref[0]]);
      });
    return elements;
  }

  /**
   * Creates the final referential constraints from all dependent key <-> parent key pairs stemming from the same $sourceAssociation
   * and attaches it to the given artifact.
   *
   * Go over all elements with $foreignKeyConstraint property:
   *  - Find all other elements in artifact with the same $sourceAssociation
   *  - Create constraints with the information supplied by $parentKey, $parentTable and $onDelete
   *
   * @param {CSN.Artifact} artifact
   * @param {string} artifactName
   */
  function collectAndAttachReferentialConstraints(artifact, artifactName) {
    const referentialConstraints = Object.create(null);
    for (const elementName in artifact.elements) {
      const element = artifact.elements[elementName];
      if (!element.$foreignKeyConstraint)
        continue;
      // copy constraint property, and delete it from the element
      const $foreignKeyConstraint = Object.assign({}, element.$foreignKeyConstraint);
      delete element.$foreignKeyConstraint;
      const { parentTable } = $foreignKeyConstraint;
      const parentKey = [ $foreignKeyConstraint.parentKey ];
      const dependentKey = [ elementName ];
      const onDeleteRules = new Set();
      onDeleteRules.add($foreignKeyConstraint.onDelete);
      // find all other $foreignKeyConstraint with same $sourceAssociation and same parentTable
      Object.entries(artifact.elements)
        .filter(([ , e ]) => e.$foreignKeyConstraint &&
            e.$foreignKeyConstraint.sourceAssociation === $foreignKeyConstraint.sourceAssociation &&
            e.$foreignKeyConstraint.parentTable === $foreignKeyConstraint.parentTable)
        .forEach(([ foreignKeyName, foreignKey ]) => {
          const $foreignKeyConstraintCopy = Object.assign({}, foreignKey.$foreignKeyConstraint);
          delete foreignKey.$foreignKeyConstraint;
          parentKey.push($foreignKeyConstraintCopy.parentKey);
          dependentKey.push(foreignKeyName);
          onDeleteRules.add($foreignKeyConstraintCopy.onDelete);
        });
      // onDelete Rule is the "weakest" rule applicable. Precedence: RESTRICT > SET NULL > CASCADE
      const onDelete = onDeleteRules.has('RESTRICT') ? 'RESTRICT' : 'CASCADE';
      let onDeleteRemark = null;
      // comments in sqlite files are causing the JDBC driver to throw an error on deployment
      if (options.testMode && onDelete === 'CASCADE')
        onDeleteRemark = `Composition "${$foreignKeyConstraint.sourceAssociation}" implies existential dependency`;
      referentialConstraints[`${getResultingName(csn, 'quoted', artifactName)}_${$foreignKeyConstraint.nameSuffix}`] = {
        identifier: `${getResultingName(csn, options.forHana.names, artifactName)}_${$foreignKeyConstraint.nameSuffix}`,
        foreignKey: dependentKey,
        parentKey,
        dependentTable: artifactName,
        parentTable,
        onDelete,
        onDeleteRemark, // explain why this particular rule is chosen
        // TODO: do we want to switch off validation / enforcement via annotation on association?
        validated: $foreignKeyConstraint.validated,
        enforced: $foreignKeyConstraint.enforced,
      };
    }
    if (Object.keys(referentialConstraints).length) {
      if (!('$tableConstraints' in artifact))
        artifact.$tableConstraints = Object.create(null);

      artifact.$tableConstraints.referential = referentialConstraints;
    }
  }

  /**
   * If we have a managed composition with a target cardinality of one, we will treat it like
   * a regular association when it comes to referential constraints.
   * The constraints will thus be generated in the entity containing the composition and not in the target entity.
   *
   * @param {CSN.Composition} composition the composition which might be treated like an association
   * @returns {boolean} true if the composition should be treated as an association in regards to foreign key constraints
   */
  function treatCompositionLikeAssociation(composition) {
    return Boolean((isToOne(composition) && !composition.$selfOnCondition) || composition.keys);
  }

  /**
   * returns true if the association/composition has a max target cardinality of one
   *
   * @param {CSN.Element} assocOrComposition
   * @returns {boolean}
   */
  function isToOne(assocOrComposition) {
    const { min, max } = assocOrComposition.cardinality || {};
    return !min && !max || max === 1;
  }
}

/**
 * If the artifact has both, unique- and foreign key constraints, it is possible that the constraints have the same identifier.
 * This would end in table which can't be activated.
 *
 * @param {CSN.Artifact} artifact
 * @param {string} artifactName
 * @param {CSN.Path} path
 * @param {Function} error
 */
function assertConstraintIdentifierUniqueness(artifact, artifactName, path, error) {
  // can only happen if referential & unique constraints are present
  if (!(artifact.$tableConstraints && artifact.$tableConstraints.referential && artifact.$tableConstraints.unique))
    return;

  Object.keys(artifact.$tableConstraints.unique)
    .map(id => `${artifactName}_${id}`) // final unique constraint identifier will be generated in renderer likewise
    .forEach((uniqueConstraintIdentifier) => {
      if (artifact.$tableConstraints.referential[uniqueConstraintIdentifier]) {
        error(null, path,
              { name: uniqueConstraintIdentifier, art: artifactName },
              'Duplicate constraint name $(NAME) in artifact $(ART)');
      }
    });
}

module.exports = { createReferentialConstraints, assertConstraintIdentifierUniqueness };
