'use strict';

const { setProp, isBetaEnabled } = require('../base/model');
const { getUtils, cloneCsn, forEachGeneric,
        forEachMember,
        forEachMemberRecursively, forEachRef,
        forAllQueries, forAllElements, hasAnnotationValue, getArtifactDatabaseNameOf,
        getElementDatabaseNameOf, isBuiltinType, applyTransformations,
        isPersistedOnDatabase, getNormalizedQuery, isAspect, walkCsnPath,
      } = require('../model/csnUtils');
const { makeMessageFunction } = require('../base/messages');
const transformUtils = require('./transformUtilsNew');
const { translateAssocsToJoinsCSN } = require('./translateAssocsToJoins');
const { csnRefs, pathId, implicitAs } = require('../model/csnRefs');
const { checkCSNVersion } = require('../json/csnVersion');
const validate = require('../checks/validator');
const { addLocalizationViewsWithJoins, addLocalizationViews } = require('../transform/localized');
const timetrace = require('../utils/timetrace');
const { createReferentialConstraints, assertConstraintIdentifierUniqueness } = require('./db/constraints');
const { createDict } = require('../utils/objectUtils');
const handleExists = require('./db/transformExists');
const { usesMixinAssociation, getMixinAssocOfQueryIfPublished } = require('./db/helpers');
const replaceAssociationsInGroupByOrderBy = require('./db/groupByOrderBy');
const _forEachDefinition = require('../model/csnUtils').forEachDefinition;
const flattening = require('./db/flattening');
const expansion = require('./db/expansion');
const assertUnique = require('./db/assertUnique');
const generateDrafts = require('./db/draft');
const enrichUniversalCsn = require('./universalCsnEnricher');

// By default: Do not process non-entities/views
function forEachDefinition(csn, cb) {
  _forEachDefinition(csn, cb, {skip: ['annotation', 'action', 'function','event']})
}

/**
 * Return a copy of the compact CSN model with a number of transformations made for rendering
 * in HANA CDS style, used by 'toHana', toSql' and 'toRename'.
 * The behavior is controlled by the following options:
 * options = {
 *    forHana.names                               // See the behavior of 'names' in toHana, toSql and toRename
 *    forHana.alwaysResolveDerivedTypes           // Always resolve derived type chains (by default, this is only
 *                                                // done for 'quoted' names). FIXME: Should always be done in general.
 * }
 * The result model will always have 'options.forHana' set, to indicate that these transformations have happened.
 * The following transformations are made:
 * - (000) Some primitive type names are mapped to HANA type names (e.g. DateTime => UTCDateTime,
 *         Date => LocalDate, ...).The primitive type 'UUID' is renamed to 'String' (see also 060 below).
 * - (001) Add a temporal where condition to views where applicable before assoc2join
 * - (010) (not for to.hdbcds with hdbcds names): Transform associations to joins
 * - (015) Draft shadow entities are generated for entities/views annotated with '@odata.draft.enabled'.
 * - (020) Check: in "plain" mode, quoted ids are not allowed.
 *         (a) check in namespace declarations
 *         (b) check in artifact/element definitions.
 * - (040) Abstract entities and entities 'implemented in' something are ignored, as well
 *         as entities annotated with '@cds.persistence.skip' or '@cds.persistence.exists'.
 * - (050) Checks on the hierarchical model (pre-flattening)
 *         array of, @cds.valid.from/to
 * - (045) The query is stripped from entities that are annotated with '@cds.persistence.table',
 *         essentially converting views to entities.
 * - (060) Users of primitive type 'UUID' (which is renamed to 'String' in 000) get length 36'.
 * - (070) Default length 5000 is supplied for strings if not specified.
 * - (080) Annotation definitions are ignored (note that annotation assignments are filtered out by toCdl).
 * - (090) Compositions become associations.
 * - (100) 'masked' is ignored (a), and attribute 'localized' is removed (b)
 * - (110) Actions and functions (bound or unbound) are ignored.
 * - (120) (a) Services become contexts.
 * - (130) (not for to.hdbcds with hdbcds names): Elements having structured types are flattened into
 *         multiple elements (using '_' or '.' as name separator, depending on 'forHana.names').
 * - (140) (not for to.hdbcds with hdbcds names): Managed associations get explicit ON-conditions, with
 *         generated foreign key elements (also using '_' or '.' as name separator, depending on 'forHana.names').
 * - (150) (a) Elements from inherited (included) entities are copied into the receiving entity
 *         (b) The 'include' property is removed from entities.
 * - (160) Projections become views, with MIXINs for association elements (adding $projection where
 *         appropriate for ON-conditions).
 * - (170) ON-conditions referring to '$self' are transformed to compare explicit keys instead.
 * - (180) In projections and views, ...
 *         (a) association elements that are mixins must not be explicitly redirected
 *         (b) MIXINs are created for association elements in the select list that are not mixins by themselves.
 * - (190) For all enum types, ...
 *         (a) enum constants in defaults are replaced by their values (assuming a matching enum as element type)
 *         (b) the enum-ness is stripped off (i.e. the enum type is replaced by its final base type).
 * - (200) The 'key' property is removed from all elements of types.
 * - (210) (not for to.hdbcds with hdbcds names): Managed associations in GROUP BY and ORDER BY are
 *         replaced by by their foreign key fields.
 * - (220) Contexts that contain no artifacts or only ignored artifacts are ignored.
 * - (230) (only for to.hdbcds with hdbcds names): The following are rejected in views
 *         (a) Structured elements
 *         (b) Managed association elements
 *         (c) Managed association entries in GROUP BY
 *         (d) Managed association entries in ORDER BY
 * - (240) All artifacts (a), elements, foreign keys, parameters (b) that have a DB representation are annotated
 *         with their database name (as '@cds.persistence.name') according to the naming convention chosen
 *         in 'options.forHana.names'.
 * - (250) Remove name space definitions again (only in forHanaNew). Maybe we can omit inserting namespace definitions
 *         completely (TODO)
 *
 * @param {CSN.Model}   inputModel
 * @param {CSN.Options} options
 * @param {string}      moduleName The calling compiler module name, e.g. `to.hdi` or `to.hdbcds`.
 */
function transformForHanaWithCsn(inputModel, options, moduleName) {
  const columnClearer = [];
  // copy the model as we don't want to change the input model
  timetrace.start('HANA transformation');
  /** @type {CSN.Model} */
  let csn = cloneCsn(inputModel, options);



  checkCSNVersion(csn, options);

  const pathDelimiter = (options.forHana.names === 'hdbcds') ? '.' : '_';

  let error, warning, info; // message functions
  /** @type {() => void} */
  let throwWithError;
  let artifactRef, inspectRef, queryOrMain, effectiveType, // csnRefs
    addDefaultTypeFacets, expandStructsInExpression, toFinalBaseType, getFinalBaseType, // transformUtils
    get$combined; // csnUtils

  bindCsnReference();

  throwWithError(); // reclassify and throw in case of non-configurable errors
  
  if (options.csnFlavor === 'universal' && isBetaEnabled(options, 'enableUniversalCsn')) {
    enrichUniversalCsn(csn, options); 
    bindCsnReference();
  }

  const dialect = options.forHana && options.forHana.dialect || options.toSql && options.toSql.dialect;
  const doA2J = !(options.transformation === 'hdbcds' && options.sqlMapping === 'hdbcds');
  if (!doA2J)
    forEachDefinition(csn, handleMixinOnConditions);

  // Run validations on CSN - each validator function has access to the message functions and the inspect ref via this
  const cleanup = validate.forHana(csn, {
    error, warning, info, inspectRef, effectiveType, artifactRef, csnUtils: getUtils(csn), csn, options, getFinalBaseType, isAspect
  });

  // Check if structured elements and managed associations are compared in an expression
  // and expand these structured elements. This tuple expansion allows all other
  // subsequent procession steps (especially a2j) to see plain paths in expressions.
  // If errors are detected, throwWithError() will return from further processing

  expandStructsInExpression(csn, { drillRef: true });

  throwWithError();

  // FIXME: This does something very similar to cloneWithTransformations -> refactor?
  const transformCsn = transformUtils.transformModel;

  handleExists(csn, options, error);

  // (001) Add a temporal where condition to views where applicable before assoc2join
  //       assoc2join eventually rewrites the table aliases
  forEachDefinition(csn, addTemporalWhereConditionToView);

  // check unique constraints - further processing is done in rewriteUniqueConstraints
  assertUnique.prepare(csn, options, error, info);

  if(doA2J) {
    // Expand a structured thing in: keys, columns, order by, group by
    expansion.expandStructureReferences(csn, options, pathDelimiter, {error, info, throwWithError});
    bindCsnReference();
  }

  // Remove properties attached by validator - they do not "grow" as the model grows.
  cleanup();

  bindCsnReferenceOnly();


  if(doA2J) {
    const resolved = new WeakMap();
    // No refs with struct-steps exist anymore
    flattening.flattenAllStructStepsInRefs(csn, options, resolved, pathDelimiter);
    // No type references exist anymore
    // Needs to happen exactly between flattenAllStructStepsInRefs and flattenElements to keep model resolvable.
    flattening.resolveTypeReferences(csn, options, resolved, pathDelimiter);
    // No structured elements exists anymore
    flattening.flattenElements(csn, options, pathDelimiter, error);
  } else {
    // For to.hdbcds with naming mode hdbcds we also need to resolve the types
    flattening.resolveTypeReferences(csn, options, undefined, pathDelimiter);
  }

  // (010) If requested, translate associations to joins
  if (doA2J)
    handleAssocToJoins();

  bindCsnReference();

  const redoProjections = [];
  // Use the "raw" forEachDefinition here to ensure that the _ignore takes effect
  _forEachDefinition(csn, (artifact) => {
    if(artifact.kind === 'entity' && artifact.projection) {
      artifact.query = { SELECT: artifact.projection };
      delete artifact.projection;
      redoProjections.push(() => {
        if(artifact.query) {
          artifact.projection = artifact.query.SELECT;
          delete artifact.query;
          if(artifact.$syntax === 'projection') {
            delete artifact.$syntax;
          }
        }
      })
    } else if(artifact.kind === 'annotation' || artifact.kind === 'action' || artifact.kind === 'function' || artifact.kind === 'event'){
      // _ignore actions etc. - this loop seemed handy for this, as we can hook into an existing if
      artifact._ignore = true;
    }
  });

  // Must happen after A2J, as A2J needs $self to correctly resolve stuff
  if(doA2J)
    flattening.removeLeadingSelf(csn);

  const {
    flattenStructuredElement,
    flattenStructStepsInRef, getForeignKeyArtifact,
    isAssociationOperand, isDollarSelfOrProjectionOperand,
    extractValidFromToKeyElement, checkAssignment, checkMultipleAssignments,
    recurseElements
  } = transformUtils.getTransformers(csn, options, pathDelimiter);

  const {
    getCsnDef,
    isAssocOrComposition,
    isManagedAssociationElement,
    isStructured,
    addStringAnnotationTo,
    cloneWithTransformations,
  } = getUtils(csn);

  // (000) Rename primitive types, make UUID a String
  transformCsn(csn, {
    type: (val, node, key, path) => {
      // Resolve type-of chains
      function fn() {
        // val can be undefined: books as myBooks : redirected to Model.MyBooks
        if (val && val.ref) {
          const { art } = inspectRef(path);
          if (art && art.type) {
            val = art.type;
            // This is somehow needed to update the ref so that inspectRef sees it
            node[key] = val;
            if (val.ref)
              fn();
          }
          else {
            // Doesn't seem to ever ocurr
          }
        }
      }
      fn();
      renamePrimitiveTypesAndUuid(val, node, key);
      addDefaultTypeFacets(node);
    },
    cast: (val) => {
      if (options.forHana.names === 'plain' || options.toSql )
        toFinalBaseType(val);
      renamePrimitiveTypesAndUuid(val.type, val, 'type');
      addDefaultTypeFacets(val);
    },
    // HANA/SQLite do not support array-of - turn into CLOB/Text
    items: (val, node) => {
      node.type = 'cds.LargeString';
      delete node.items;
    },
  }, true);

  // (040) Ignore entities and views that are abstract or implemented
  // or carry the annotation cds.persistence.skip/exists
  // These entities are not removed from the csn, but flagged as "to be ignored"
  forEachDefinition(csn, handleCdsPersistence);


  // (050) Check @cds.valid.from/to only on entity
  //       Views are checked in (001), unbalanced valid.from/to's or mismatching origins
  //       Temporal only in beta-mode
  forEachDefinition(csn, handleTemporalAnnotations);

  handleManagedAssociationsAndCreateForeignKeys();
  
  function handleManagedAssociationsAndCreateForeignKeys() {
    forEachDefinition(csn, (art, artName) => handleManagedAssociationFKs(art, artName));
    forEachDefinition(csn, (art, artName) => createForeignKeyElements(art, artName));
  }

  forEachDefinition(csn, flattenIndexes);
  // Basic handling of associations in views and entities
  forEachDefinition(csn, handleAssociations);

  // (045) Strip all query-ish properties from views and projections annotated with '@cds.persistence.table',
  // and make them entities
  forEachDefinition(csn, handleQueryish);

  // Allow using managed associations as steps in on-conditions to access their fks
  // To be done after handleAssociations, since then the foreign keys of the managed assocs
  // are part of the elements
  forEachDefinition(csn, handleManagedAssocStepsInOnCondition);

  // Create convenience views for localized entities/views.
  // To be done after handleManagedAssocStepsInOnCondition because associations are
  // handled and before handleDBChecks which removes the localized attribute.
  // Association elements of localized convenience views do not have hidden properties
  // like $managed set, so we cannot do this earlier on.
  if (doA2J)
    addLocalizationViewsWithJoins(csn, options);
  else
    addLocalizationViews(csn, options);

  // For generating DB stuff:
  // - table-entity with parameters: not allowed
  // - view with parameters: ok on HANA, not allowed otherwise
  // (don't complain about action/function with parameters)
  forEachDefinition(csn, handleChecksForWithParameters);

  // Remove .masked
  // Check that keys are not explicitly nullable
  // Check that Associations are not used in entities/views with parameters
  // (150 b) Strip inheritance
  // Note that this should happen after implicit redirection, because includes are required for that
  forEachDefinition(csn, handleDBChecks);

  // (170) Transform '$self' in backlink associations to appropriate key comparisons
  // Must happen before draft processing because the artificial ON-conditions in generated
  // draft shadow entities have crooked '_artifact' links, confusing the backlink processing.
  // But it must also happen after flattenForeignKeys has been called for all artifacts,
  // because otherwise we would produce wrong ON-conditions for the keys involved. Sigh ...
  forEachDefinition(csn, transformSelfInBacklinks);

  if(isBetaEnabled(options, 'foreignKeyConstraints') && options.forHana){
    /**
     * Referential Constraints are only supported for sql-dialect "hana" and "sqlite".
     * For to.hdbcds with naming mode "hdbcds", no foreign keys are calculated,
     * hence we do not generate the referential constraints for them.
     */
    const validOptionsForConstraint = () => {
      return (options.forHana.dialect === 'sqlite' || options.forHana.dialect === 'hana') && doA2J;
    }
    if(validOptionsForConstraint())
      createReferentialConstraints(csn, options);
  }

  generateDrafts(csn, options, pathDelimiter, { info, warning, error });

  // Set the final constraint paths and produce hana tc indexes if required
  // See function comment for extensive information.
  assertUnique.rewrite(csn, options, pathDelimiter);

  // Associations that point to thins marked with @cds.persistence.skip are removed
  forEachDefinition(csn, ignoreAssociationToSkippedTarget);

  // Apply view-specific transformations
  // (160) Projections now finally become views
  // Replace managed association in group/order by with foreign keys
  forEachDefinition(csn, transformViews);

  // Recursively apply transformCommon and attach @cds.persistence.name
  forEachDefinition(csn, recursivelyApplyCommon);

  const checkConstraintIdentifiers = (artifact, artifactName, prop, path) => {
    assertConstraintIdentifierUniqueness(artifact, artifactName, path, error);
  };
  const removeNamespaces = (artifact, artifactName) => {
    if (artifact.kind === 'namespace')
      delete csn.definitions[artifactName];
  };
  const ignoreNonPersistedArtifactsWithAnonymousAspectComposition = (artifact) => {
    if(artifact.kind === 'type' || artifact.kind === 'aspect' || artifact.kind === 'entity' && artifact.abstract){
      if(artifact.elements && Object.keys(artifact.elements).some((elementName) => {
        const element = artifact.elements[elementName];
        return !element.target && element.targetAspect && typeof element.targetAspect !== 'string';
      })) {
        artifact._ignore = true;
      }
    }
  };

  forEachDefinition(csn, [
    /* assert that there will be no conflicting unique- and foreign key constraint identifiers */
    checkConstraintIdentifiers,
    /* (250) Remove all namespaces from definitions */
    removeNamespaces,
    /* (190 b) Replace enum types by their final base type */
    replaceEnumsByBaseTypes,
    /* Check Type Parameters (precision, scale, length ...) */
    checkTypeParameters,
    /* Filter out aspects/types/abstract entities containing managed compositions of anonymous aspects */
    ignoreNonPersistedArtifactsWithAnonymousAspectComposition,
    // (200) Strip 'key' property from type elements
    removeKeyPropInType,
  ]);

  throwWithError();

  timetrace.stop();

  function killProp(parent, prop){
    delete parent[prop];
  }

  const killers = {
    '_ignore': function (parent, a, b, path){
      if(path.length > 2) {
        const tail = path[path.length-1];
        const parentPath = path.slice(0, -1)
        const parentParent = walkCsnPath(csn, parentPath);
        delete parentParent[tail];
      } else {
        delete parent._ignore;
      }
    },
    '_art': killProp,
    '_effectiveType': killProp,
    '_flatElementNameWithDots': killProp,
    '_sources': killProp,
    '$default': killProp,
    '$draftRoot': killProp,
    '$env': killProp,
    '$fksgenerated': killProp,
    '$lateFlattening': killProp,
    '$path': killProp,
    '$renamed': killProp,
    '$key': killProp,
    '$generatedExists': killProp
  }

  applyTransformations(csn, killers, [], false);

  redoProjections.forEach(fn => fn());
  columnClearer.forEach(fn => fn());

  return csn;

  /* ----------------------------------- Functions start here -----------------------------------------------*/

  /**
   * Create the foreign key elements for managed associations.
   * Create them in-place, right after the corresponding association.
   *
   *
   * @param {CSN.Artifact} art
   * @param {string} artName
   */
  function createForeignKeyElements(art, artName) {
    if ((art.kind === 'entity' || art.kind === 'view') && doA2J) {
      forAllElements(art, artName, (parent, elements, pathToElements) => {
        const elementsArray = [];
        forEachGeneric(parent, 'elements', (element, elemName) => {
          elementsArray.push([elemName, element]);
          if (isManagedAssociationElement(element)) {
            if (element.keys) {
              for(let i = 0; i < element.keys.length; i++){
                const foreignKey = element.keys[i];
                const path =  [...pathToElements, elemName, 'keys', i];
                foreignKey.ref = flattenStructStepsInRef(foreignKey.ref, path);
                const [fkName, fkElem] = getForeignKeyArtifact(element, elemName, foreignKey, path);
                if(parent.elements[fkName]) {
                  error(null, [...pathToElements, elemName], { name: fkName, art: elemName },
                       'Generated foreign key element $(NAME) for association $(ART) conflicts with existing element');
                } else {
                  elementsArray.push([fkName, fkElem]);
                }
                applyCachedAlias(foreignKey);
                // join ref array as the struct / assoc steps are not necessary anymore
                foreignKey.ref = [foreignKey.ref.join(pathDelimiter)]
              }
            }
          }
        });

        // Don't fake consistency of the model by adding empty elements {}
        if(elementsArray.length === 0)
          return;

        parent.elements = elementsArray.reduce((previous, [name, element]) => {
          previous[name] = element;
          return previous;
        }, Object.create(null));

      })
    }

    function applyCachedAlias(foreignKey) {
      // If we have a $ref use that - it resolves aliased FKs correctly
      if (foreignKey.$ref) {
        foreignKey.ref = foreignKey.$ref;
        delete foreignKey.$ref;
      }
    }
  }


  function bindCsnReference(){
    ({ error, warning, info, throwWithError } = makeMessageFunction(csn, options, moduleName));
    ({ artifactRef, inspectRef, queryOrMain, effectiveType } = csnRefs(csn));
    ({ getFinalBaseType, get$combined } = getUtils(csn));
    ({ addDefaultTypeFacets, expandStructsInExpression, toFinalBaseType } = transformUtils.getTransformers(csn, options, pathDelimiter));
  }

  function bindCsnReferenceOnly(){
    // invalidate caches for CSN ref API
    ({ artifactRef, inspectRef, queryOrMain, effectiveType } = csnRefs(csn));
  }

  function handleMixinOnConditions(artifact, artifactName) {
    if (!artifact.query)
      return;
    forAllQueries(artifact.query, (query, path) => {
      const { mixin } = query.SELECT  || {};
      if(mixin) {
        query.SELECT.columns
        // filter for associations which are used in the SELECT
        .filter((c) => {
          return c.ref &&  c.ref.length > 1;
        })
        .map((usedAssoc) => {
          const assocName = pathId(usedAssoc.ref[0]);
          const mixinAssociation = mixin[assocName];
          if(mixinAssociation){
            mixinAssociation.on = getResolvedMixinOnCondition(csn, mixinAssociation, query, assocName, path.concat(['mixin', assocName]));
          }
        })
      }
    }
    , [ 'definitions', artifactName, 'query' ]);
    function getResolvedMixinOnCondition(csn, mixinAssociation, query, assocName, path){
      const { inspectRef } = csnRefs(csn);
      const referencedThroughStar = query.SELECT.columns.some((column) => column === '*');
      return mixinAssociation.on
            .map((onConditionPart, i) => {
              let columnToReplace;
              if(onConditionPart.ref && (onConditionPart.ref[0] === '$projection' || onConditionPart.ref[0] === '$self')){
                const { links } = inspectRef(path.concat(['on', i]));
                if(links){
                  columnToReplace = onConditionPart.ref[links.length - 1];
                }
              }
              if (!columnToReplace)
                return onConditionPart;

              const replaceWith = query.SELECT.columns.find((column) =>
                column.as && column.as === columnToReplace ||
                column.ref && column.ref[0] === columnToReplace
              );
              if (!replaceWith && referencedThroughStar) {
                // not explicitly in column list, check query sources
                // get$combined also includes elements which are part of "excluding {}"
                // this shouldn't be an issue here, as such references get rejected
                const elementsOfQuerySources = get$combined(query);
                Object.entries(elementsOfQuerySources).forEach(([id, element]) => {
                  // if the ref points to an element which is not explicitly exposed in the column list,
                  // but through the '*' operator -> replace the $projection / $self with the correct source entity
                  if(id === columnToReplace)
                    onConditionPart.ref[0] = element[0].parent;
                });
              }

              // No implicit CAST in on-condition
              if(replaceWith && replaceWith.cast) {
                const clone = cloneCsn(replaceWith, options);
                delete clone.cast;
                return clone;
              }
              return replaceWith || onConditionPart;
            });
    }
  }

  /**
   * @param {CSN.Artifact} artifact
   * @param {string} artifactName
   */
  function transformViews(artifact, artifactName) {
    if (!artifact._ignore) {
      // Do things specific for entities and views (pass 2)
      if ((artifact.kind === 'entity' || artifact.kind === 'view') && artifact.query) {
        forAllQueries(artifact.query, (q, p) => {
          transformEntityOrViewPass2(q, artifact, artifactName, p)
          replaceAssociationsInGroupByOrderBy(q, options, inspectRef, error, p);
        }, [ 'definitions', artifactName, 'query' ]);
      }
    }
  }

  /**
   * @param {CSN.Artifact} artifact
   * @param {string} artifactName
   */
  function recursivelyApplyCommon(artifact, artifactName) {
    if (!artifact._ignore) {
      if (![ 'service', 'context', 'namespace', 'annotation', 'action', 'function' ].includes(artifact.kind))
        addStringAnnotationTo('@cds.persistence.name', getArtifactDatabaseNameOf(artifactName, options.forHana.names, csn), artifact);

      forEachMemberRecursively(artifact, (member, memberName, property, path) => {
        transformCommon(member, memberName, path);
        // (240 b) Annotate elements, foreign keys, parameters etc with their DB names
        // Virtual elements in entities and types are not annotated, as they have no DB representation.
        // In views they are, as we generate a null expression for them (null as <colname>)
        if ((!member.virtual || artifact.query))
          addStringAnnotationTo('@cds.persistence.name', getElementDatabaseNameOf(memberName, options.forHana.names), member);
      }, [ 'definitions', artifactName ]);
    }
  }

  /**
   * @param {CSN.Artifact} artifact
   * @param {string} artifactName
   */
  function removeKeyPropInType(artifact, artifactName) {
    if (!artifact._ignore) {
      forEachMemberRecursively(artifact, (member) => {
        if (artifact.kind === 'type' && member.key)
          delete member.key;
      }, [ 'definitions', artifactName ]);
    }
  }

  /**
   * @param {CSN.Artifact} artifact
   * @param {string} artifactName
   */
  function replaceEnumsByBaseTypes(artifact, artifactName) {
    replaceEnumByBaseType(artifact);
    forEachMemberRecursively(artifact, (member) => {
      replaceEnumByBaseType(member);
      if (options.forHana.alwaysResolveDerivedTypes || options.forHana.names === 'plain') {
        toFinalBaseType(member);
        addDefaultTypeFacets(member);
      }
    }, [ 'definitions', artifactName ]);
  }

  /**
   * @param {CSN.Artifact} artifact
   * @param {string} artifactName
   */
  function transformSelfInBacklinks(artifact, artifactName, dummy, path) {
    // Fixme: For toHana mixins must be transformed, for toSql -d hana
    // mixin elements must be transformed, why can't toSql also use mixins?
    doit(artifact.elements, path.concat([ 'elements' ]));
    if (artifact.query && artifact.query.SELECT && artifact.query.SELECT.mixin)
      doit(artifact.query.SELECT.mixin, path.concat([ 'query', 'SELECT', 'mixin' ]));

    function doit(dict, subPath) {
      for (const elemName in dict) {
        const elem = dict[elemName];
        if (isAssocOrComposition(elem.type) && elem.on)
          processBacklinkAssoc(elem, elemName, artifact, artifactName, subPath.concat([ elemName, 'on' ]));
      }
    }
  }

  /**
   * @param {CSN.Artifact} artifact
   * @param {string} artifactName
   */
  function handleDBChecks(artifact, artifactName) {
    // Strip inheritance
    if (artifact.includes)
      delete artifact.includes;

    // Process the artifact's  members
    forEachMemberRecursively(artifact, (member, memberName, prop, path) => {
      // (100 a) Ignore the property 'masked' itself (but not its effect on projections)
      if (member.masked)
        delete member.masked;
      // For HANA: Report an error on
      // - view with parameters that has an element of type association/composition
      // - association that points to entity with parameters
      if (options.forHana.dialect === 'hana' && member.target && isAssocOrComposition(member.type) && !isBetaEnabled(options, 'assocsWithParams')) {
        if (artifact.params) {
          // HANA does not allow 'WITH ASSOCIATIONS' on something with parameters:
          // SAP DBTech JDBC: [7]: feature not supported: parameterized sql view cannot support association: line 1 col 1 (at pos 0)
          error(null, path, 'Unexpected association in parameterized view');
        }
        else if(artifact['@cds.persistence.udf'] || artifact['@cds.persistence.calcview']) {
          // UDF/CVs w/o params don't support 'WITH ASSOCIATIONS'
          error(null, path, `Associations are not allowed in entities annotated with @cds.persistence { udf, calcview }`);
        }
        if (csn.definitions[member.target].params) {
          // HANA does not allow association targets with parameters or to UDFs/CVs w/o parameters:
          // SAP DBTech JDBC: [7]: feature not supported: cannot support create association to a parameterized view
          error(null, path, 'Unexpected parameterized association target');
        }
        else if(csn.definitions[member.target]['@cds.persistence.udf'] || artifact['@cds.persistence.calcview']) {
          // HANA won't check the assoc target but when querying an association with target UDF, this is the error:
          // SAP DBTech JDBC: [259]: invalid table name: target object SYSTEM.UDF does not exist: line 3 col 6 (at pos 43)
          // CREATE TABLE F (id INTEGER NOT NULL);
          // CREATE FUNCTION UDF RETURNS TABLE (ID INTEGER) LANGUAGE SQLSCRIPT SQL SECURITY DEFINER AS BEGIN RETURN SELECT ID FROM F; END;
          // CREATE TABLE Y (  id INTEGER NOT NULL,  toUDF_id INTEGER) WITH ASSOCIATIONS (MANY TO ONE JOIN UDF AS toUDF ON (toUDF.id = toUDF_id));
          // CREATE VIEW U AS SELECT  id, toUDF.a FROM Y;
          error(null, path, `Associations can't point to entities annotated with @cds.persistence { udf, calcview }`);
        }
      }
    }, [ 'definitions', artifactName ]);
  }

  /**
   *
   * Generate foreign keys for managed associations
   * Forbid aliases for foreign keys
   *
   * @param {CSN.Artifact} artifact
   * @param {string} artifactName
   */
  function handleAssociations(artifact, artifactName) {
    // Do things specific for entities and views (pass 1)
    if (artifact.kind === 'entity' || artifact.kind === 'view') {
      forAllElements(artifact, artifactName, (parent, elements) => {
        for (const elemName in elements) {
          const elem = elements[elemName];
          // (140) Generate foreign key elements and ON-condition for managed associations
          // (unless explicitly asked to keep assocs unchanged)
          if (doA2J) {
            if (isManagedAssociationElement(elem))
              transformManagedAssociation(parent, artifactName, elem, elemName);
          }
        }
      })
    }
  }

  function fixBorkedElementsOfLocalized(elements, pathToElements){
    const pathToNonLocalized = ['definitions', pathToElements[1].replace('localized.',''), ...pathToElements.slice(2)];
    const nonLocalizedElements = walkCsnPath(csn, pathToNonLocalized);


    for(const elementName in elements){
      const element = elements[elementName];
      const reference = nonLocalizedElements[elementName];

      // if the declared element is an enum, these values are with priority
      if (!element.enum && reference.enum)
        Object.assign(element, { enum: reference.enum });
      if (!element.length && reference.length && !reference.$default)
        Object.assign(element, { length: reference.length });
      if (!element.precision && reference.precision)
        Object.assign(element, { precision: reference.precision });
      if (!element.scale && reference.scale)
        Object.assign(element, { scale: reference.scale });
      if (!element.srid && reference.srid)
        Object.assign(element, { srid: reference.srid });
      if (!element.keys && reference.keys)
        Object.assign(element, { keys: cloneCsn(reference.keys, options)})
      if (!element.type && reference.type)
        Object.assign(element, { type: reference.type})
      if (!element.on && reference.on && !reference.keys)
        Object.assign(element, {on: cloneCsn(reference.on, options)})
    }
  }

  /**
   * @param {CSN.Artifact} artifact
   * @param {string} artifactName
   */
  function handleChecksForWithParameters(artifact, artifactName) {
    if (!artifact._ignore && artifact.params && (artifact.kind === 'entity' || artifact.kind === 'view')) {
      if (!artifact.query) { // table entity with params
        // Allow with plain
        error(null, [ 'definitions', artifactName ], { '#': options.toSql ? 'sql' : 'std' }, {
          std: 'Table-like entities with parameters are not supported for conversion to SAP HANA CDS',
          sql: 'Table-like entities with parameters are not supported for conversion to SQL',
        });
      }
      else if (options.forHana.dialect === 'sqlite') { // view with params
        // Allow with plain
        error(null, [ 'definitions', artifactName ], `SQLite does not support entities with parameters`);
      }
      else {
        for (const pname in artifact.params) {
          if (pname.match(/\W/g) || pname.match(/^\d/) || pname.match(/^_/)) { // parameter name must be regular SQL identifier
            warning(null, [ 'definitions', artifactName, 'params', pname ], `Expecting regular SQL-Identifier`);
          }
          else if (options.forHana.names !== 'plain' && pname.toUpperCase() !== pname) { // not plain mode: param name must be all upper
            warning(null, [ 'definitions', artifactName, 'params', pname ], { name: options.forHana.names },
                   'Expecting parameter to be uppercase in naming mode $(NAME)');
          }
        }
      }
    }
  }

   /**
    * @param {CSN.Artifact} artifact
    * @param {string} artifactName
    */
  function handleQueryish(artifact, artifactName) {
    const stripQueryish = artifact.query && hasAnnotationValue(artifact, '@cds.persistence.table');

    if (stripQueryish) {
      artifact.kind = 'entity';
      delete artifact.query;
    }

    recurseElements(artifact, [ 'definitions', artifactName ], (member, path) => {
      // All elements must have a type for this to work
      if (stripQueryish && !member._ignore && !member.kind && !member.type)
        error(null, path, 'Expecting element to have a type if view is annotated with “@cds.persistence.table“');
    });
  }

  /**
   * @param {CSN.Artifact} artifact
   * @param {string} artifactName
   */
  function handleCdsPersistence(artifact, artifactName) {
    if (artifact.kind === 'entity' || artifact.kind === 'view') {
      if (artifact.abstract
        || hasAnnotationValue(artifact, '@cds.persistence.skip')
        || hasAnnotationValue(artifact, '@cds.persistence.exists'))
        artifact._ignore = true;

      // issue #3450 HANA CDS can not handle external artifacts which are part of a HANA CDS context
      if (options.forHana.names === 'quoted' &&
          hasAnnotationValue(artifact, '@cds.persistence.exists')) {
        const firstPath = artifactName.split('.')[0];
        const topParent = csn.definitions[firstPath];
        // namespaces, contexts and services become contexts in HANA CDS
        if (topParent && [ 'namespace', 'context', 'service' ].includes(topParent.kind))
          warning(null, [ 'definitions', artifactName ], `"${ artifactName }": external definition belongs to ${ topParent.kind } "${ firstPath }"`);
      }
    }
  }

  function handleAssocToJoins() {
    // With flattening errors, it makes little sense to continue.
    throwWithError();
    // the augmentor isn't able to deal with technical configurations and since assoc2join can ignore it we
    // simply make it invisible and copy it over to the result csn
    forEachDefinition(csn, art => art.technicalConfig && setProp(art, 'technicalConfig', art.technicalConfig));

    const newCsn = translateAssocsToJoinsCSN(csn, options);

    // restore all (non-enumerable) properties that wouldn't survive reaugmentation/compactification into the new compact model
    forEachDefinition(csn, (art, artName) => {
      if(art['$tableConstraints']) {
        setProp(newCsn.definitions[artName], '$tableConstraints', art['$tableConstraints']);
      }
      if (art.technicalConfig)
        newCsn.definitions[artName].technicalConfig = art.technicalConfig;

      const newArt = newCsn.definitions[artName];

      // No need to loop/check artifacts that won't reach the DB anyways
      if (art.query && newArt && newArt.query && isPersistedOnDatabase(newArt)) {
        // Loop through the newCSN and add possible new _ignore mixin to the kill list
        forAllQueries(newArt.query, (q, p) => {
          if (q.SELECT && q.SELECT.mixin) {
            for(let mixinName of Object.keys(q.SELECT.mixin)) {
              const mixinElement = q.SELECT.mixin[mixinName];
              if (mixinElement._ignore && options.toSql) {
                columnClearer.push(() => {
                  const query = walkCsnPath(csn, p);
                  for(let i = query.columns.length-1; i > -1; i--){
                    const col = query.columns[i];
                    if(col && col.ref && col.ref[0] === mixinName){
                      query.columns.splice(i, 1);
                    }
                  }
                });
              }
            }
          }
        }, ['definitions', artName, 'query']);
      }
    });
    csn = newCsn;
  }

  /**
   * @param {CSN.Artifact} artifact
   * @param {string} artifactName
   */
  function handleTemporalAnnotations(artifact, artifactName) {
    const validFrom = [];
    const validTo = [];
    const validKey = [];

    recurseElements(artifact, [ 'definitions', artifactName ], (member, path) => {
      const [ f, t, k ] = extractValidFromToKeyElement(member, path);
      validFrom.push(...f);
      validTo.push(...t);
      validKey.push(...k);
    });

    if (artifact.kind === 'entity' && !artifact.query) {
      validFrom.forEach(obj => checkAssignment('@cds.valid.from', obj.element, obj.path, artifact));
      validTo.forEach(obj => checkAssignment('@cds.valid.to', obj.element, obj.path, artifact));
      validKey.forEach(obj => checkAssignment('@cds.valid.key', obj.element, obj.path, artifact));
      checkMultipleAssignments(validFrom, '@cds.valid.from', artifact, artifactName);
      checkMultipleAssignments(validTo, '@cds.valid.to', artifact, artifactName, true);
      checkMultipleAssignments(validKey, '@cds.valid.key', artifact, artifactName);
    }

    // if there is an cds.valid.key, make this the only primary key
    // otherwise add all cds.valid.from to primary key tuple
    if (validKey.length) {
      if (!validFrom.length || !validTo.length)
        error(null, [ 'definitions', artifactName ],
        'Expecting “@cds.valid.from” and “@cds.valid.to” if “@cds.valid.key” is used');

      forEachMember(artifact, (member) => {
        if (member.key) {
          member.unique = true;
          delete member.key;
          // Remember that this element was a key in the original artifact.
          // This is needed for localized convenience view generation.
          setProp(member, '$key', true);
        }
      });
      validKey.forEach((member) => {
        member.element.key = true;
      });

      validFrom.forEach((member) => {
        member.element.unique = true;
      });
    }
    else {
      validFrom.forEach((member) => {
        member.element.key = true;
      });
    }
  }

  function hasFalsyTemporalAnnotations(SELECT, elements, from, to) {
    let fromElement = elements[from.name];
    let toElement = elements[to.name];

    if(SELECT.columns) {
      for(const col of SELECT.columns) {
        if(col.ref) {
          const implicitAlias = implicitAs(col.ref);
          if(implicitAlias === from.name)
            fromElement = elements[col.as || implicitAlias];
          else if(implicitAlias === to.name)
            toElement = elements[col.as || implicitAlias];
        }
      }
    }
    const val = fromElement && toElement && hasAnnotationValue(fromElement, '@cds.valid.from', false) && hasAnnotationValue(toElement, '@cds.valid.to', false);
    return val;
  }

  /**
   * Add a where condition to views that
   * - are annotated with @cds.valid.from and @cds.valid.to,
   * - have only one @cds.valid.from and @cds.valid.to,
   * - and both annotations come from the same entity
   *
   * If the view has one of the annotations but the other conditions are not met, an error will be raised.
   *
   * @param {CSN.Artifact} artifact
   * @param {string} artifactName
   */
  function addTemporalWhereConditionToView(artifact, artifactName) {
    const normalizedQuery = getNormalizedQuery(artifact);
    if (normalizedQuery && normalizedQuery.query && normalizedQuery.query.SELECT) {
      // BLOCKER: We need information to handle $combined
      // What we are trying to achieve by this:
      // Forbid joining/selecting from two or more temporal entities
      // Idea: Follow the query-tree and check each from
      // Collect all source-entities and compute our own $combined
      const $combined = get$combined(normalizedQuery.query);
      const [ from, to ] = getFromToElements($combined);
      // exactly one validFrom & validTo
      if (from.length === 1 && to.length === 1) {
        // and both are from the same origin
        if (from[0].source === to[0].source && from[0].parent === to[0].parent) {
          if(!hasFalsyTemporalAnnotations(normalizedQuery.query.SELECT, artifact.elements, from[0], to[0])) {
            const fromPath = {
              ref: [
                from[0].parent,
                from[0].name,
              ],
            };

            const toPath = {
              ref: [
                to[0].parent,
                to[0].name,
              ],
            };


            const atFrom = { ref: [ '$at', 'from' ] };
            const atTo = { ref: [ '$at', 'to' ] };

            const cond = [ '(', fromPath, '<', atTo, 'and', toPath, '>', atFrom, ')' ];

            if (normalizedQuery.query.SELECT.where) { // if there is an existing where-clause, extend it by adding 'and (temporal clause)'
              normalizedQuery.query.SELECT.where = [ '(', ...normalizedQuery.query.SELECT.where, ')', 'and', ...cond ];
            }
            else {
              normalizedQuery.query.SELECT.where = cond;
            }
          }
        }
        else {
          info(null, [ 'definitions', artifactName ], `No temporal WHERE clause added as "${ from[0].error_parent }"."${ from[0].name }" and "${ to[0].error_parent }"."${ to[0].name }" are not of same origin`);
        }
      }
      else if (from.length > 0 || to.length > 0) {
        const missingAnnotation = from.length > to.length ? '@cds.valid.to' : '@cds.valid.from';
        info(null, [ 'definitions', artifactName ],
          { anno: missingAnnotation },
          'No temporal WHERE clause added because $(ANNO) is missing'
        )
      }
    }
  }

  /**
   * Get all elements tagged with @cds.valid.from/to from the union of all entities of the from-clause.
   *
   * @param {any} combined union of all entities of the from-clause
   * @returns {Array[]} Array where first field is array of elements with @cds.valid.from, second field is array of elements with @cds.valid.to.
   */
  function getFromToElements(combined) {
    const from = [];
    const to = [];
    for (const name in combined) {
      let elt = combined[name];
      if (!Array.isArray(elt))
        elt = [ elt ];
      elt.forEach((e) => {
        if (hasAnnotationValue(e.element, '@cds.valid.from'))
          from.push(e);

        if (hasAnnotationValue(e.element, '@cds.valid.to'))
          to.push(e);
      });
    }

    return [ from, to ];
  }

  /**
   * Associations that target a @cds.persistence.skip artifact must be removed
   * from the persistence model
   *
   * @param {CSN.Artifact} artifact
   * @param {string} artifactName
   * @param {string} prop
   * @param {CSN.Path} path
   */
  function ignoreAssociationToSkippedTarget(artifact, artifactName, prop, path) {
    if (isPersistedOnDatabase(artifact)) {
      // TODO: structure in CSN is artifact.query.[SELECT/SET].mixin
      if (artifact.query) {
        if (artifact.query.SELECT && artifact.query.SELECT.mixin)
          forEachGeneric(artifact.query.SELECT, 'mixin', ignore, path.concat([ 'query', 'SELECT' ]));

        else if (artifact.query.SET && artifact.query.SET.mixin)
          forEachGeneric(artifact.query.SET, 'mixin', ignore, path.concat([ 'query', 'SET' ]));
      }
      forEachMemberRecursively(artifact, ignore, [ 'definitions', artifactName ]);
    }
    function ignore(member, memberName, prop, path) {
      if (dialect === 'hana' && !member._ignore && member.target && isAssocOrComposition(member.type) && isUnreachableAssociationTarget(csn.definitions[member.target])) {
        const targetAnnotation = hasAnnotationValue(csn.definitions[member.target], '@cds.persistence.exists') ? '@cds.persistence.exists' : '@cds.persistence.skip';
        info(null, path,
          { target: member.target, anno: targetAnnotation },
          'Association has been removed as it\'s target $(TARGET) is annotated with $(ANNO)'
        );
        member._ignore = true;
      }
    }
  }

  /**
   * @param {CSN.Artifact} art
   * @returns {boolean}
   */
  function isUnreachableAssociationTarget(art) {
    return !isPersistedOnDatabase(art) || hasAnnotationValue(art, '@cds.persistence.exists');
  }

  /**
   * Remove `localized` from elements and replace Enum symbols by their values.
   *
   * Only applies to elements.
   *
   * @param {CSN.Element} obj
   * @param {String} objName
   * @param {CSN.Path} path
   */
  function transformCommon(obj, objName, path) {
    // (100 b) Remove attribute 'localized'
    if (obj.localized)
      delete obj.localized;

    // (190 a) Replace enum symbols by their value (if found)
    replaceEnumSymbolsByValues(obj, path);
  }

  // Change the names of those builtin types that have different names in HANA.
  // (do that directly in the csn where the builtin types are defined, so that
  // all users of the types benefit from it). Also add the type parameter 'length'
  // to 'UUID' (which becomes a string).
  // TODO: there is no benefit at all - it is fundamentally wrong
  function renamePrimitiveTypesAndUuid(val, node, key) {
    // assert key === 'type'
    const hanaNamesMap = createDict({
      'cds.DateTime': 'cds.UTCDateTime',
      'cds.Timestamp': 'cds.UTCTimestamp',
      'cds.Date': 'cds.LocalDate',
      'cds.Time': 'cds.LocalTime',
      'cds.UUID': 'cds.String',
    });
    node[key] = hanaNamesMap[val] || val;
    if (val === 'cds.UUID' && !node.length) {
      node.length = 36;
      setProp(node, '$renamed', 'cds.UUID');
    }
    // Length/Precision/Scale is done in addDefaultTypeFacets
  }

  // If 'obj' has final type 'cds.UUID' (renamed to String in 000), set its length to 36.
  // function setLengthForFormerUuid(obj) {
  //   if (!obj || !obj.type)
  //     return;
  //   if (obj.type === 'cds.UUID' && !obj.length) {
  //     obj.length = 36;
  //   }
  // }

  /**
  * Strip of leading $self of the ref
  * @param {object} col A column
  *
  * @returns {object}
  */
  function stripLeadingSelf(col) {
    if (col.ref && col.ref.length > 1 && col.ref[0] === '$self')
      col.ref = col.ref.slice(1);


    return col;
  }

  function isUnion(path){
    const subquery = path[path.length-1];
    const queryIndex = path[path.length-2]
    const args = path[path.length-3];
    const unionOperator = path[path.length-4];
    return path.length > 3 && (subquery === 'SET' || subquery === 'SELECT') && typeof queryIndex === 'number' && queryIndex >= 0 && args === 'args' && unionOperator === 'SET';
  }

  function transformEntityOrViewPass2(query, artifact, artName, path) {
    const { elements } = queryOrMain(query, artifact);
    let hasNonAssocElements = false;
    const isSelect = query && query.SELECT;
    let isProjection = !!artifact.projection;
    const columnMap = Object.create(null);
    let isSelectStar = false;
    if (isSelect) {
      if (!query.SELECT.columns) {
        isProjection = true;
      }
      else {
        query.SELECT.columns.forEach((col) => {
          if (col === '*') {
            isSelectStar = true;
          }
          else if (col.as) {
            if (!columnMap[col.as])
              columnMap[col.as] = col;
          }
          else if (col.ref) {
            if (!columnMap[col.ref[col.ref.length - 1]])
              columnMap[col.ref[col.ref.length - 1]] = col;
          }
          else if (col.func) {
            columnMap[col.func] = col;
          }
          else if (!columnMap[col]) {
            columnMap[col] = col;
          }
        });
      }
    }
    if (query && options.transformation === 'hdbcds') {
      // check all queries/subqueries for mixin publishing inside of unions -> forbidden in hdbcds
      if (query.SELECT && query.SELECT.mixin && path.indexOf('SET') !== -1) {
        for (const elementName in elements) {
          const element = elements[elementName];
          if (element.target) {
            let colLocation;
            for (let i = 0; i < query.SELECT.columns.length; i++) {
              const col = query.SELECT.columns[i];
              if (col.ref && col.ref.length === 1) {
                if (!colLocation && col.ref[0] === elementName)
                  colLocation = i;


                if (col.as === elementName)
                  colLocation = i;
              }
            }
            if (colLocation) {
              const matchingCol = query.SELECT.columns[colLocation];
              const possibleMixinName = matchingCol.ref[0];
              const isMixin = query.SELECT.mixin[possibleMixinName] !== undefined;
              if (element.target && isMixin)
                error(null, path.concat([ 'columns', colLocation ]),
                  `Element "${ elementName }" is a mixin association${ possibleMixinName !== elementName ? ` ("${ possibleMixinName }")` : '' } and can't be published in a UNION`);
            }
          }
        }
      }
    }

    // Second walk through the entity elements: Deal with associations (might also result in new elements)

    // Will be initialized JIT inside the elements-loop
    let $combined;

    for (const elemName in elements) {
      const elem = elements[elemName];
      if (isSelect) {
        if (!columnMap[elemName]) {
          // Prepend an alias if present
          let alias = (isProjection || isSelectStar) &&
              (query.SELECT.from.as || (query.SELECT.from.ref && implicitAs(query.SELECT.from.ref)));
          // In case of * and no explicit alias
          // find the source of the col by looking at $combined and prepend it
          if (isSelectStar && !alias && !isProjection) {
            if (!$combined)
              $combined = get$combined(query);


            const matchingCombined = $combined[elemName];
            // Internal errors - this should never happen!
            if (matchingCombined.length > 1) { // should already be caught by compiler
              throw new Error(`Ambiguous name - can't be resolved: ${ elemName }. Found in: ${ matchingCombined.map(o => o.parent) }`);
            }
            else if (matchingCombined.length === 0) { // no clue how this could happen? Invalid CSN?
              throw new Error(`No matching entry found in UNION of all elements for: ${ elemName }`);
            }
            alias = matchingCombined[0].parent;
          }
          if (alias)
            columnMap[elemName] = { ref: [ alias, elemName ] };
          else
            columnMap[elemName] = { ref: [ elemName ] };
        }

        // For associations - make sure that the foreign keys have the same "style"
        // If A.assoc => A.assoc_id, else if assoc => assoc_id or assoc as Assoc => Assoc_id
        if (elem.keys && doA2J) {
          const assoc_col = columnMap[elemName];
          if (assoc_col && assoc_col.ref) {
            elem.keys.forEach((key) => {
              const ref = cloneCsn(assoc_col.ref, options);
              ref[ref.length - 1] = [ ref[ref.length - 1] ].concat(key.as || key.ref).join(pathDelimiter);
              const result = {
                ref,
              };
              if (assoc_col.as)
                result.as = key.$generatedFieldName;


              if (assoc_col.key)
                result.key = true;


              const colName = result.as || ref[ref.length - 1];
              columnMap[colName] = result;
            });
          }
        }
        // Add flattened structured things preserving aliases and refs with/without table alias
        // If we add them when we get to them in "elements", we cannot know what table alias was used...
        if (isStructured(elem) && doA2J) {
          const col = columnMap[elemName];
          const originalName = col.ref[col.ref.length - 1];
          const flatElements = flattenStructuredElement(elem, originalName, [], path);
          const aliasedFlatElements = originalName !== elemName ? Object.keys(flattenStructuredElement(elem, elemName, [], path)) : [];

          Object.keys(flatElements).forEach((flatElemName, index ) => {
            const clone = cloneCsn(col, options);
            // For the ref, use the "original"
            if (clone.ref)
              clone.ref[clone.ref.length - 1] = flatElemName;

            // If the column was aliased, use the alias-prefix for the flattened element
            if (originalName !== elemName)
              clone.as = aliasedFlatElements[index];

            // Insert into map, giving precedence to the alias
            columnMap[clone.as || flatElemName] = clone;
          });
        }
      }
      // Views must have at least one element that is not an unmanaged assoc
      if (!elem.on && !elem._ignore)
        hasNonAssocElements = true;

      // (180 b) Create MIXINs for association elements in projections or views (those that are not mixins by themselves)
      // CDXCORE-585: Allow mixin associations to be used and published in parallel
      if (query !== undefined && elem.target) {
        if(isUnion(path) && options.transformation === 'hdbcds'){
          if(isBetaEnabled(options, 'ignoreAssocPublishingInUnion') && doA2J){
            if(elem.keys) {
              info(null, path, `Managed association "${elemName}", published in a UNION, will be ignored`)
            } else {
              info(null, path, `Association "${elemName}", published in a UNION, will be ignored`)
            }
            elem._ignore = true;
          }
          else {
            error(null, path, `Association "${elemName}" can't be published in a SAP HANA CDS UNION`)
          }
        } else if(path.length > 4 && options.transformation === 'hdbcds'){ // path.length > 4 -> is a subquery
          error(null, path, { name: elemName },
          'Association $(NAME) can\'t be published in a subquery')
        } else {
          /* Old implementation:
        const isNotMixinByItself = !(elem.value && elem.value.path && elem.value.path.length == 1 && art.query && art.query.mixin && art.query.mixin[elem.value.path[0].id]);
        */
          const isNotMixinByItself = checkIsNotMixinByItself(query, columnMap, elem, elemName);
          const {mixinElement, mixinName } = getMixinAssocOfQueryIfPublished(query, elem, elemName);
          if (isNotMixinByItself || mixinElement !== undefined) {
            // If the mixin is only published and not used, only display the __ clone. Ignore the "original".
            if (mixinElement !== undefined && !usesMixinAssociation(query, elem, elemName)){
              mixinElement._ignore = true;
            }

            delete elem._typeIsExplicit;
            // Create an unused alias name for the MIXIN - use 3 _ to avoid collision with usings
            let mixinElemName = `___${ mixinName || elemName }`;
            while (elements[mixinElemName])
              mixinElemName = `_${ mixinElemName }`;

          // Copy the association element to the MIXIN clause under its alias name
          // (shallow copy is sufficient, just fix name and value)
            const mixinElem = Object.assign({}, elem);
          // Perform common transformations on the newly generated MIXIN element (won't be reached otherwise)
            transformCommon(mixinElem, mixinElemName);
          // TODO: Can we rely on query.SELECT.mixin to check for mixins?
          // Yes, we can - only SELECT can have mixin. But:
          // - UNION
          // - JOINS
          // - Subqueries
          // Are currently (and in the old transformer) not handled!
            if (query.SELECT && !query.SELECT.mixin)
              query.SELECT.mixin = Object.create(null);

          // Let the original association element use the newly generated MIXIN name as value and alias
            delete elem.viaAll;

          // Clone 'on'-condition, pre-pending '$projection' to paths where appropriate,
          // and fixing the association alias just created

            if (mixinElem.on) {
              mixinElem.on = cloneWithTransformations(mixinElem.on, {
                ref: (ref) => {
                // Clone the path, without any transformations
                  const clonedPath = cloneWithTransformations(ref, {});
                // Prepend '$projection' to the path, unless the first path step is the (mixin) element itself or starts with '$')
                  if (clonedPath[0] == elemName) {
                    clonedPath[0] = mixinElemName;
                  }
                  else if (!(clonedPath[0] && clonedPath[0].startsWith('$'))) {
                    const projectionId = '$projection';
                    clonedPath.unshift(projectionId);
                  }
                  return clonedPath;
                },
                func: (func) => {
                // Unfortunately, function names are disguised as paths, so we would prepend a '$projection'
                // above (no way to distinguish that in the callback for 'path' above). We can only pluck it
                // off again here ... sigh
                  if (func.ref && func.ref[0] && func.ref[0] === '$projection')
                    func.ref = func.ref.slice(1);

                  return func;
                },
              });
            }

            if (!mixinElem._ignore)
              columnMap[elemName] = { ref: [ mixinElemName ], as: elemName };

            if (query.SELECT) {
              query.SELECT.mixin[mixinElemName] = mixinElem;
            }
          }
        }
      }
    }

    if (query && !hasNonAssocElements) {
      // Complain if there are no elements other than unmanaged associations
      // Allow with plain
      error(null, [ 'definitions', artName ], { $reviewed: true } ,
        'Expecting view or projection to have at least one element that is not an unmanaged association');
    }

    if (isSelect) {
      // Workaround for bugzilla 176495 FIXME FIXME FIXME: is this really still needed?
      // If a select item of a cdx view contains an expression, the result type cannot be computed
      // but must be explicitly specified. This is important for the OData channel, which doesn't
      // work if the type is missing (for HANA channel an explicit type is not required, as HANA CDS
      // can compute the result type).
      // Due to bug in HANA CDS, providing explicit type 'LargeString' or 'LargeBinary' causes a
      // diserver crash. Until a fix in HANA CDS is available, we allow to suppress the explicit
      // type in the HANA channel via an annotation.
      Object.keys(columnMap).forEach((value) => {
        const elem = elements[value];
        if (elem && elem['@cds.workaround.noExplicitTypeForHANA'])
          delete columnMap[value].cast;
      });

      query.SELECT.columns = Object.keys(elements).filter(elem => !elements[elem]._ignore).map(key => stripLeadingSelf(columnMap[key]));
      // If following an association, explicitly set the implicit alias
      // due to an issue with HANA
      for (let i = 0; i < query.SELECT.columns.length; i++) {
        const col = query.SELECT.columns[i];
        if (!col.as && col.ref && col.ref.length > 1) {
          const { links } = inspectRef(path.concat([ 'columns', i ]));
          if (links && links.slice(0, -1).some(({ art }) => isAssocOrComposition(art && art.type || '')))
            col.as = col.ref[col.ref.length - 1];
        }
      }
      delete query.SELECT.excluding;  // just to make the output of the new transformer the same as the old
    }
  }


  // If 'elem' has a default that is an enum constant, replace that by its value. Complain
  // if not found or not an enum type,
  function replaceEnumSymbolsByValues(elem, path) {
    // (190 a) Replace enum symbols by their value (if found)
    if (elem.default && elem.default['#']) {
      let Enum = elem.enum;
      if (!Enum && !isBuiltinType(elem.type)) {
        const typeDef = getCsnDef(elem.type);
        Enum = typeDef && typeDef.enum;
      }
      if (!Enum) {
        // Not an enum at all
        // Looks like it is always run?! But message says HANA CDS?!
        error(null, path, {
          $reviewed: true,
          name: `#${elem.default['#']}`
        },
        'Expecting enum literal $(NAME) to be used with an enum type');
      }
      else {
        // Try to get the corresponding enum symbol from the element's type
        const enumSymbol = Enum[elem.default['#']];
        if (!enumSymbol) {
          error(null, path, {
            $reviewed: true,
            name: `#${elem.default['#']}`
          }, 'Enum literal $(NAME) is undefined in enumeration type');
        }
        else if (enumSymbol.val !== undefined) { // `val` may be `null`
          // Replace default with enum value
          elem.default.val = enumSymbol.val;
          delete elem.default['#'];
        }
        else {
          // Enum symbol without explicit value - replace default by the symbol in string form
          elem.default.val = elem.default['#'];
          delete elem.default['#'];
        }
      }
    }
  }

  // If 'node' has an enum type, change node's type to be the enum's base type
  // and strip off the 'enum' property.
  function replaceEnumByBaseType(node) {
    if (node.items)
      replaceEnumByBaseType(node.items);

    // (190 b) Replace enum types by their final base type (must happen after 190 a)
    /* Old implementation:
    if (node && node._finalType && (node.enum || node._finalType.enum)) {
      node.type = node._finalType.type
      // node.type = node._finalType.type._artifact._finalType.type;
      if (node._finalType.length) {
        node.length = node._finalType.length;
      }
      setProp(node, '_finalType', node.type._artifact);
      delete node.enum;
    }
    */
    if (node && node.enum) {
      // toFinalBaseType(node);
      // addDefaultTypeFacets(node);
      delete node.enum;
    }
  }

  // If the association element 'elem' of 'art' is a backlink association, massage its ON-condition
  // (in place) so that it
  // - compares the generated foreign key fields of the corresponding forward
  //   association with their respective keys in 'art' (for managed forward associations)
  // - contains the corresponding forward association's ON-condition in "reversed" form,
  //   i.e. as seen from 'elem' (for unmanaged associations)
  // Otherwise, do nothing.
  function processBacklinkAssoc(elem, elemName, art, artName, pathToOn) {
    // Don't add braces if it is a single expression (ignoring superfluous braces)
    const multipleExprs = elem.on.filter(x => x !== '(' && x !== ')' ).length > 3;
    /**
     * Process the args
     *
     * @param {Array} xprArgs
     * @param {CSN.Path} path
     * @returns {Array} Array of parsed expression
     */
    function processExpressionArgs(xprArgs, path) {
      const result = [];
      let i = 0;
      while (i < xprArgs.length) {
        // Only token tripel `<path>, '=', <path>` are of interest here
        if (i < xprArgs.length - 2 && xprArgs[i + 1] === '=') {
          // Check if one side is $self and the other an association
          // (if so, replace all three tokens with the condition generated from the other side, in parentheses)
          if (isDollarSelfOrProjectionOperand(xprArgs[i]) && isAssociationOperand(xprArgs[i + 2], path.concat([ i + 2 ]))) {
            const assoc = inspectRef(path.concat([ i + 2 ])).art;
            if (multipleExprs)
              result.push('(');
            const backlinkName = xprArgs[i + 2].ref[xprArgs[i + 2].ref.length - 1];
            result.push(...transformDollarSelfComparison(xprArgs[i + 2],
              assoc,
              backlinkName,
              elem, elemName, art, artName, path.concat([ i ])
            ));
            if (multipleExprs)
              result.push(')');
            i += 3;
             // remember name of backlink, important for foreign key constraints
            if(elem.$selfOnCondition)
              elem.$selfOnCondition.backlinkName += `_${ backlinkName }`;
            else {
              setProp(elem, '$selfOnCondition', {
                backlinkName
              }) // important for the foreign key constraints
            }
          }
          else if (isDollarSelfOrProjectionOperand(xprArgs[i + 2]) && isAssociationOperand(xprArgs[i], path.concat([ i ]))) {
            const assoc = inspectRef(path.concat([ i ])).art;
            if (multipleExprs)
              result.push('(');
            const backlinkName = xprArgs[i].ref[xprArgs[i].ref.length - 1];
            result.push(...transformDollarSelfComparison(xprArgs[i], assoc, backlinkName, elem, elemName, art, artName, path.concat([ i + 2 ])));
            if (multipleExprs)
              result.push(')');
            i += 3;
            // remember name of backlink, important for foreign key constraints
            if(elem.$selfOnCondition)
              elem.$selfOnCondition.backlinkName += `_${ backlinkName }`;
            else {
              setProp(elem, '$selfOnCondition', {
                backlinkName
              }) // important for the foreign key constraints
            }
          }
          // Otherwise take one (!) token unchanged
          else {
            result.push(xprArgs[i]);
            i++;
          }
        }
        // Process subexpressions - but keep them as subexpressions
        else if(xprArgs[i].xpr){
          result.push({xpr: processExpressionArgs(xprArgs[i].xpr, path.concat([i, 'xpr']))});
          i++;
        }
        // Take all other tokens unchanged
        else {
          result.push(xprArgs[i]);
          i++;
        }
      }
      return result;
    }

    elem.on = processExpressionArgs(elem.on, pathToOn);

    // Return the condition to replace the comparison `<assocOp> = $self` in the ON-condition
    // of element <elem> of artifact 'art'. If there is anything to complain, use location <loc>
    function transformDollarSelfComparison(assocOp, assoc, assocName, elem, elemName, art, artifactName, path) {
      // Check: The forward link <assocOp> must point back to this artifact
      // FIXME: Unfortunately, we can currently only check this for non-views (because when a view selects
      // a backlink association element from an entity, the forward link will point to the entity,
      // not to the view).
      // FIXME: This also means that corresponding key fields should be in the select list etc ...
      if (!art.query && !art.projection && assoc.target && assoc.target != artifactName)
        error(null, path, `Only an association that points back to this artifact can be compared to "$self"`);


      // Check: The forward link <assocOp> must not contain '$self' in its own ON-condition
      if (assoc.on) {
        const containsDollarSelf = assoc.on.some(isDollarSelfOrProjectionOperand);

        if (containsDollarSelf)
          error(null, path, `An association that uses "$self" in its ON-condition can't be compared to "$self"`);
      }

      // Transform comparison of $self to managed association into AND-combined foreign key comparisons
      if (assoc.keys) {
        if(assoc.keys.length)
          return transformDollarSelfComparisonWithManagedAssoc(assocOp, assoc, assocName, elemName);
        else {
          elem._ignore = true;
          return [];
        }
      }

      // Transform comparison of $self to unmanaged association into "reversed" ON-condition
      else if (assoc.on)
        return transformDollarSelfComparisonWithUnmanagedAssoc(assocOp, assoc, assocName, elemName);

      throw new Error(`Expected either managed or unmanaged association in $self-comparison: ${ JSON.stringify(elem.on) }`);
    }

    // For a condition `<elemName>.<assoc> = $self` in the ON-condition of element <elemName>,
    // where <assoc> is a managed association, return a condition comparing the generated
    // foreign key elements <elemName>.<assoc>_<fkey1..n> of <assoc> to the corresponding
    // keys in this artifact.
    // For example, `ON elem.ass = $self` becomes `ON elem.ass_key1 = key1 AND elem.ass_key2 = key2`
    // (assuming that `ass` has the foreign keys `key1` and `key2`)
    function transformDollarSelfComparisonWithManagedAssoc(assocOp, assoc, originalAssocName, elemName) {
      const conditions = [];
      // if the element was structured then it was flattened => change of the delimiter from '.' to '_'
      // this is done in the flattening, but as we do not alter the onCond itself there should be done here as well
      const assocName = originalAssocName.replace(/\./g, pathDelimiter);
      elemName = elemName.replace(/\./g, pathDelimiter);

      assoc.keys.forEach((k) => {
        // Depending on naming conventions, the foreign key may two path steps (hdbcds) or be a single path step with a flattened name (plain, quoted)
        // With to.hdbcds in conjunction with hdbcds naming, we need to NOT use the alias - else we get deployment errors
        const keyName = k.as && doA2J ? [k.as] : k.ref;
        const fKeyPath = !doA2J ? [ assocName, ...keyName ] : [ `${ assocName }${ pathDelimiter }${ keyName[0] }` ];
        // FIXME: _artifact to the args ???
        const a = [
          {
            ref: [ elemName, ...fKeyPath ],
          },
          { ref: k.ref },
        ];

        conditions.push([ a[0], '=', a[1] ]);
      });

      const result = conditions.reduce((prev, current) => {
        if (prev.length === 0)
          return [ ...current ];

        return [ ...prev, 'and', ...current ];
      }, []);

      return result;
    }

    // For a condition `<elemName>.<assoc> = $self` in the ON-condition of element <elemName>,
    // where <assoc> is an unmanaged association, return the ON-condition of <assoc> as it would
    // be written from the perspective of the artifact containing association <elemName>.
    // For example, `ON elem.ass = $self` becomes `ON a = elem.x AND b = elem.y`
    // (assuming that `ass` has the ON-condition `ON ass.a = x AND ass.b = y`)
    function transformDollarSelfComparisonWithUnmanagedAssoc(assocOp, assoc, originalAssocName, elemName) {
      // if the element was structured then it may have been flattened => change of the delimiter from '.' to '_'
      // this is done in the flattening, but as we do not alter the onCond itself there should be done here as well
      elemName = elemName.replace(/\./g, pathDelimiter);
      const assocName = originalAssocName.replace(/\./g, pathDelimiter);
      // clone the onCond for later use in the path transformation,
      // also assign the _artifact elements of the path elements to the copy
      const newOnCond = cloneWithTransformations(assoc.on, {
        ref: (value) => cloneWithTransformations(value, {}),
      });
      // goes through the the newOnCond and transform all the 'path' elements
      forEachRef(newOnCond, (ref) => {
        if (ref[0] === assocName) // we are in the "path" from the forwarding assoc => need to remove the first part of the path
        {
          ref.shift();
        }
        else { // we are in the backlink assoc "path" => need to push at the beginning the association's id
          ref.unshift(elemName);
          // if there was a $self identifier in the forwarding association onCond
          // we do not need it any more, as we prepended in the previous step the back association's id
          if (ref[1] === '$self')
            ref.splice(1, 1);
        }
      });
      return newOnCond;
    }
  }

  /**
   * @todo: XSN - Implementation most likely too naive, can we rely on query.SELECT.mixin?
   *
   * @param {CSN.Query} query
   * @param {object} columnMap
   * @param {CSN.Artifact} columnMap
   * @param {string} elementName
   */
  function checkIsNotMixinByItself(query, columnMap, element, elementName) {
    if (query && query.SELECT && query.SELECT.mixin) {
      const col = columnMap[elementName];

      const realName = col.ref[col.ref.length - 1];
      // If the element is not part of the mixin => True
      return query.SELECT.mixin[realName] == undefined;
    }
    // the artifact does not define any mixins, the element cannot be a mixin
    return true;
  }

  /**
   * @param {CSN.Artifact} artifact
   * @param {string} artifactName
   */
  function checkTypeParameters(artifact, artifactName) {
    forEachMemberRecursively(artifact, (member, memberName, prop, path) => {
      // Check type parameters (length, precision, scale ...)
      if (!member._ignore && member.type)
        _check(member, memberName, csn, path);

      if (!member._ignore && member.items && member.items.type)
        _check(member.items, memberName, csn, path.concat([ 'items' ]));
    }, [ 'definitions', artifactName ]);

    // Check that required actual parameters on 'node.type' are set, that their values are in the correct range etc.
    function _check(node, nodeName, model, path) {
      if (node.type) {
        const absolute = node.type;
        const parameters = node.parameters || [];
        // :FIXME: Is this dead code? node.parameters is always undefined...
        // forHana tested against the parameters of the type definition which is not available in CSN
        for (const name in parameters) {
          const param = parameters[name];
          if (!node[param] && absolute !== 'cds.hana.ST_POINT' && absolute !== 'cds.hana.ST_GEOMETRY')
            error('missing-type-parameter', path, { name: param, id: absolute, $reviewed: false });
        }
        switch (absolute) {
          case 'cds.String':
          case 'cds.Binary':
          case 'cds.hana.VARCHAR': {
            checkTypeParamValue(node, 'length', { min: 1, max: 5000 }, path);
            break;
          }
          case 'cds.Decimal': {
            // Don't check with "plain"?
            if (node.precision || node.scale) {
              checkTypeParamValue(node, 'precision', { max: 38 }, path);
              checkTypeParamValue(node, 'scale', { max: node.precision }, path);
            }
            break;
          }

          case 'cds.hana.BINARY':
          case 'cds.hana.NCHAR':
          case 'cds.hana.CHAR': {
            checkTypeParamValue(node, 'length', { min: 1, max: 2000 }, path);
            break;
          }
          case 'cds.hana.ST_POINT':
          case 'cds.hana.ST_GEOMETRY': {
            checkTypeParamValue(node, 'srid', { max: Number.MAX_SAFE_INTEGER }, path);
            break;
          }
        }
      }

      // Check that the value of the type property `paramName` (e.g. length, precision, scale ...) is of `expectedType`
      // (which can currently only be 'positiveInteger') and (optional) the value is in a given range
      function checkTypeParamValue(node, paramName, range = null, path = null) {
        const paramValue = node[paramName];
        if (paramValue == undefined) {
          if(options.toSql || artifact.query || !['cds.Binary','cds.hana.BINARY', 'cds.hana.NCHAR','cds.hana.CHAR'].includes(node.type)) {
            return true;
          } else {
            return error('missing-type-parameter', path, { name: paramName, id: node.type, $reviewed: false });
          }
        }
        if (range) {
          if (isMaxParameterLengthRestricted(node.type) && range.max && paramValue > range.max) {
            error(null, path,
              { prop: paramName, type: node.type, number: range.max, $reviewed: false },
              'Expecting parameter $(PROP) for type $(TYPE) to not exceed $(NUMBER)');
            return false;
          }
          if (range.min && paramValue < range.min) {
            error(null, path,
              { prop: paramName, type: node.type, number: range.min, $reviewed: false },
              'Expecting parameter $(PROP) for type $(TYPE) to be greater than or equal to $(NUMBER)');
            return false;
          }
        }
        return true;
      }
    }
  }

  /**
  * Check if the maximum length of the value of the given type is restricted.
  *
  * @param {string} type
  * @returns {boolean}
  */
  function isMaxParameterLengthRestricted(type) {
    return !(options.toSql && type === 'cds.String' && (options.toSql.dialect === 'sqlite' || options.toSql.dialect === 'plain'));
  }

  /**
   * Flatten and create the foreign key elements of managed associaitons
   *
   * @param {CSN.Artifact} art
   * @param {string} artName
   */
  function handleManagedAssociationFKs(art, artName) {
    if ((art.kind === 'entity' || art.kind === 'view') && doA2J) {
      forAllElements(art, artName, (parent, elements, pathToElements) => {
        if(artName.startsWith('localized.') && pathToElements.length > 3) {
          // In subqueries, the elements of localized views are missing all the important bits and pieces...
          fixBorkedElementsOfLocalized(elements, pathToElements);
        }
        forEachGeneric(parent, 'elements', (element, elemName) => {
          if (isManagedAssociationElement(element)) {
            if (element.keys) {
              // replace foreign keys that are managed associations by their respective foreign keys
              flattenFKs(element, elemName, [ ...pathToElements, elemName ]);
            }
          }
        });
      })
    }
  }


  /**
   * Flattens all foreign keys
   *
   * Structures will be resolved to individual elements with scalar types
   *
   * Associations will be replaced by their respective foreign keys
   *
   * If a structure contains an assoc, this will also be resolved and vice versa
   *
   * @param {*} assoc
   * @param {*} assocName
   * @param {*} path
   */
  function flattenFKs(assoc, assocName, path) {
    let finished = false;
    while(!finished) {
      const newKeys = [];
      finished = processKeys(assoc, assocName, path, newKeys);
      assoc.keys = newKeys;
    }

    function processKeys(assoc, assocName, path, collector) {
      let finished = true;
      for (let i = 0; i < assoc.keys.length; i++) {
        const pathToKey = path.concat([ 'keys', i ]);
        const { art } = inspectRef(pathToKey);
        const { ref } = assoc.keys[i];
        if (isStructured(art)) {
          finished = false;
          // Mark this element to filter it later - not needed after expansion
          setProp(assoc.keys[i], '$toDelete', true);
          const flat = flattenStructuredElement(art, ref[ref.length - 1], [], pathToKey);
          Object.keys(flat).forEach((flatElemName) => {
            const key = assoc.keys[i];
            const clone = cloneCsn(assoc.keys[i], options);
            if (clone.as) {
              const lastRef = clone.ref[clone.ref.length - 1];
              // Cut off the last ref part from the beginning of the flat name
              const flatBaseName = flatElemName.slice(lastRef.length);
              // Join it to the existing table alias
              clone.as += flatBaseName;
              // do not loose the $ref for nested keys
              if(key.$ref){
                let aliasedLeaf = key.$ref[key.$ref.length - 1 ];
                aliasedLeaf += flatBaseName;
                setProp(clone, '$ref', key.$ref.slice(0, key.$ref.length - 1).concat(aliasedLeaf));
              }
            }
            if (clone.ref) {
              clone.ref[clone.ref.length - 1] = flatElemName;
              // Now we need to properly flatten the whole ref
              clone.ref = flattenStructStepsInRef(clone.ref, pathToKey);
            }
            if (!clone.as) {
              clone.as = flatElemName;
              // TODO: can we use $inferred? Does it have other weird side-effects?
              setProp(clone, '$inferredAlias', true);
            }
            // Directly work on csn.definitions - this way the changes take effect in csnRefs/inspectRef immediately
            // Add the newly generated foreign keys to the end - they will be picked up later on
            // Recursive solutions run into call stack issues
            collector.push(clone);
          });
        }
        else if (art.target) {
          finished = false;
          // Mark this element to filter it later - not needed after expansion
          setProp(assoc.keys[i], '$toDelete', true);
          // Directly work on csn.definitions - this way the changes take effect in csnRefs/inspectRef immediately
          // Add the newly generated foreign keys to the end - they will be picked up later on
          // Recursive solutions run into call stack issues
          art.keys.forEach(key => collector.push(cloneAndExtendRef(key, assoc.keys[i], ref)));
        }
        else if (assoc.keys[i].ref && !assoc.keys[i].as) {
          setProp(assoc.keys[i], '$inferredAlias', true);
          assoc.keys[i].as = assoc.keys[i].ref[assoc.keys[i].ref.length - 1];
          collector.push(assoc.keys[i]);
        } else {
          collector.push(assoc.keys[i]);
        }
      }
      return finished;
    }
    assoc.keys = assoc.keys.filter(o => !o.$toDelete);
  }

  function cloneAndExtendRef(key, base, ref) {
    const clone = cloneCsn(base, options);
    if (key.ref) {
      // We build a ref that contains the aliased fk - that element will be created later on, so this ref is not resolvable yet
      // Therefore we keep it as $ref - ref is the non-aliased, resolvable "clone"
      // Later on, after we know that these foreign key elements are created, we replace ref with this $ref
      let $ref;
      if(base.$ref){
        // if a base $ref is provided, use it to correctly resolve association chains
        const refChain =  [base.$ref[base.$ref.length - 1]].concat(key.as || key.ref);
        $ref = base.$ref.slice(0, base.$ref.length - 1).concat(refChain)
      } else {
        $ref = base.ref.concat( key.as || key.ref); // Keep along the aliases
      }
      setProp(clone, '$ref', $ref);
      clone.ref = clone.ref.concat(key.ref);
    }

    if (!clone.as && clone.ref && clone.ref.length > 0) {
      clone.as = ref[ref.length - 1] + pathDelimiter + (key.as || key.ref.join(pathDelimiter));
      // TODO: can we use $inferred? Does it have other weird side-effects?
      setProp(clone, '$inferredAlias', true);
    }
    else {
      clone.as += pathDelimiter + (key.as || key.ref.join(pathDelimiter));
    }

    return clone;
  }

  /**
   * Flatten technical configuration stuff
   *
   * @param {CSN.Artifact} art
   * @param {string} artName Artifact Name
   */
  function flattenIndexes(art, artName) {
    // Flatten structs in indexes (unless explicitly asked to keep structs)
    const tc = art.technicalConfig;
    if ((art.kind === 'entity' || art.kind === 'view') && doA2J) {
      if (tc && tc[dialect]) {
        // Secondary and fulltext indexes
        for (const name in tc[dialect].indexes) {
          const index = tc[dialect].indexes[name];
          if (Array.isArray(index)) {
            const flattenedIndex = [];
            const isFulltextIndex = (index[0] === 'fulltext');
            index.forEach((val, idx) => {
              if (typeof val === 'object' && val.ref) {
                // Replace a reference by references to it's elements, if it is structured
                const path = [ 'definitions', artName, 'technicalConfig', dialect, 'indexes', name, idx ];
                const { art } = inspectRef(path);
                if (!art) {
                  // A reference that has no artifact (e.g. the reference to the index name itself). Just copy it over
                  flattenedIndex.push(val);
                }
                else if (art.elements) {
                  // The reference is structured
                  if (isFulltextIndex)
                    error(null, path, `"${ artName }": A fulltext index can't be defined on a structured element`);
                  // First, compute the name from the path, e.g ['s', 's1', 's2' ] will result in 'S_s1_s2' ...
                  const refPath = flattenStructStepsInRef(val.ref, path);
                  // ... and take this as the prefix for all elements
                  const flattenedElems = flattenStructuredElement(art, refPath, [], ['definitions', artName, 'elements']);
                  Object.keys(flattenedElems).forEach((elem, i, elems) => {
                    // if it's not the first entry, add a ',' ...
                    if (i)
                      flattenedIndex.push(',');
                    // ... then add the flattend element name as a single ref
                    flattenedIndex.push({ ref: [ elem ] });
                    // ... then check if we have to propagate a 'asc'/'desc', omitting the last, which will be copied automatically
                    if ((idx + 1) < index.length && (index[idx + 1] === 'asc' || index[idx + 1] === 'desc') && i < elems.length - 1)
                      flattenedIndex.push(index[idx + 1]);
                  });
                }
                else {
                  // The reference is not structured, so just replace it by a ref to the combined prefix path
                  const refPath = flattenStructStepsInRef(val.ref, path);
                  flattenedIndex.push({ ref: refPath });
                }
              }
              else // it's just some token like 'index', '(' etc. so we copy it over
              {
                flattenedIndex.push(val);
              }
            });
            // Replace index by the flattened one
            tc[dialect].indexes[name] = flattenedIndex;
          }
        }
      }
    }
  }

  /**
   * Loop over all elements and for all unmanaged associations translate
   * <assoc base>.<managed assoc>.<fk> to <assoc base>.<managed assoc>_<fk>
   *
   * Or in other words: Allow using the foreign keys of managed associations in on-conditions
   *
   * @param {CSN.Artifact} artifact Artifact to check
   * @param {string} artifactName Name of the artifact
   */
  function handleManagedAssocStepsInOnCondition(artifact, artifactName) {
    for (const elemName in artifact.elements) {
      const elem = artifact.elements[elemName];
      if (doA2J) {
        // The association is an unmanaged on
        if (!elem.keys && elem.target && elem.on) {
          forEachRef(elem.on, (ref, refOwner, path) => {
            // [<assoc base>.]<managed assoc>.<field>
            if (ref.length > 1) {
              const { links } = inspectRef(path);
              if (links) {
                // eslint-disable-next-line for-direction
                for (let i = links.length - 1; i >= 0; i--) {
                  const link = links[i];
                  // We found the latest managed assoc path step
                  if (link.art && link.art.target && link.art.keys) {
                    // Doesn't work when ref-target (filter condition) or similar is used
                    if (!ref.slice(i).some(refElement => typeof refElement !== 'string')) {
                      // We join the managed assoc with everything following it
                      const sourceElementName = ref.slice(i).join(pathDelimiter);
                      const source = findSource(links, i - 1) || artifact;
                      // allow specifying managed assoc on the source side
                      const fks = link.art.keys.filter(fk => ref[i] + pathDelimiter + fk.ref[0] ===  sourceElementName);

                      if(fks && fks.length >= 1){
                        const fk = fks[0];
                        if(source && source.elements[fk.$generatedFieldName])
                          refOwner.ref = [ ...ref.slice(0, i), fk.$generatedFieldName ];
                      }
                    }
                  }
                }
              }
            }
          }, [ 'definitions', artifactName, 'elements', elemName, 'on' ]);
        }
      }
    }

    /**
     * Find out where the managed association is
     *
     * @param {Array} links
     * @param {Number} startIndex
     * @returns {Object| undefined} CSN definition of the source of the managed association
     *
     */
    function findSource(links, startIndex) {
      for (let i = startIndex; i >= 0; i--) {
        const link = links[i];
        // We found the latest assoc step - now check where that points to
        if (link.art && link.art.target)
          return csn.definitions[link.art.target];
      }

      return undefined;
    }
  }

  /**
   * Create the foreign key elements for a managed association and build the on-condition
   *
   * @param {CSN.Artifact} artifact
   * @param {string} artifactName
   * @param {Object} elem The association to process
   * @param {string} elemName
   * @returns {void}
   */
  function transformManagedAssociation(artifact, artifactName, elem, elemName) {
    // No need to run over this - we already did, possibly because it was referenced in the ON-Condition
    // of another association - see a few lines lower
    if (elem.$fksgenerated)
      return;
    // Generate foreign key elements for managed associations, and assemble an ON-condition with them
    const onCondParts = [];
    let join_with_and = false;
    if(elem.keys.length === 0)
      elem._ignore = true;
    else {
      for (let i = 0; i < elem.keys.length; i++) {
        const foreignKey = elem.keys[i];

      // Assemble left hand side of 'assoc.key = fkey'
        const assocKeyArg = {
          ref: [
            elemName,
          ].concat(foreignKey.ref),
        };

        const fKeyArg = {
          ref: [
            foreignKey.$generatedFieldName,
          ],
        };

        if (join_with_and) { // more than one FK
          onCondParts.push('and');
        }

        onCondParts.push(
        assocKeyArg
      );
        onCondParts.push('=');
        onCondParts.push(fKeyArg);

        if (!join_with_and)
          join_with_and = true;
      }
      elem.on = onCondParts;
    }

    // If the managed association has a 'key' property => remove it as unmanaged assocs cannot be keys
    // TODO: Are there other modifiers (like 'key') that are valid for managed, but not valid for unmanaged assocs?
    if (elem.key)
      delete elem.key;


    // If the managed association has a 'not null' property => remove it
    if (elem.notNull)
      delete elem.notNull;


    // The association is now unmanaged, i.e. actually it should no longer have foreign keys
    // at all. But the processing of backlink associations below expects to have them, so
    // we don't delete them (but mark them as implicit so that toCdl does not render them)
    /* Skip for now - forHana adds this to elements, but it is not part of the resulting CSN
        forHanaNew -> Somehow ends up in the CSN?!
        elem.implicitForeignKeys = true;
    */
    // Remember that we already processed this
    setProp(elem, '$fksgenerated', true);
  }
}


module.exports = {
  transformForHanaWithCsn,
};
