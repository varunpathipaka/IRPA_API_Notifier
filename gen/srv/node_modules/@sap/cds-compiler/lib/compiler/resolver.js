// Compiler phase "resolve": resolve all references

// The resolve phase tries to find the artifacts (and elements) for all
// references in the augmented CSN.  If there are unresolved references, this
// compiler phase fails with an error containing a vector of corresponding
// messages (alternatively, we could just store this vector in the CSN).

// References are resolved according to the scoping rules of CDS specification.
// That means, the first name of a reference path is not only searched in the
// current environments, but also in the parent environments, with the source
// as second-last, and the environment for builtins as the last search
// environment.

// For all type references, we set the property `type._artifact`, the latter is
// the actual type definition.

// If the referred type definition has a `parameters` property, we use it to
// transform the `$typeArgs` property (sibling to the `type` property`) to
// named properties.  See function `resolveTypeExpr` below for details.

// Example 'file.cds' (see './definer.js' for the CSN before "resolve"):
//   type C { elem: String(4); }
//
// The corresponding definition of element "elem" looks as follows:
//   {
//     kind: 'element',
//     name: { id: 'elem', component: 'elem', location: ... }
//     type: { absolute: 'cds.String', _artifact: {...}, path: ...},
//     length: { val: 4, location: <of the number literal> },
//     location: ..., _parent: ...
//   }

'use strict';

const {
  isDeprecatedEnabled,
  isBetaEnabled,
  setProp,
  forEachDefinition,
  forEachMember,
  forEachGeneric,
  forEachInOrder,
} = require('../base/model');
const {
  dictAdd, dictAddArray,
} = require('../base/dictionaries');
const { dictLocation } = require('../base/location');
const { searchName, weakLocation } = require('../base/messages');
const { combinedLocation } = require('../base/location');
const { pushLink } = require('./utils');
const {
  getDefinerFunctions,
  augmentPath,
  splitIntoPath,
} = require('./definer');

const detectCycles = require('./cycle-detector');
const layers = require('./moduleLayers');

const {
  kindProperties, fns, setLink, linkToOrigin, setMemberParent, withAssociation, storeExtension,
  dependsOn, dependsOnSilent,
} = require('./shared');

const annotationPriorities = {
  define: 1, extend: 2, annotate: 2, edmx: 3,
};


// Export function of this file.  Resolve type references in augmented CSN
// `model`.  If the model has a property argument `messages`, do not throw
// exception in case of an error, but push the corresponding error object to
// that property (should be a vector).
function resolve( model ) {
  const { options } = model;
  // Get shared "resolve" functionality and the message function:
  const {
    resolvePath,
    resolveTypeArguments,
    defineAnnotations,
    attachAndEmitValidNames,
  } = fns( model, environment );
  const {
    info, warning, error, message,
  } = model.$messageFunctions;
  const {
    initArtifact,
    lateExtensions,
    projectionAncestor,
  } = getDefinerFunctions(model);
  /** @type {any} may also be a boolean */
  let newAutoExposed = [];

  // behavior depending on option `deprecated`:
  const enableExpandElements = !isDeprecatedEnabled( options, 'noElementsExpansion' );
  // TODO: we should get rid of noElementsExpansion soon; both
  // beta.nestedProjections and beta.universalCsn do not work with it.
  const scopedRedirections
        = enableExpandElements &&
          !isDeprecatedEnabled( options, 'generatedEntityNameWithUnderscore' ) &&
          !isDeprecatedEnabled( options, 'shortAutoexposed' ) &&
          !isDeprecatedEnabled( options, 'longAutoexposed' ) &&
          !isDeprecatedEnabled( options, 'noInheritedAutoexposeViaComposition' ) &&
          !isDeprecatedEnabled( options, 'noScopedRedirections' );
  const autoexposeViaComposition
        = (isDeprecatedEnabled( options, 'noInheritedAutoexposeViaComposition' ))
          ? 'Composition'
          : true;

  return doResolve();

  function doResolve() {
    // Phase 1: check paths in usings:
    forEachGeneric( model, 'sources', resolveUsings );
    // Phase 2: calculate/init view elements & collect views in order:
    forEachDefinition( model, traverseElementEnvironments );
    while (newAutoExposed.length) {
      // console.log( newAutoExposed.map( a => a.name.absolute ) )
      const all = newAutoExposed;
      newAutoExposed = [];
      all.forEach( traverseElementEnvironments );
    }
    newAutoExposed = true;      // internal error if auto-expose after here
    // It might be that we need to call propagateKeyProps() and
    // addImplicitForeignKeys() in Phase 2, as we might need to know the
    // foreign keys in Phase 2 (foreign key access w/o JOINs).

    // Phase 3: calculate keys along simple queries in collected views:
    model._entities.forEach( propagateKeyProps );
    // While most dependencies leading have been added at this point, new
    // cycles could be added later (e.g. via assocs in where conditions),
    // i.e. keep cycle detection with messages at the end (or after phase 4).

    // Phase 4: resolve all artifacts:
    forEachDefinition( model, resolveRefs );
    forEachGeneric( model, 'vocabularies', resolveRefs );
    // for builtin types
    forEachGeneric( model.definitions.cds, '_subArtifacts', chooseAnnotationsInArtifact);
    forEachGeneric( model.definitions['cds.hana'], '_subArtifacts', chooseAnnotationsInArtifact);

    // Phase 5: rewrite associations
    forEachDefinition( model, rewriteSimple );
    // TODO: sequence not good enough with derived type of structure with
    // includes: first "direct" structures, then _entities, then the rest.
    // v2: We might run a silent cycle detection earlier, then we could use the
    // SCC number (_scc.lowlink) to sort.
    model._entities.forEach( rewriteView );
    model._entities.forEach( rewriteViewCheck );
    // Phase 6: apply ANNOTATE on autoexposed entities and unknown artifacts:
    lateExtensions( annotateMembers );
    if (model.extensions)
      model.extensions.map( annotateUnknown );
    // Phase 7: report cyclic dependencies:
    detectCycles( model.definitions, ( user, art, location ) => {
      if (location) {
        error( 'ref-cyclic', [ location, user ], { art }, {
          std: 'Illegal circular reference to $(ART)',
          element: 'Illegal circular reference to element $(MEMBER) of $(ART)',
        });
      }
    });
    return model;
  }

  // Resolve the using declarations in `using`.  Issue
  // error message if the referenced artifact does not exist.
  function resolveUsings( src, topLevel ) {
    if (!src.usings)
      return;
    for (const def of src.usings) {
      if (def.usings)           // using {...}
        resolveUsings( def );
      if (!def.name || !def.name.absolute || def.$inferred === 'LOCALIZED-IGNORED')
        continue;               // using {...}, parse error, USING localized.XYZ
      const art = model.definitions[def.name.absolute];
      if (art && art.$duplicates)
        continue;
      const ref = def.extern;
      const from = (topLevel ? def : src).fileDep;
      if (art || !from || from.realname)   // no error for non-existing ref with non-existing module
        resolvePath( ref, 'global', def ); // TODO: consider FROM for validNames
    }
  }


  //--------------------------------------------------------------------------
  // The central functions for path resolution - must work on-demand
  //--------------------------------------------------------------------------
  // Phase 2: call populateView(), which also works on-demand

  // Return effective search environment provided by artifact `art`, i.e. the
  // `artifacts` or `elements` dictionary.  For the latter, follow the `type`
  // chain and resolve the association `target`.  View elements are calculated
  // on demand.
  function environment( art, location, user, assocSpec ) {
    if (!art)
      return Object.create(null);
    const env = navigationEnv( art, location, user, assocSpec );
    return env && env.elements || Object.create(null);
  }

  function navigationEnv( art, location, user, assocSpec ) {
    let type = effectiveType(art) || art;
    // console.log( info(null, [ art.location, art ], { art, type }, 'ENV')
    //              .toString(), art.elements && Object.keys(art.elements))
    if (type.target) {
      type = resolvePath( type.target, 'target', type );
      if (!type) {
        if (type === 0 && location)
          dependsOn( art, art, (art.target || art.type).location );
        return type;
      }
      // TODO: combine this with setTargetReferenceKey&Co in getPathItem?
      else if (assocSpec === false) { // TODO: else warning for assoc usage
        error( null, [ location, user ], {},
               'Following an association is not allowed in an association key definition' );
      }
      else if (assocSpec && user) {
        dependsOn( user, type, location );
      }
    }
    populateView( type );
    return type;
  }

  // Follow the `type` chain, i.e. derived types and TYPE OF, stop just before
  // built-in types (otherwise, we would loose type parameters).  Return that
  // type and set it as property `_effectiveType` on all artifacts on the chain.
  // TODO: clarify for (query) elements without type: self, not undefined - also for entities!
  // TODO: directly "propagate" (with implicit redirection the targets), also
  // "proxy-copy" elements

  // In v2, the name-resolution relevant properties (elements,items,target) are
  // proxy-copied, i.e. the effectiveType of an artifact is always the artifact
  // itself.  Except for the situation with recursive element expansions; then
  // we have element: 0, and use original final type.
  function effectiveType( art ) {
    if ('_effectiveType' in art)
      return art._effectiveType;

    // console.log(message( null, art.location, art, {}, 'Info','FT').toString())
    const chain = [];
    while (art && !('_effectiveType' in art) &&
           (art.type || art._origin || art.value && art.value.path) &&
           !art.target && !art.enum && !art.elements && !art.items) {
      chain.push( art );
      setProp( art, '_effectiveType', 0 ); // initial setting in case of cycles
      art = directType( art );
    }
    if (art) {
      if ('_effectiveType' in art) { // is the case for builtins
        art = art._effectiveType;
      }
      else {
        setProp( art, '_effectiveType', art );
        if (art.expand && !art.value && !art.elements)
          initFromColumns( art, art.expand );
        // When not blocked (by future origin = false) and not REDIRECTED TO and not MIXIN
        // try to implicitly redirect explicitly provided target:
        else if (art.target && art._origin == null && !art.value && art.kind !== 'mixin')
          redirectImplicitly( art, art );
      }
    }
    chain.reverse();
    if (!art) {
      for (const a of chain)
        setProp( a, '_effectiveType', art );
    }
    else {
      let eType = art;
      if (eType._outer)
        eType = effectiveType( eType._outer );
      // collect the "latest" cardinality (calculate lazyly if necessary)
      let cardinality = art.cardinality ||
          art._effectiveType && (() => getCardinality( art._effectiveType ));
      for (const a of chain) {
        if (a.cardinality)
          cardinality = a.cardinality;
        if (a.expand && expandFromColumns( a, art, cardinality ) ||
            art.target && redirectImplicitly( a, art ) ||
            art.elements && expandElements( a, art, eType ))
          art = a;
        setProp( a, '_effectiveType', art );
      }
    }
    return art;
  }

  function expandFromColumns( elem, assoc, cardinality ) {
    const path = elem.value && elem.value.path;
    if (!path || path.broken)
      return null;
    if (!assoc.target)
      return initFromColumns( elem, elem.expand );
    const { targetMax } = path[path.length - 1].cardinality ||
          (cardinality instanceof Function ? cardinality() : cardinality);
    if (targetMax && (targetMax.val === '*' || targetMax.val > 1))
      elem.items = { location: dictLocation( elem.expand ) };
    return initFromColumns( elem, elem.expand );
  }

  function getCardinality( type ) {
    // only to be called without cycles
    while (type) {
      if (type.cardinality)
        return type.cardinality;
      type = directType( type );
    }
    return {};
  }

  function userQuery( user ) {
    // TODO: we need _query links set by the definer
    while (user._main) {
      if (user.kind === 'select' || user.kind === '$join')
        return user;
      user = user._parent;
    }
    return null;
  }

  // TODO: test it in combination with top-level CAST function
  function directType( art ) {
    // Be careful when using it with art.target or art.enum or art.elements
    if (art._origin || art.builtin)
      return art._origin;
    if (art.type)
      return resolveType( art.type, art );
    // console.log( 'EXPR-IN', art.kind, refString(art.name) )
    if (!art._main || !art.value || !art.value.path)
      return undefined;
    if (art._pathHead && art.value) {
      setProp( art, '_origin', resolvePath( art.value, 'expr', art, null ) );
      return art._origin;
    }
    const query = userQuery( art ) || art._parent;
    if (query.kind !== 'select')
      return undefined;
    // Reached an element in a query which is a simple ref -> return referred artifact
    // TODO: remember that we still have to resolve path arguments and filters
    return setProp( art, '_origin',
                    resolvePath( art.value, 'expr', art, query._combined ) );
    // console.log( 'EXPR-OUT', art.value._artifact.kind, refString(art.val ue._artifact.name) );
  }

  function resolveType( ref, user ) {
    if ('_artifact' in ref)
      return ref._artifact;
    if (ref.scope === 'typeOf') {
      let struct = user;
      while (struct.kind === 'element')
        struct = struct._parent;
      if (struct.kind === 'select') {
        message( 'ref-invalid-typeof', [ ref.location, user ],
                 { keyword: 'type of', '#': struct.kind } );
        // we actually refer to an element in _combined; TODO: return null if
        // not configurable; would produce illegal CSN with sub queries in FROM
      }
      else if (struct !== user._main) {
        message( 'ref-invalid-typeof', [ ref.location, user ],
                 { keyword: 'type of', '#': struct.kind } );
        return setProp( ref, '_artifact', null );
      }
      return resolvePath( ref, 'typeOf', user );
    }
    while (user._outer)         // in items
      user = user._outer;
    if (user.kind === 'event')
      return resolvePath( ref, 'eventType', user );
    if (user.kind === 'param' && user._parent &&
        [ 'action', 'function' ].includes( user._parent.kind ))
      return resolvePath( ref, 'actionParamType', user );
    return resolvePath( ref, 'type', user );
  }

  // Make a view to have elements (remember: wildcard), and prepare that their
  // final type can be resolved, i.e. we know how to resolve select item refs.
  // We do so by first populate views in the FROM clause, then the view query.
  function populateView( art ) {
    if (!art._from || art._status === '_query')
      return;
    const resolveChain = [];
    const fromChain = [ art ];
    while (fromChain.length) {
      const view = fromChain.pop();
      if (view._status === '_query') // already fully resolved (status at def)
        continue;
      resolveChain.push( view );
      for (const from of view._from) {
        if (from._status)       // status at the ref -> illegal recursion -> stop
          continue;
        setProp( from, '_status', '_query' );
        // setProp before resolvePath - Cycle: view V as select from V.toV
        let source = resolvePath( from, 'from', view ); // filter and args in resolveQuery
        // console.log('ST:',msgName(source),from._status)
        if (source && source._main) { // element -> should be assoc
          const type = effectiveType( source );
          source = type && type.target;
        }
        if (source && source._from && source._status !== '_query')
          fromChain.push( source );
      }
    }
    // console.log( resolveChain.map( v => msgName(v)+v._status ) );
    for (const view of resolveChain.reverse()) {
      if (view._status !== '_query' ) { // not already resolved
        setProp( view, '_status', '_query' );
        traverseQueryPost( view.query, false, populateQuery );
        if (view.elements$)     // specified elements
          mergeSpecifiedElements( view );
        if (!view.$entity) {
          model._entities.push( view );
          view.$entity = ++model.$entity;
        }
      }
    }
  }

  function mergeSpecifiedElements( view ) {
    // Later we use specified elements as proxies to inferred of leading query
    for (const id in view.elements) {
      const ielem = view.elements[id];  // inferred element
      const selem = view.elements$[id]; // specified element
      if (!selem) {
        info( 'query-missing-element', [ ielem.name.location, view ], { id },
              'Element $(ID) is missing in specified elements' );
      }
      else {
        for (const prop in selem) {
          // just annotation assignments and doc comments for the moment
          if (prop.charAt(0) === '@' || prop === 'doc')
            ielem[prop] = selem[prop];
        }
        selem.$replacement = true;
      }
    }
    for (const id in view.elements$) {
      const selem = view.elements$[id]; // specified element
      if (!selem.$replacement) {
        error( 'query-unspecified-element', [ selem.name.location, selem ], { id },
               'Element $(ID) does not result from the query' );
      }
    }
  }

  function traverseElementEnvironments( art ) {
    populateView( art );
    environment( art );
    forEachMember( art, traverseElementEnvironments );
  }

  function populateQuery( query ) {
    if (query._combined || !query.from || !query.$tableAliases)
      // already done or $join query or parse error
      return;
    setProp( query, '_combined', Object.create(null) );
    query.$inlines = [];
    forEachGeneric( query, '$tableAliases', resolveTabRef );

    initFromColumns( query, query.columns );
    // TODO: already in definer: complain about EXCLUDING with no wildcard
    // (would have been automatically with a good CDL syntax: `* without (...)`)
    if (query.excludingDict) {
      for (const name in query.excludingDict)
        resolveExcluding( name, query._combined, query.excludingDict, query );
    }
    return;

    function resolveTabRef( alias ) {
      if (alias.kind === 'mixin' || alias.kind === '$self')
        return;
      if (!alias.elements) { // could be false in hierarchical JOIN
        // if (main._block.$frontend!=='json') console.log('TABREF:',alias.name,main,main._block)
        // const tab = alias.path && resolvePath( alias, 'from', query );
        // // if (tab) setProp( alias, '_effectiveType', alias );
        // const elements = alias.query ? alias.query.elements : environment( tab );
        if (!('_origin' in alias) && alias.path) {
          const tab = resolvePath( alias, 'from', query );
          setProp( alias, '_origin', tab && navigationEnv( tab ) );
        }
        alias.elements = Object.create(null); // Set explicitly, as...
        // ...with circular dep to source, no elements can be found.
        const location = alias.path && alias.path.location;
        const qtab = alias._origin;
        if (!qtab || !qtab.elements)
          return;
        forEachGeneric( qtab, 'elements', ( origin, name ) => {
          const elem = linkToOrigin( origin, name, alias, 'elements',
                                     location || origin.name.location );
          elem.kind = '$navElement';
          // elem.name.select = query.name.select;
          if (origin.masked)
            elem.masked = Object.assign( { $inferred: 'nav' }, origin.masked );
        });
      }
      forEachGeneric( { elements: alias.elements }, 'elements', ( elem, name ) => {
        dictAddArray( query._combined, name, elem, null ); // not dictAdd()
      });
    }
  }

  function initElem( elem, query ) {
    if (elem.type && !elem.type.$inferred)
      return;                 // explicit type -> enough or directType()
    if (elem.$inferred) {
      // redirectImplicitly( elem, elem._origin );
      return;
    }
    if (!elem.value || !elem.value.path) // TODO: test $inferred
      return;                 // no value ref or $inferred
    // TODO: what about SELECT from E { $projection.a as a1, a } !!!!!!

    const env = columnEnv( elem._pathHead, query );
    const origin = setProp( elem, '_origin',
                            resolvePath( elem.value, 'expr', elem, env ) );
    // console.log( message( null, elem.location, elem, {art:query}, 'Info','RED').toString(),
    //              elem.value)
    // TODO: make this resolvePath() also part of directType() ?!
    if (!origin)
      return;
    if (elem.foreignKeys) {   // REDIRECTED with explicit foreign keys
      forEachGeneric( elem, 'foreignKeys', (key, name) => initKey( key, name, elem ) );
    }

    // now set things which are necessary for later sub phases:
    const nav = pathNavigation( elem.value );
    if (nav.navigation && nav.item === elem.value.path[elem.value.path.length - 1]) {
      // redirectImplicitly( elem, origin );
      pushLink( nav.navigation, '_projections', elem );
    }
  }

  function initKey( key, name, elem ) {
    setProp( key, '_block', elem._block );
    setMemberParent( key, name, elem ); // TODO: set _block here if not present?
  }

  function expandElements( art, struct, eType ) {
    if (!enableExpandElements)
      return false;
    if (art.elements || art.kind === '$tableAlias' ||
        // no element expansions for "non-proper" types like
        // entities (as parameter types) etc:
        struct.kind !== 'type' && struct.kind !== 'element' && struct.kind !== 'param' &&
        !struct._outer)
      return false;
    if (struct.elements === 0 || isInParents( art, eType )) {
      art.elements = 0;         // circular
      return true;
    }
    const ref = art.type || art.value || art.name;
    const location = ref && ref.location || art.location;
    // console.log( message( null, location, art, {target:struct,art}, 'Info','EXPAND-ELEM')
    //              .toString(), Object.keys(struct.elements))
    for (const name in struct.elements) {
      const orig = struct.elements[name];
      // const orig = elem.kind === '$navElement'
      if (Array.isArray( orig )) // redefinitions
        continue;
      linkToOrigin( orig, name, art, 'elements', weakLocation( location ), true )
        // or should we use orig.location? - TODO: try to find test to see message
        .$inferred = 'expand-element';
    }
    // Set elements expansion status (the if condition is always true, as no
    // elements expansion will take place on artifact with existing other
    // member property):
    if (!art.$expand)
      art.$expand = 'origin';   // if value stays, elements won't appear in CSN
    // TODO: have some art.elements[SYM.$inferred] = 'expand-elements';
    return true;
  }

  /**
   * Return true iff `struct` is `art` or a direct or indirect parent of `art`,
   * we also check for the outer objects of `items`.  (There is no need to
   * check the parents of main artifacts, as these are contexts, services or
   * namespaces, and do not serve as type.)
   */
  function isInParents( art, struct ) {
    if (art === struct)
      return true;
    while (art._outer) {        // for items
      art = art._outer;
      if (art === struct)
        return true;
    }
    while (art._main) {
      art = art._parent;
      if (art === struct)
        return true;
    }
    return false;
  }

  // About Helper property $expand for faster the XSN-to-CSN transformation
  // - null/undefined: artifact, member, items does not contain expanded members
  // - 'origin': all expanded (sub) elements have no new target/on and no new annotations
  //             that value is only on elements, types, and params -> no other members
  //             when set, only on elem/art with expanded elements
  // - 'target': all expanded (sub) elements might only have new target/on, but
  //             no indivual annotations on any (sub) member
  //             when set, traverse all parents where the value has been 'origin' before
  // - 'annotate': at least one inferred (sub) member has an individual annotation,
  //               not counting propagated ones; set up to the definition (main artifact)
  //               (only set with anno on $inferred elem)
  // Usage according to CSN flavor:
  // - gensrc: do not render enferred elements (including expanded elements),
  //           collect annotate statements with value 'annotate'
  // - client: do not render expanded sub elements if artifact/member is no type, has a type,
  //           has $expand = 'origin', and all its _origin also have $expand = 'origin'
  //           (might sometimes render the elements unnecessarily, which is not wrong)
  // - universal: do not render expanded sub elements if $expand = 'origin'
  function setExpandStatus( elem, status ) {
    // set on element
    while (elem._main) {
      elem = elem._parent;
      if (elem.$expand !== 'origin')
        return;
      elem.$expand = status;    // meaning: expanded, containing assocs
      for (let line = elem.items; line; line = line.items)
        line.$expand = status; // to-csn just uses the innermost $expand
    }
  }
  function setExpandStatusAnnotate( elem, status ) {
    for (;;) {
      if (elem.$expand === status)
        return;                 // already set
      elem.$expand = status;    // meaning: expanded, containing annos
      for (let line = elem.items; line; line = line.items)
        line.$expand = status; // to-csn just uses the innermost $expand
      if (!elem._main)
        return;
      elem = elem._parent;
    }
  }

  // Conditions for redirecting target of assoc in elem
  // - we (the elem) are in a service
  // - assoc is not defined in current service
  // - target provided in assoc is not defined in current service
  function redirectImplicitly( elem, assoc ) {
    // PRE: elem has no target, assoc has target prop
    if (elem.kind === '$tableAlias')
      return false;
    setExpandStatus( elem, 'target' );
    let target = resolvePath( assoc.target, 'target', assoc );
    // console.log( info( null, [ elem.location, elem ], {target,art:assoc,name:''+assoc.target},
    //              'RED').toString())
    if (!target)
      return false;             // error in target ref
    const { location } = elem.value || elem.type || elem.name;
    const service = (elem._main || elem)._service;
    if (service &&
        (service !== (assoc._main || assoc)._service || elem === assoc || assoc.kind === 'mixin') &&
        service !== target._service) {
      // console.log('ES:',elem.name.absolute,elem.name.element);
      const elemScope = scopedRedirections && // null if no scoped redirections
                        preferredElemScope( target, service, elem, assoc._main || assoc );
      const exposed = minimalExposure( target, service, elemScope );

      if (!exposed.length && elemScope !== true) {
        const origTarget = target;
        if (isAutoExposed( target ))
          target = createAutoExposed( origTarget, service, elemScope );
        const desc = origTarget._descendants ||
              setLink( origTarget, Object.create(null), '_descendants' );
        if (!desc[service.name.absolute]) // could be the target itself (no repeated msgs)!
          desc[service.name.absolute] = [ target ];
        else
          desc[service.name.absolute].push( target );
      }
      else if (exposed.length === 1) {
        target = exposed[0];
      }
      else {
        message( (elem !== assoc ? 'redirected-implicitly-ambiguous' : 'type-ambiguous-target'),
                 [ (elem.value || elem.name).location, elem ],
                 {
                   target,
                   service,
                   art: definitionScope( target ),
                   sorted_arts: exposed,
                   '#': ( elemScope !== true ? 'std' : 'scoped' ),
                 }, {
                   // eslint-disable-next-line max-len
                   std: 'Target $(TARGET) is exposed in service $(SERVICE) by multiple projections $(SORTED_ARTS) - no implicit redirection',
                   // eslint-disable-next-line max-len
                   scoped: 'Target $(TARGET) is defined in scope $(ART) which exposed in service $(SERVICE) by multiple projections - no implicit redirection',
                 });
        // continuation semantics: no implicit redirections
      }
    }
    if (elem.target) {          // redirection for Association to / Composition of
      if (elem.target._artifact === target) // no change (due to no implicit redirection)
        return true;
      const origin = {
        kind: elem.kind,
        name: elem.name,
        target: elem.target,
        $inferred: 'REDIRECTED',
        location: elem.target.location,
      };
      setLink( elem, origin, '_origin' );
      setLink( origin, elem._parent, '_parent' );
      if (elem._main)           // remark: the param `elem` can also be a type
        setLink( origin, elem._main, '_main' );
      setLink( origin, origin, '_effectiveType' );
      setLink( origin, elem._block, '_block' );
      if (elem.foreignKeys) {
        origin.foreignKeys = elem.foreignKeys;
        delete elem.foreignKeys;
      }
      if (elem.on) {
        origin.on = elem.on;
        delete elem.on;
      }
    }
    elem.target = {
      path: [ { id: target.name.absolute, location } ],
      scope: 'global',
      location,
      $inferred: (target !== assoc.target._artifact ? 'IMPLICIT' : 'rewrite' ),
    };
    setLink( elem.target, target );
    setLink( elem.target.path[0], target );
    return true;
  }

  function preferredElemScope( target, service, elem, assocMain ) {
    const assocScope = definitionScope( assocMain );
    const targetScope = definitionScope( target );
    if (targetScope === assocScope) { // intra-scope in model
      const elemScope = definitionScope( elem._main || elem );
      if (targetScope === target ||  // unscoped target in model
          assocScope === assocMain || // unscoped assoc source in model
          elemScope !== (elem._main || elem)) // scoped assoc source in service
        return elemScope;                     // own scope, then global
    }
    if (targetScope === target)  // unscoped target in model / other service
      return false;              // all (there could be no scoped autoexposed)
    // scoped target in model:
    const exposed = minimalExposure( targetScope, service, false );
    // console.log('PES:',elem.name.absolute,elem.name.element,exposed.map(e=>e.name.absolute))
    if (exposed.length === 1)   // unique redirection for target scope: use that
      return exposed[0];
    // TODO: warning if exposed.length >= 2?  Probably not
    // TODO: use excessive testing for the following
    // Now re-scope according to naming of auto-exposed entity:
    const autoScopeName = autoExposedName( targetScope, service, false );
    const autoScope = model.definitions[autoScopeName];
    // console.log('AEN:',autoScopeName,autoScope&&(autoScope.$inferred || autoScope.kind))
    if (autoScope)
      return autoScope;
    const { location } = service.name;
    const nullScope = {
      kind: 'namespace', name: { absolute: autoScopeName, location }, location,
    };
    model.definitions[autoScopeName] = nullScope;
    initArtifact( nullScope );
    return nullScope;
  }

  // Return projections of `target` in `service`.  Shorted by
  // - first, only consider projections with @cds.redirection.target=true
  // - exclude all indirect projections, i.e. those which are projection on others in list
  //
  // To avoid repeated messages: if already tried to do autoexposure, return
  // autoexposed entity when successful, or `target` otherwise (no/failed autoexposure)
  function minimalExposure( target, service, elemScope ) {
    const descendants = scopedExposure( target._descendants &&
                                        target._descendants[service.name.absolute] ||
                                        [],
                                        elemScope, target );
    const preferred = descendants.filter( ( d ) => {
      const anno = d['@cds.redirection.target'];
      return anno && (anno.val === undefined || anno.val );
    } );
    const exposed = preferred.length ? preferred : descendants;
    if (exposed.length < 2)
      return exposed || [];
    let min = null;
    for (const e of exposed) {
      if (!min || min._ancestors && min._ancestors.includes(e)) {
        min = e;
      }
      else if (!e._ancestors || !e._ancestors.includes( min )) {
        if (elemScope === '')
          return [];
        return exposed;
      }
    }
    return [ min ];
  }

  function isDirectProjection( proj, base ) {
    return proj.kind === 'entity' && // not event
      projectionAncestor( base, proj.params ) && // same params
      // direct proj (TODO: or should we add them to another list?)
      proj.query && proj.query.op && proj.query.op.val === 'SELECT' &&
      proj._from && proj._from.length === 1 &&
      base === resolvePath( proj._from[0], 'from', proj );
  }

  function scopedExposure( descendants, elemScope, target ) {
    if (!elemScope)             // no scoped redirections
      return descendants;
    if (elemScope === true || elemScope === 'auto') {
      // cross-scope navigation, scoped model target, but there is no unique
      // redirection target for target model scope -> unsure redirection scope
      const unscoped = descendants.filter( d => d === definitionScope( d ) );
      if (unscoped.length)      // use unscoped new targets if present
        return unscoped;
      // Need to filter out auto-exposed, otherwise the behavior is
      // processing-order dependent (not storing the autoexposed in
      // _descendents would only be an alternative w/o recompilation)
      return descendants.filter( d => !d['@cds.autoexposed'] );
    }
    // try scope as target first, even if it has @cds.redirection.target: false
    if (isDirectProjection( elemScope, target ))
      return [ elemScope ];
    const scoped = descendants.filter( d => elemScope === definitionScope( d ) );
    if (scoped.length)          // use scoped new targets if present
      return scoped;
    // otherwise return new targets outside any scope
    return descendants.filter( d => d === definitionScope( d ) );
  }

  function isAutoExposed( target ) {
    if (target.$autoexpose !== undefined)
      return target.$autoexpose;
    const origTarget = target;
    const chain = [];
    let source = target._from && resolvePath( target._from[0], 'from', target );
    // query source ref might not have been resolved yet, cycle avoided as
    // setAutoExposed() sets $autoexpose and a second call on same art would
    // return false
    while (target.$autoexpose === undefined && setAutoExposed( target ) && source) {
      // stop at first ancestor with annotation or at non-query entity
      chain.push( target );
      target = source;
      source = target._from && resolvePath( target._from[0], 'from', target );
    }
    const autoexpose = target.$autoexpose;
    if (typeof autoexpose === 'boolean') {
      for (const a of chain)
        a.$autoexpose = autoexpose;
    }
    return origTarget.$autoexpose;
  }

  function setAutoExposed( art ) {
    const anno = art['@cds.autoexpose'];
    if (anno && anno.val !== null) { // XSN TODO: set val, but no location for anno short form
      // @cds.autoexpose:true or @cds.autoexpose:false
      art.$autoexpose = anno.val === undefined || !!anno.val;
      return false;
    }
    // no @cds.autoexpose or @cds.autoexpose:null
    // TODO: introduce deprecated.noInheritedAutoexposeViaComposition
    art.$autoexpose = model.$compositionTargets[art.name.absolute]
      ? autoexposeViaComposition
      : null;
    return true;                // still check for inherited @cds.autoexpose
  }

  // Return the scope of a definition.  It is the last parent of the definition
  // which is not a context/service/namespace, or the definition itself.
  // If inside service, it is the direct child of the (most inner) service.
  function definitionScope( art ) {
    if (art._base)              // with deprecated.generatedEntityNameWithUnderscore
      return art._base;
    let base = art;
    while (art._parent) {
      if (art._parent.kind === 'service')
        return art;
      art = art._parent;
      if (!kindProperties[art.kind].artifacts)
        base = art;
    }
    return base;
  }

  function autoExposedName( target, service, elemScope ) {
    const { absolute } = target.name;
    if (isDeprecatedEnabled( options, 'shortAutoexposed' )) {
      const parent = definitionScope( target )._parent;
      const name = (parent) ? absolute.substring( parent.name.absolute.length + 1 ) : absolute;
      // no need for dedot here (as opposed to deprecated.longAutoexposed), as
      // the name for dependent entities have already been created using `_` then
      return `${ service.name.absolute }.${ name }`;
    }
    if (isDeprecatedEnabled( options, 'longAutoexposed' )) {
      const dedot = isDeprecatedEnabled( options, 'generatedEntityNameWithUnderscore' );
      return `${ service.name.absolute }.${ dedot ? absolute.replace( /\./g, '_' ) : absolute }`;
    }
    const base = definitionScope( target );
    if (base === target)
      return `${ service.name.absolute }.${ absolute.substring( absolute.lastIndexOf('.') + 1 ) }`;
    // for scoped (e.g. calculated) entities, use exposed name of base:
    const exposed = minimalExposure( base, service, elemScope );
    // console.log(exposed.map( a => a.name.absolute ));
    const sbasename = (exposed.length === 1 && exposed[0] !== base) // same with no/failed expose
      ? exposed[0].name.absolute
      : autoExposedName( base, service, elemScope );
    return sbasename + absolute.slice( base.name.absolute.length );
  }

  function createAutoExposed( target, service, elemScope ) {
    const absolute = autoExposedName( target, service, elemScope );
    const autoexposed = model.definitions[absolute];
    if (autoexposed && (autoexposed.kind !== 'namespace' || !scopedRedirections)) {
      if (isDirectProjection( autoexposed, target )) {
        if (options.testMode)
          throw new Error( `Tried to auto-expose ${ target.name.absolute } twice`);
        return autoexposed;
      }
      error( 'duplicate-autoexposed', [ service.name.location, service ],
             { target, art: absolute },
             'Name $(ART) of autoexposed entity for $(TARGET) collides with other definition' );
      info( null, [ target.name.location, target ],
            { art: service },
            'Expose this (or the competing) entity explicitly in service $(ART)' );
      if (autoexposed.$inferred !== 'autoexposed')
        return target;
      const firstTarget = autoexposed.query.from._artifact;
      error( 'duplicate-autoexposed', [ service.name.location, service ],
             { target: firstTarget, art: absolute },
             'Name $(ART) of autoexposed entity for $(TARGET) collides with other definition' );
      info( null, [ firstTarget.name.location, firstTarget ],
            { art: service },
            'Expose this (or the competing) entity explicitly in service $(ART)' );
      autoexposed.$inferred = 'duplicate-autoexposed';
      return target;
    }
    // console.log(absolute)
    const { location } = target.name;
    const from = augmentPath( location, target.name.absolute );
    let art = {
      kind: 'entity',
      name: { location, path: splitIntoPath( location, absolute ), absolute },
      location: target.location,
      query: { location, op: { val: 'SELECT', location }, from },
      $syntax: 'projection',
      $inferred: 'autoexposed',
      '@cds.autoexposed': {
        name: { path: [ { id: 'cds.autoexposed', location } ], location },
      },
    };
    // TODO: do we need to tag the generated entity with elemScope = 'auto'?
    if (autoexposed) {
      Object.assign( autoexposed, art );
      art = autoexposed;
    }
    else {
      model.definitions[absolute] = art;
    }
    setLink( art, service, '_service' );
    setLink( art, model.$internal, '_block' );
    initArtifact( art, !!autoexposed );
    // populate view (phase 2 of resolver has to be repeated as the view was created afterwards)
    populateView( art );
    // TODO: try to set locations of elements locations of orig target elements
    newAutoExposed.push( art );
    return art;
  }

  // TODO: probably do this already in definer.js
  function ensureColumnName( col, query ) {
    if (col.name)
      return col.name.id;
    if (col.inline || col.val === '*')
      return '';
    const path = col.value &&
        (col.value.path || !col.value.args && col.value.func && col.value.func.path);
    if (path) {
      const last = !path.broken && path.length && path[path.length - 1];
      if (last) {
        col.name = { id: last.id, location: last.location, $inferred: 'as' };
        return col.name.id;
      }
    }
    else if (col.value || col.expand) {
      error( 'query-req-name', [ col.value && col.value.location || col.location, query ], {},
             'Alias name is required for this select item' );
    }
    // invent a name for code completion in expression
    col.name = {
      id: '',
      location: col.value && col.value.location || col.location,
      $inferred: 'none',
    };
    return '';
  }

  // TODO: make this function shorter - make part of this (e.g. setting
  // parent/name) also be part of definer.js
  // TODO: query is actually the elemParent, where the new elements are added to
  // top-level: just query, columns
  // inline: + elements (TODO: remove), colParent
  // expand: just query (which is a column/element), columns=array of expand
  function initFromColumns( query, columns, inlineHead = undefined ) {
    const elemsParent = query.items || query;
    if (!inlineHead) {
      elemsParent.elements = Object.create(null);
      if (query._main._leadingQuery === query) // never the case for 'expand'
        query._main.elements = elemsParent.elements;
    }

    for (const col of columns || [ { val: '*' } ]) {
      if (col.val === '*') {
        const siblings = wildcardSiblings( columns, query );
        expandWildcard( col, siblings, inlineHead, query );
      }
      if ((col.expand || col.inline) && !isBetaEnabled( options, 'nestedProjections' )) {
        error( null, [ col.location, query ], { prop: (col.expand ? 'expand' : 'inline') },
               'Unsupported nested $(PROP)' );
      }
      if (!col.value && !col.expand)
        continue;             // error should have been reported by parser
      if (col.inline) {
        col.kind = '$inline';
        col.name = {};
        // a name for this internal symtab entry (e.g. '.2' to avoid clashes
        // with real elements) is only relevant for for `cdsc -R`/debugging
        const q = userQuery( query );
        q.$inlines.push( col );
        // or use userQuery( query ) in the following, too?
        setMemberParent( col, `.${ q.$inlines.length }`, query );
        initFromColumns( query, col.inline, col );
        continue;
      }
      else if (!col.$replacement) {
        const id = ensureColumnName( col, query );
        col.kind = 'element';
        dictAdd( elemsParent.elements, id, col, ( name, location ) => {
          error( 'duplicate-definition', [ location, query ], { name, '#': 'element' } );
        });
        setMemberParent( col, id, query );
      }
    }
    forEachGeneric( query, 'elements', e => initElem( e, query ) );
    return true;
  }

  // col ($replacement set before *)
  // false if two cols have same name
  function wildcardSiblings( columns, query ) {
    const siblings = Object.create(null);
    if (!columns)
      return siblings;

    let seenWildcard = null;
    for (const col of columns) {
      const id = ensureColumnName( col, query );
      if (id) {
        col.$replacement = !seenWildcard;
        siblings[id] = !(id in siblings) && col;
      }
      else if (col.val === '*') {
        seenWildcard = true;
      }
    }
    return siblings;
  }

  // TODO: make struct.* are to be added at place, not sub-wildcards first,
  // see test3/Queries/ExpandInlineCreate/Excluding.cds
  // TODO: disallow $self.elem.* and $self.*, toSelf.* (circular dependency)
  function expandWildcard( wildcard, siblingElements, colParent, query ) {
    const { elements } = query.items || query;
    let location = wildcard.location || query.from && query.from.location || query.location;
    const inferred = query._main.$inferred;
    const excludingDict = (colParent || query).excludingDict || Object.create(null);

    const envParent = wildcard._pathHead; // TODO: rename _pathHead to _pathEnv
    // console.log('S1:',location.line,location.col,
    //             envParent&&!!envParent._origin&&envParent._origin.name)
    const env = columnEnv( envParent, query );
    // console.log('S2:',location.line,location.col,
    //             envParent&&!!envParent._origin&&envParent._origin.name,
    //             Object.keys(env),Object.keys(elements))
    for (const name in env) {
      const navElem = env[name];
      // TODO: if it is an array, filter out those with masked
      if (excludingDict[name] || navElem.masked && navElem.masked.val)
        continue;
      const sibling = siblingElements[name];
      if (sibling) {          // is explicitly provided (without duplicate)
        if (!inferred && !envParent) // not yet for expand/inline
          reportReplacement( sibling, navElem, query );
        if (!sibling.$replacement) {
          sibling.$replacement = true;
          sibling.kind = 'element';
          dictAdd( elements, name, sibling, ( _name, loc ) => {
            // there can be a definition from a previous inline with the same name:
            error( 'duplicate-definition', [ loc, query ], { name, '#': 'element' } );
          });
          setMemberParent( sibling, name, query );
        }
        // else {
        //   sibling.$inferred = 'query';
        // }
      }
      else if (Array.isArray(navElem)) {
        const names = navElem.filter( e => !e.$duplicates)
          .map( e => `${ e.name.alias }.${ e.name.element }` );
        if (names.length) {
          error( 'wildcard-ambiguous', [ location, query ], { id: name, names },
                 'Ambiguous wildcard, select $(ID) explicitly with $(NAMES)' );
        }
      }
      else {
        location = weakLocation( location );
        const origin = envParent ? navElem : navElem._origin;
        const elem = linkToOrigin( origin, name, query, null, location );
        // TODO: check assocToMany { * }
        dictAdd( elements, name, elem, ( _name, loc ) => {
          // there can be a definition from a previous inline with the same name:
          error( 'duplicate-definition', [ loc, query ], { name, '#': 'element' } );
        });
        elem.$inferred = '*';
        elem.name.$inferred = '*';
        if (envParent)
          setWildcardExpandInline( elem, envParent, origin, name, location );
        else
          setElementOrigin( elem, navElem, name, location );
      }
    }
    if (envParent || query.kind !== 'select') {
      // already done in populateQuery (TODO: change that and check whether
      // `*` is allowed at all in definer)
      const user = colParent || query;
      for (const name in user.excludingDict)
        resolveExcluding( name, env, excludingDict, query );
    }
  }

  function reportReplacement( sibling, navElem, query ) {
    // TODO: bring this much less often = only if shadowed elem does not appear
    // in expr and if not projected as other name.
    // Probably needs to be reported at a later phase
    const path = sibling.value && sibling.value.path;
    if (!sibling.target || sibling.target.$inferred || // not explicit REDIRECTED TO
        path && path[path.length - 1].id !== sibling.name.id) { // or renamed
      const { id } = sibling.name;
      if (Array.isArray(navElem)) {
        info( 'wildcard-excluding-many', [ sibling.name.location, query ], { id },
              'This select item replaces $(ID) from two or more sources' );
      }
      else {
        info( 'wildcard-excluding-one', [ sibling.name.location, query ],
              { id, alias: navElem._parent.name.id },
              'This select item replaces $(ID) from table alias $(ALIAS)' );
      }
    }
  }

  function columnEnv( envParent, query ) { // etc.  wildcard._pathHead;
    return (envParent)
      ? environment( directType( envParent ) || envParent )
      : userQuery( query )._combined;
  }

  function resolveExcluding( name, env, excludingDict, user ) {
    if (env[name])
      return;
    /** @type {object} */
    // console.log(name,Object.keys(env),Object.keys(excludingDict))
    const compileMessageRef = info(
      'ref-undefined-excluding', [ excludingDict[name].location, user ], { name },
      'Element $(NAME) has not been found'
    );
    attachAndEmitValidNames( compileMessageRef, env );
  }

  function setElementOrigin( queryElem, navElem, name, location ) {
    const sourceElem = navElem._origin;
    const alias = navElem._parent;
    // always expand * to path with table alias (reason: columns current_date etc)
    const path = [ { id: alias.name.id, location }, { id: name, location } ];
    queryElem.value = { path, location };
    setProp( path[0], '_navigation', alias );
    setProp( path[0], '_artifact', alias._origin );
    setProp( path[1], '_artifact', sourceElem );
    // TODO: or should we set the _artifact/_effectiveType directly to the target?
    setProp( queryElem.value, '_artifact', sourceElem );
    pushLink( navElem, '_projections', queryElem );
    // TODO: _effectiveType?
  }

  function setWildcardExpandInline( queryElem, pathHead, origin, name, location ) {
    setProp( queryElem, '_pathHead', pathHead );
    const path = [ { id: name, location } ];
    queryElem.value = { path, location }; // TODO: can we omit that?  We have _origin
    setProp( path[0], '_artifact', origin );
    setProp( queryElem, '_origin', origin );
    // TODO: set _projections when top-level?
  }

  //--------------------------------------------------------------------------
  // Phase 3: calculate propagated KEYs
  //--------------------------------------------------------------------------

  function propagateKeyProps( view ) {
    // Second argument true ensure that `key` is only propagated along simple
    // view, i.e. ref or subquery in FROM, not UNION or JOIN.
    traverseQueryPost( view.query, true, ( query ) => {
      if (!withExplicitKeys( query ) && inheritKeyProp( query ) &&
          withKeyPropagation( query )) // now the part with messages
        inheritKeyProp( query, true );
    } );
  }

  function withExplicitKeys( query ) {
    for (const name in query.elements) {
      const elem = query.elements[name];
      if (elem.key && !elem.$duplicates) // also those from includes
        return true;
    }
    return false;
  }

  function inheritKeyProp( query, doIt ) {
    for (const name in query.elements) {
      const elem = query.elements[name];
      // no key prop for duplicate elements or additional specified elements:
      if (elem.$duplicates || !elem.value)
        continue;
      const nav = pathNavigation( elem.value );
      if (!nav.navigation)
        continue;            // undefined, expr, $magic, :const, $self (!), $self.elem
      const { item } = nav;
      if (item !== elem.value.path[elem.value.path.length - 1])
        continue;         // having selected a sub elem / navigated along assoc
      const { key } = item._artifact;
      if (key) {
        if (!doIt)
          return true;
        elem.key = { location: elem.value.location, val: key.val, $inferred: 'query' };
      }
    }
    return false;
  }

  function primarySourceNavigation( aliases ) {
    for (const name in aliases)
      return aliases[name].elements;
    return undefined;
  }

  function withKeyPropagation( query ) {
    const { from } = query;
    if (!from)                  // parse error SELECT FROM <EOF>
      return false;

    let propagateKeys = true;   // used instead early RETURN to get more messages
    const toMany = withAssociation( from, targetMaxNotOne, true );
    if (toMany) {
      propagateKeys = false;
      info( 'query-from-many', [ toMany.location, query ], { art: toMany },
            {
              // eslint-disable-next-line max-len
              std: 'Selecting from to-many association $(ART) - key properties are not propagated',
              // eslint-disable-next-line max-len
              element: 'Selecting from to-many association $(MEMBER) of $(ART) - key properties are not propagated',
            } );
    }
    // Check that all keys from the source are projected:
    const notProjected = [];    // we actually push to the array
    const navElems = primarySourceNavigation( query.$tableAliases );
    for (const name in navElems) {
      const nav = navElems[name];
      if (nav.$duplicates)
        continue;
      const { key } = nav._origin;
      if (key && key.val && !(nav._projections && nav._projections.length))
        notProjected.push( nav.name.id );
    }
    if (notProjected.length) {
      propagateKeys = false;
      info( 'query-missing-keys', [ from.location, query ], { names: notProjected },
            {
              std: 'Keys $(NAMES) have not been projected - key properties are not propagated',
              one: 'Key $(NAMES) has not been projected - key properties are not propagated',
            } );
    }
    // Check that there is no to-many assoc used in select item:
    for (const name in query.elements) {
      const elem = query.elements[name];
      if (!elem.$inferred && elem.value &&
          testExpr( elem.value, selectTest, () => false ))
        propagateKeys = false;
    }
    return propagateKeys;

    function selectTest( expr ) {
      const art = withAssociation( expr, targetMaxNotOne );
      if (art) {
        info( 'query-navigate-many', [ art.location, query ], { art },
              {
                // eslint-disable-next-line max-len
                std: 'Navigating along to-many association $(ART) - key properties are not propagated',
                // eslint-disable-next-line max-len
                element: 'Navigating along to-many association $(MEMBER) of $(ART) - key properties are not propagated',
                // eslint-disable-next-line max-len
                alias: 'Navigating along to-many mixin association $(MEMBER) - key properties are not propagated',
              } );
      }
      return art;
    }
  }

  //--------------------------------------------------------------------------
  // Phase 4:
  //--------------------------------------------------------------------------

  function adHocOrMainKind( elem ) {
    const main = elem._main;
    if (main) {
      do {
        elem = elem._parent;
        if (elem.targetAspect)
          return 'aspect';        // ad-hoc composition target aspect
      } while (elem !== main);
    }
    return elem.kind;
  }
  // TODO: have $applied/$extension/$status on extension with the following values
  //  - 'unknown': artifact to extend/annotate is not defined or contains unknown member
  //  - 'referred': contains annotation for element of referred type (not yet supported)
  //  - 'inferred': only contains extension for known member, but some inferred ones
  //    (inferred = elements from structure includes, query elements)
  //  - 'original': only contains extensions on non-inferred members

  // Resolve all references in artifact or element `art`.  Do so recursively in
  // all sub elements.
  // TODO: make this function smaller
  function resolveRefs( art ) {
    // console.log(message( null, art.location, art, {}, 'Info','REFS').toString())
    // console.log(message( null, art.location, art, {target:art.target}, 'Info','RR').toString())
    const parent = art._parent;
    const allowedInMain = [ 'entity', 'aspect' ].includes( adHocOrMainKind( art ) );
    const isTopLevelElement = parent && (parent.kind !== 'element' || parent.targetAspect);
    if (art.key && art.key.val && !art.key.$inferred && !(allowedInMain && isTopLevelElement)) {
      warning( 'unexpected-key', [ art.key.location, art ],
               { '#': allowedInMain ? 'sub' : 'std' }, {
                 std: 'KEY is only supported for elements in an entity or an aspect',
                 sub: 'KEY is only supported for top-level elements',
               });
    }
    if (art.targetAspect && !(allowedInMain && isTopLevelElement)) {
      message( 'type-managed-composition', [ art.targetAspect.location, art ],
               { '#': allowedInMain ? 'sub' : 'std' } );
    }
    if (art.includes && !allowedInMain) {
      for (const include of art.includes) {
        const struct = include._artifact;
        if (struct && struct.kind !== 'type' && struct.elements &&
            Object.values( struct.elements ).some( e => e.targetAspect)) {
          message( 'type-managed-composition', [ include.location, art ],
                   { '#': struct.kind, art: struct } );
        }
      }
    }
    let obj = art;
    if (obj.type)             // TODO: && !obj.type.$inferred ?
      resolveTypeExpr( obj, art );
    const type = effectiveType( obj ); // make sure implicitly redirected target exists
    if (!obj.items && type && type.items && enableExpandElements) {
      const items = {
        location: weakLocation( (obj.type || obj).location ),
        $inferred: 'expand-items',
      };
      setProp( items, '_outer', obj );
      setProp( items, '_origin', type.items );
      obj.items = items;
      obj.$expand = 'origin';
    }
    if (obj.items) {            // TODO: make this a while in v2 (also items proxy)
      obj = obj.items || obj; // the object which has type properties
      if (enableExpandElements)
        effectiveType(obj);
    }
    if (obj.type) {             // TODO: && !obj.type.$inferred ?
      if (obj !== (art.returns || art)) // not already checked
        resolveTypeExpr( obj, art );
      // typeOf unmanaged assoc?
      const elemtype = obj.type._artifact;
      if (elemtype) {
        if (elemtype.on && !obj.on)
          obj.on = { $inferred: 'rewrite' };
        if (elemtype.targetAspect) {
          error( 'composition-as-type-of', [ obj.type.location, art ], {},
                 'A managed aspect composition element can\'t be used as type' );
          return;
        }
        else if (elemtype.on) {
          error( 'assoc-as-type-of', [ obj.type.location, art ], {},
                 'An unmanaged association can\'t be used as type' );
          return;
        }

        // Check if relational type is missing its target or if it's used directly.
        if (elemtype.category === 'relation' && obj.type.path.length > 0 &&
          !obj.target && !obj.targetAspect) {
          const isCsn = (obj._block && obj._block.$frontend === 'json');
          error('type-missing-target', [ obj.type.location, obj ],
                { '#': isCsn ? 'csn' : 'std', type: elemtype.name.absolute }, {
                  // We don't say "use 'association to <target>" because the type could be used
                  // in action parameters, etc. as well.
                  std: 'The type $(TYPE) can\'t be used directly because it\'s compiler internal',
                  csn: 'Type $(TYPE) is missing a target',
                });
        }
      }
    }
    if (obj.target) {
      // console.log(obj.name,obj._origin.name)
      if (obj._origin && obj._origin.$inferred === 'REDIRECTED')
        resolveTarget( art, obj._origin );
      // console.log(message( null, obj.location, obj, {target:obj.target}, 'Info','TARGET')
      //             .toString(), obj.target.$inferred)
      if (!obj.target.$inferred || obj.target.$inferred === 'aspect-composition')
        resolveTarget( art, obj );
      else
        // TODO: better write when inferred target must be redirected
        resolveRedirected( obj, obj.target._artifact );
    }
    else if (obj.kind === 'mixin') {
      error( 'non-assoc-in-mixin', [ (obj.type || obj.name).location, art ], {},
             'Only unmanaged associations are allowed in mixin clauses' );
    }
    if (art.targetElement) {    // in foreign keys
      const target = parent && parent.target;
      if (target && target._artifact) {
        // we just look in target for the path
        // TODO: also check that we do not follow associations? no args, no filter
        resolvePath( art.targetElement, 'targetElement', art,
                     environment( target._artifact ), target._artifact );
      }
    }
    // Resolve projections/views
    // if (art.query)console.log( info( null, [art.query.location,art.query], 'VQ:' ).toString() );
    // TODO: here, any order should be ok, i.e. just loop over $queries
    traverseQueryPost( art.query, false, resolveQuery );

    if (obj.type || obj._origin || obj.value && obj.value.path || obj.elements) // typed artifacts
      effectiveType(obj);  // set _effectiveType if appropriate, (future?): copy elems if extended

    if (obj.elements) {           // silent dependencies
      forEachGeneric( obj, 'elements', elem => dependsOnSilent( art, elem ) );
    }
    else if (obj.targetAspect && obj.targetAspect.elements) { // silent dependencies
      forEachGeneric( obj.targetAspect, 'elements', elem => dependsOnSilent( art, elem ) );
    }
    if (obj.foreignKeys) {       // silent dependencies
      forEachGeneric( obj, 'foreignKeys', (elem) => {
        dependsOnSilent( art, elem );
      } );
      addForeignKeyNavigations( art );
    }

    resolveExpr( art.default, 'default', art );
    resolveExpr( art.value, 'expr', art, undefined, art.expand || art.inline );
    if (art.value && !art.type && !art.target && !art.elements)
      inferTypeFromCast( art );

    if (art.kind === 'element' || art.kind === 'mixin')
      effectiveType( art );

    annotateMembers( art );     // TODO recheck - recursively, but also forEachMember below
    chooseAnnotationsInArtifact( art );

    forEachMember( art, resolveRefs, art.targetAspect );

    // Set '@Core.Computed' in the Core Compiler to have it propagated...
    if (art.kind !== 'element' || art['@Core.Computed'])
      return;
    if (art.virtual && art.virtual.val ||
        art.value &&
        (!art.value._artifact || !art.value.path || // in localization view: _artifact, but no path
         [ 'builtin', 'param' ].includes( art.value._artifact.kind ))) {
      art['@Core.Computed'] = {
        name: {
          path: [ { id: 'Core.Computed', location: art.location } ],
          location: art.location,
        },
        $inferred: 'computed',
      };
    }
  }

  function inferTypeFromCast( elem ) {
    // TODO: think about CAST checks in checks.js
    const { op, type } = elem.value;
    if (op && op.val === 'cast' && type && type._artifact) {
      // op.val is also correctly set with CSN input
      elem.type = { ...type, $inferred: 'cast' };
      setProp( elem.type, '_artifact', type._artifact );
      for (const prop of [ 'length', 'precision', 'scale', 'srid' ]) {
        if (elem.value[prop])
          elem[prop] = { ...elem.value[prop], $inferred: 'cast' };
      }
    }
  }

  // Phase 4 - annotations ---------------------------------------------------

  function annotateUnknown( ext ) {
    // extensions may have annotations for elements/actions/... which may
    // themselves may be unknown
    forEachMember(ext, annotateUnknown);

    if (ext.$extension) // extension for known artifact -> already applied
      return;
    annotateMembers( ext );
    for (const prop in ext) {
      if (prop.charAt(0) === '@')
        chooseAssignment( prop, ext );
    }
  }

  function annotateMembers( art, extensions = [], prop, name, parent, kind ) {
    const showMsg = !art && parent && parent.kind !== 'annotate';
    if (!art && extensions.length) {
      if (Array.isArray( parent ))
        return;
      const parentExt = extensionFor(parent);
      art = parentExt[prop] && parentExt[prop][name];
      if (!art) {
        art = {
          kind,                 // for setMemberParent()
          name: { id: name, location: extensions[0].name.location },
          location: extensions[0].location,
        };
        setMemberParent( art, name, parentExt, prop );
        art.kind = 'annotate';  // after setMemberParent()!
      }
    }

    for (const ext of extensions) {
      if ('_artifact' in ext.name) // already applied
        continue;
      setProp( ext.name, '_artifact', art );

      if (art) {
        defineAnnotations( ext, art, ext._block, ext.kind );
        // eslint-disable-next-line no-shadow
        forEachMember( ext, ( elem, name, prop ) => {
          storeExtension( elem, name, prop, art, ext._block );
        });
      }
      if (showMsg) {
        // somehow similar to checkDefinitions():
        const feature = kindProperties[parent.kind][prop];
        if (prop === 'elements' || prop === 'enum') {
          if (!feature) {
            warning( 'anno-unexpected-elements', [ ext.name.location, art ], {},
                     'Elements only exist in entities, types or typed constructs' );
          }
          else {
            notFound( 'anno-undefined-element', ext.name.location, art,
                      { art: searchName( parent, name, parent.enum ? 'enum' : 'element' ) },
                      parent.elements || parent.enum );
          }
        }
        else if (prop === 'actions') {
          if (!feature) {
            warning( 'anno-unexpected-actions', [ ext.name.location, art ], {},
                     'Actions and functions only exist top-level and for entities' );
          }
          else {
            notFound( 'anno-undefined-action', ext.name.location, art,
                      { art: searchName( parent, name, 'action' ) },
                      parent.actions );
          }
        }
        else if (!feature) {
          warning( 'anno-unexpected-params', [ ext.name.location, art ], {},
                   'Parameters only exist for actions or functions' );
        } // TODO: entities betaMod
        else {
          notFound( 'anno-undefined-param', ext.name.location, art,
                    { art: searchName( parent, name, 'param' ) },
                    parent.params );
        }
      }
    }
    if (art && art._annotate) {
      if (art.kind === 'action' || art.kind === 'function') {
        expandParameters( art );
        if (art.returns)
          effectiveType( art.returns );
      }
      const aor = art.returns || art;
      const obj = aor.items || aor.targetAspect || aor;
      // Currently(?), effectiveType() does not calculate the effective type of
      // its line item:
      effectiveType( obj );
      if (art._annotate.elements)
        setExpandStatusAnnotate( aor, 'annotate' );
      annotate( obj, 'element', 'elements', 'enum', art );
      annotate( art, 'action', 'actions' );
      annotate( art, 'param', 'params' );
      // const { returns } = art._annotate;
      // if (returns) {
      //   const dict = returns.elements;
      //   const env = obj.returns && obj.returns.elements || null;
      //   for (const n in dict)
      //     annotateMembers( env && env[n], dict[n], 'elements', n, parent, 'element' );
      // }
    }
    return;

    function notFound( msgId, location, address, args, validDict ) {
      // TODO: probably move this to shared.js and use for EXTEND, too
      const msg = message( msgId, [ location, address ], args );
      attachAndEmitValidNames(msg, validDict);
    }

    // eslint-disable-next-line no-shadow
    function annotate( obj, kind, prop, altProp, parent = obj ) {
      const dict = art._annotate[prop];
      const env = obj[prop] || altProp && obj[altProp] || null;
      for (const n in dict)
        annotateMembers( env && env[n], dict[n], prop, n, parent, kind );
    }
  }
  function expandParameters( action ) {
    // see also expandElements()
    if (!enableExpandElements || !effectiveType( action ))
      return;
    const chain = [];
    // Should we be able to consider params and returns separately?
    // Probably not, let to-csn omit unchanged params/returns.
    while (action._origin && !action.params) {
      chain.push( action );
      action = action._origin;
    }
    chain.reverse();
    for (const art of chain) {
      const origin = art._origin;
      if (!art.params && origin.params) {
        for (const name in origin.params) {
          // TODO: we could check _annotate here to decide whether we really
          // not to create proxies
          const orig = origin.params[name];
          linkToOrigin( orig, name, art, 'params', weakLocation( orig.location ), true )
            .$inferred = 'expand-param';
        }
      }
      if (!art.returns && origin.returns) {
        // TODO: make linkToOrigin() work for returns, kind/name?
        const location = weakLocation( origin.returns.location );
        art.returns = {
          name: Object.assign( {}, art.name, { id: '', param: '', location } ),
          kind: 'param',
          location,
          $inferred: 'expand-param',
        };
        setProp( art.returns, '_origin', origin.returns );
      }
    }
  }

  function extensionFor( art ) {
    if (art.kind === 'annotate')
      return art;
    if (art._extension)
      return art._extension;

    // $extension means: already applied
    const ext = {
      kind: art.kind,           // set kind for setMemberParent()
      $extension: 'exists',
      location: art.location,    // location( extension to existing art ) = location(art)
    };
    const { location } = art.name;
    if (!art._main) {
      ext.name = {
        path: [ { id: art.name.absolute, location } ],
        location,
        absolute: art.name.absolute,
      };
      if (model.extensions)
        model.extensions.push(ext);
      else
        model.extensions = [ ext ];
    }
    else {
      ext.name = { id: art.name.id, location };
      const parent = extensionFor( art._parent );
      const kind = kindProperties[art.kind].normalized || art.kind;
      // enums would be first in elements
      if ( parent[kindProperties[kind].dict] &&
           parent[kindProperties[kind].dict][art.name.id] )
        throw new Error(art.name.id);
      setMemberParent( ext, art.name.id, parent, kindProperties[kind].dict );
    }
    ext.kind = 'annotate';    // after setMemberParent()!
    setProp( art, '_extension', ext );
    setProp( ext.name, '_artifact', art );
    if (art.returns)
      ext.$syntax = 'returns';
    return ext;
  }

  /**
   * Goes through all (applied) annotations in the given artifact and chooses one
   * if multiple exist according to the module layer.
   *
   * @param {XSN.Artifact} art
   */
  function chooseAnnotationsInArtifact( art ) {
    for (const prop in art) {
      if (prop.charAt(0) === '@')
        chooseAssignment( prop, art );
    }
  }

  function chooseAssignment( annoName, art ) {
    // TODO: getPath an all names
    const anno = art[annoName];
    if (!Array.isArray(anno)) { // just one assignment -> use it
      if (removeEllipsis( anno )) {
        error( 'anno-unexpected-ellipsis',
               [ anno.name.location, art ], { code: '...' } );
      }
      return;
    }
    // sort assignment according to layer
    const layerAnnos = Object.create(null);
    for (const a of anno) {
      const layer = layers.layer( a._block );
      const name = (layer) ? layer.realname : '';
      const done = layerAnnos[name];
      if (done)
        done.annos.push( a );
      else
        layerAnnos[name] = { layer, annos: [ a ] };
    }
    mergeArrayInSCCs();
    art[annoName] = mergeLayeredArrays( findLayerCandidate( ) );
    return;

    function mergeArrayInSCCs( ) {
      let pos = 0;
      Object.values( layerAnnos ).forEach( (layer) => {
        const mergeSource
        = layer.annos.find(v => (v.$priority === undefined ||
           annotationPriorities[v.$priority] === annotationPriorities.define));
        if (mergeSource) {
          if (removeEllipsis( mergeSource )) {
            error( 'anno-unexpected-ellipsis',
                   [ mergeSource.name.location, art ], { code: '...' } );
          }
          // merge source into elipsis array annotates
          layer.annos.forEach( (mergeTarget) => {
            if (mergeTarget.$priority &&
                annotationPriorities[mergeTarget.$priority] > annotationPriorities.define) {
              pos = findEllipsis( mergeTarget );
              if (pos > -1) {
                if (mergeSource.literal !== 'array') {
                  error( 'anno-mismatched-ellipsis',
                         [ mergeSource.name.location, art ], { code: '...' } );
                  return;
                }
                mergeTarget.val.splice(pos, 1, ...mergeSource.val);
              }
            }
          });
        }
      });
    }

    function mergeLayeredArrays( mergeTarget ) {
      if (mergeTarget.literal === 'array') {
        let layer = layers.layer( mergeTarget._block );
        delete layerAnnos[(layer) ? layer.realname : ''];
        let pos = findEllipsis( mergeTarget );
        while (pos > -1 && Object.keys( layerAnnos ).length ) {
          const mergeSource = findLayerCandidate();
          if (mergeSource.literal !== 'array') {
            error( 'anno-mismatched-ellipsis',
                   [ mergeSource.name.location, art ], { code: '...' } );
            return mergeTarget;
          }
          mergeTarget.val.splice(pos, 1, ...mergeSource.val);
          layer = layers.layer( mergeSource._block );
          delete layerAnnos[(layer) ? layer.realname : ''];
          pos = findEllipsis( mergeTarget );
        }
        // remove excess ellipsis
        removeEllipsis( mergeTarget, pos );
      }
      return mergeTarget;
    }

    function removeEllipsis(a, pos = findEllipsis( a )) {
      let count = 0;
      while (a.literal === 'array' && pos > -1) {
        count++;
        a.val.splice(pos, 1);
        pos = findEllipsis( a );
      }
      return count;
    }

    function findEllipsis(a) {
      return (a.literal === 'array' && a.val)
        ? a.val.findIndex(v => v.literal === 'token' && v.val === '...') : -1;
    }

    function findLayerCandidate() {
    // collect assignments of upper layers (are in no _layerExtends)
      const exts = Object.keys( layerAnnos ).map( layerExtends );
      const allExtends = Object.assign( Object.create(null), ...exts );
      const collected = [];
      for (const name in layerAnnos) {
        if (!(name in allExtends))
          collected.push( prioritizedAnnos( layerAnnos[name].annos ) );
      }
      // inspect collected assignments - choose the one or signal error
      const justOnePerLayer = collected.every( annos => annos.length === 1);
      if (!justOnePerLayer || collected.length > 1) {
        for (const annos of collected) {
          for (const a of annos ) {
          // Only the message ID is different.
            if (justOnePerLayer) {
              message( 'anno-duplicate-unrelated-layer',
                       [ a.name.location, art ], { anno: annoName },
                       'Duplicate assignment with $(ANNO)' );
            }
            else {
              message( 'anno-duplicate', [ a.name.location, art ], { anno: annoName },
                       'Duplicate assignment with $(ANNO)' );
            }
          }
        }
      }
      return collected[0][0];  // just choose any one with error
    }

    function layerExtends( name ) {
      const { layer } = layerAnnos[name];
      return layer && layer._layerExtends;
    }
  }

  function prioritizedAnnos( annos ) {
    let prio = 0;
    let r = [];
    for (const a of annos) {
      const p = annotationPriorities[a.$priority] || annotationPriorities.define;
      if (p === prio) {
        r.push(a);
      }
      else if (p > prio) {
        r = [ a ];
        prio = p;
      }
    }
    return r;
  }

  // Phase 4 - queries and associations --------------------------------------

  function resolveQuery( query ) {
    if (!query._main)           // parse error
      return;
    populateQuery( query );
    forEachGeneric( query, '$tableAliases', ( alias ) => {
      // console.log( info( null, [alias.location,alias], 'SQA:' ).toString() );
      if (alias.kind === 'mixin')
        resolveRefs( alias );   // mixin element
      else if (alias.kind !== '$self')
        // pure path has been resolved, resolve args and filter now:
        resolveExpr( alias, 'from', query._parent );
    } );
    for (const col of query.$inlines)
      resolveExpr( col.value, 'expr', col, undefined, true );
    // for (const col of query.$inlines)
    //   if (!col.value.path) throw Error(col.name.element)
    if (query !== query._main._leadingQuery) // will be done later
      forEachGeneric( query, 'elements', resolveRefs );
    if (query.from)
      resolveJoinOn( query.from );
    if (query.where)
      resolveExpr( query.where, 'expr', query, query._combined );
    if (query.groupBy)
      resolveBy( query.groupBy, 'expr' );
    resolveExpr( query.having, 'expr', query, query._combined );
    if (query.$orderBy)       // ORDER BY from UNION:
      // TODO clarify: can I access the tab alias of outer queries?  If not:
      // 4th arg query._main instead query._parent.
      resolveBy( query.$orderBy, 'order-by-union', query.elements, query._parent );
    if (query.orderBy) {       // ORDER BY
    // search in `query.elements` after having checked table aliases of the current query
      resolveBy( query.orderBy, 'expr', query.elements );
      // TODO: disallow resulting element ref if in expression!
      // Necessary to check it in the compiler as it might work with other semantics on DB!
      // (we could downgrade it to a warning if name is equal to unique source element name)
      // TODO: Some helping text mentioning an alias name would be useful
    }
    return;

    function resolveJoinOn( join ) {
      if (join && join.args) {  // JOIN
        for (const j of join.args)
          resolveJoinOn( j );
        if (join.on)
          resolveExpr( join.on, 'expr', query, query._combined );
          // TODO: check restrictions according to join "query"
      }
    }

    // Note the strange name resolution (dynamic part) for ORDER BY: the same
    // as for select items if it is an expression, but first look at select
    // item alias (i.e. like `$projection.NAME` if it is a path.  If it is an
    // ORDER BY of an UNION, do not allow any dynamic path in an expression,
    // and only allow the elements of the leading query if it is a path.
    //
    // This seem to be similar, but different in SQLite 3.22.0: ORDER BY seems
    // to bind stronger than UNION (see <SQLite>/src/parse.y), and the name
    // resolution seems to use select item aliases from all SELECTs of the
    // UNION (see <SQLite>/test/tkt2822.test).
    function resolveBy( array, mode, pathDict, q ) {
      for (const value of array ) {
        if (value)
          resolveExpr( value, mode, q || query, value.path && pathDict );
      }
    }
  }

  function resolveTarget( art, obj ) {
    if (art !== obj && obj.on && obj.$inferred !== 'REDIRECTED') {
      message( 'assoc-in-array', [ obj.on.location, art ], {},
               // TODO: also check parameter parent, two messages?
               'An association can\'t be used for arrays or parameters' );
      setProp( obj.target, '_artifact', undefined );
      return;
    }
    const target = resolvePath( obj.target, 'target', art );
    if (obj.on) {
      if (!art._main || !art._parent.elements && !art._parent.items && !art._parent.targetAspect) {
        // TODO: test of .items a bit unclear - we should somehow restrict the
        // use of unmanaged assocs in MANY, at least with $self
        // TODO: $self usage in anonymous aspects to be corrected in Core Compiler
        const isComposition = obj.type && obj.type.path && obj.type.path[0] &&
                              obj.type.path[0].id === 'cds.Composition';
        message( 'assoc-as-type', [ obj.on.location, art ],
                 { '#': isComposition ? 'comp' : 'std' }, {
                   std: 'An unmanaged association can\'t be defined as type',
                   comp: 'An unmanaged composition can\'t be defined as type',
                 });
        // TODO: also warning if inside structure
      }
      else if (obj.$inferred !== 'REDIRECTED') {
        // TODO: extra with $inferred (to avoid messages)?
        // TODO: in the ON condition of an explicitly provided model entity
        // which is going to be implicitly redirected, we can never navigate
        // along associations, even not to the foreign keys (at least if they
        // are renamed) - introduce extra 'expected' which inspects REDIRECTED
        resolveExpr( obj.on, art.kind === 'mixin' ? 'mixin-on' : 'on', art );
      }
      else {
        const elements = Object.create( art._parent.elements );
        elements[art.name.id] = obj;
        resolveExpr( obj.on, art.kind === 'mixin' ? 'mixin-on' : 'on', art, elements );
      }
    }
    else if (art.kind === 'mixin') {
      error( 'assoc-in-mixin', [ obj.target.location, art ], {},
             'Managed associations are not allowed for MIXIN elements' );
    }
    else if (target && !obj.foreignKeys && [ 'entity' ].includes( target.kind )) {
      if (obj.$inferred === 'REDIRECTED') {
        addImplicitForeignKeys( art, obj, target );
      }
      else if (!obj.type || obj.type.$inferred || obj.target.$inferred) { // REDIRECTED
        resolveRedirected( art, target );
      }
      else if (obj.type._artifact && obj.type._artifact.internal) { // cds.Association, ...
        addImplicitForeignKeys( art, obj, target );
      }
      // else console.log( message( null,obj.location,obj, {target}, 'Info','NOTARGET').toString())
    }
    // else console.log( message( null, obj.location, obj, {target}, 'Info','NORE').toString())
  }

  function addImplicitForeignKeys( art, obj, target ) {
    obj.foreignKeys = Object.create(null);
    forEachInOrder( target, 'elements', ( elem, name ) => {
      if (elem.key && elem.key.val) {
        const { location } = art.target;
        const key = {
          name: { location, id: elem.name.id, $inferred: 'keys' }, // more by setMemberParent()
          kind: 'key',
          targetElement: { path: [ { id: elem.name.id, location } ], location },
          location,
          $inferred: 'keys',
        };
        setMemberParent( key, name, art );
        dictAdd( obj.foreignKeys, name, key );
        setProp( key.targetElement, '_artifact', elem );
        setProp( key.targetElement.path[0], '_artifact', elem );
        setProp( key, '_effectiveType', effectiveType(elem) );
        dependsOn(key, elem, location);
        dependsOnSilent(art, key);
      }
    });
  }

  function addForeignKeyNavigations( art ) {
    art.$keysNavigation = Object.create(null);
    forEachGeneric( art, 'foreignKeys', ( key ) => {
      if (!key.targetElement || !key.targetElement.path)
        return;
      let dict = art.$keysNavigation;
      const last = key.targetElement.path[key.targetElement.path.length - 1];
      for (const item of key.targetElement.path) {
        let nav = dict[item.id];
        if (!nav) {
          nav = {};
          dict[item.id] = nav;
          if (item === last)
            setLink( nav, key );
          else
            nav.$keysNavigation = Object.create(null);
        }
        else if (item === last || nav._artifact) {
          error( 'duplicate-key-ref', [ item.location, key ], {},
                 'The same target reference has already been used in a key definition' );
          return;
        }
        dict = nav.$keysNavigation;
      }
    } );
  }

  function resolveRedirected( elem, target ) {
    setProp( elem, '_redirected', null ); // null = do not touch path steps after assoc
    const assoc = directType( elem );
    const origType = assoc && effectiveType( assoc );
    if (!origType || !origType.target) {
      error( 'redirected-no-assoc', [ elem.target.location, elem ], {},
             'Only an association can be redirected' );
      return;
    }
    // console.log(message( null, elem.location, elem, {target,art:assoc}, 'Info','RE')
    //             .toString(), elem.value)
    const nav = elem._main && elem._main.query && elem.value && pathNavigation( elem.value );
    if (nav && nav.item !== elem.value.path[elem.value.path.length - 1]) {
      if (origType.on) {
        error( 'rewrite-not-supported', [ elem.target.location, elem ], {},
               // TODO: Better text ?
               'The ON condition is not rewritten here - provide an explicit ON condition' );
        return;
      }
    }
    const origTarget = origType.target._artifact;
    if (!origTarget || !target)
      return;

    const chain = [];
    if (target === origTarget) {
      if (!elem.target.$inferred) {
        info( 'redirected-to-same', [ elem.target.location, elem ], { art: target },
              'The redirected target is the original $(ART)' );
      }
      setProp( elem, '_redirected', chain ); // store the chain
      return;
    }
    if (elem.foreignKeys || elem.on)
      return;          // TODO: or should we still bring an msg if nothing in common?
    // now check whether target and origTarget are "related"
    while (target.query) {
      const from = target.query.args ? {} : target.query.from;
      if (!from)
        return;                 // parse error - TODO: or UNION?
      if (!from.path) {
        warning( 'redirected-to-complex', [ elem.target.location, elem ],
                 { art: target, '#': target === elem.target._artifact ? 'target' : 'std' },
                 {
                   std: 'Redirection involves the complex view $(ART)',
                   target: 'The redirected target $(ART) is a complex view',
                 });
        break;
      }
      target = from._artifact;
      if (!target)
        return;
      chain.push( from );
      if (target === origTarget) {
        chain.reverse();
        setProp( elem, '_redirected', chain );
        return;
      }
    }
    let redirected = null;
    let news = [ { chain: chain.reverse(), sources: [ target ] } ];
    const dict = Object.create(null);
    while (news.length) {
      const outer = news;
      news = [];
      for (const o of outer) {
        for (const s of o.sources) {
          const art = (s.kind === '$tableAlias') ? s._origin : s;
          if (art !== origTarget) {
            if (findOrig( o.chain, s, art ) && !redirected) // adds to news []
              redirected = false;   // do not report further error
          }
          else if (!redirected) {
            redirected = (s.kind === '$tableAlias') ? [ s, ...o.chain ] : o.chain;
          }
          else {
            error( 'redirected-to-ambiguous', [ elem.target.location, elem ], { art: origTarget },
                   'The redirected target originates more than once from $(ART)' );
            return;
          }
        }
      }
    }
    if (redirected) {
      setProp( elem, '_redirected', redirected );
    }
    else if (redirected == null) {
      error( 'redirected-to-unrelated', [ elem.target.location, elem ], { art: origTarget },
             'The redirected target does not originate from $(ART)' );
    }
    return;

    // B = proj on A, C = A x B, X = { a: assoc to A on a.Q1 = ...}, Y = X.{ a: redirected to C }
    // what does a: redirected to C means?
    // -> collect all elements Qi used in ON (corr: foreign keys)
    // -> only use an tableAlias which has propagation for all elements
    // no - error if the original target can be reached twice
    // even better: disallow complex view (try as error first)

    // eslint-disable-next-line no-shadow
    function findOrig( chain, alias, art ) {
      if (!art || dict[art.name.absolute])
        // some include ref or query source cannot be found, or cyclic ref
        return true;
      dict[art.name.absolute] = true;

      if (art.includes) {
        news.push( {
          chain: [ art, ...chain ],
          sources: art.includes
            .map( r => r._artifact )
            .filter( i => i ),  // _artifact may be `null` if the include cannot be found
        } );
      }
      const query = art._leadingQuery;
      if (!query)
        return false;           // non-query entity
      if (!query.$tableAliases) // previous error in query definition
        return true;
      const sources = [];
      for (const n in query.$tableAliases) {
        const a = query.$tableAliases[n];
        if (a.path && a.kind !== '$self' && a.kind !== 'mixin')
          sources.push( a );
      }
      if (alias.kind === '$tablealias')
        news.push( { chain: [ alias, ...chain ], sources } );
      else
        news.push( { chain, sources } );
      return false;
    }
  }

  //--------------------------------------------------------------------------
  // Phase 5: rewrite associations
  //--------------------------------------------------------------------------
  // Only top-level queries and sub queries in FROM

  function rewriteSimple( art ) {
    // If we have a proper seperation of view elements and elements of the
    // primary query, we can delete this function.
    // return;
    if (!art.includes && !art.query) {
      // console.log(message( null, art.location, art, {target:art._target},
      //   'Info','RAS').toString())
      rewriteAssociation( art );
      forEachGeneric( art, 'elements', rewriteAssociation );
    }
    if (art._service)
      forEachGeneric( art, 'elements', excludeAssociation );
  }

  function rewriteView( view ) {
    traverseQueryPost( view.query, false, ( query ) => {
      forEachGeneric( query, 'elements', rewriteAssociation );
    } );
    if (view.includes)          // entities with structure includes:
      forEachGeneric( view, 'elements', rewriteAssociation );
  }

  // Check explicit ON / keys with REDIRECTED TO
  function rewriteViewCheck( view ) {
    traverseQueryPost( view.query, false, ( query ) => {
      forEachGeneric( query, 'elements', rewriteAssociationCheck );
    } );
  }

  function excludeAssociation( elem ) {
    const target = elem.target && elem.target._artifact;
    if (!target || target._service) // assoc to other service is OK
      return;
    if (!elem.$inferred) {      // && !elem.target.$inferred
      // TODO: spec meeting 2021-01-22: no warning
      warning( 'assoc-target-not-in-service', [ elem.target.location, elem ],
               { target, '#': (elem._main.query ? 'select' : 'define') }, {
                 std: 'Target $(TARGET) of association is outside any service', // not used
                 // eslint-disable-next-line max-len
                 define: 'Target $(TARGET) of explicitly defined association is outside any service',
                 // eslint-disable-next-line max-len
                 select: 'Target $(TARGET) of explicitly selected association is outside any service',
               } );
    }
    else {
      info( 'assoc-outside-service', [ elem.target.location, elem ],
            { target },
            'Association target $(TARGET) is outside any service' );
    }
  }

  function rewriteAssociationCheck( element ) {
    const elem = element.items || element; // TODO v2: nested items
    if (elem.elements && enableExpandElements)
      forEachGeneric( elem, 'elements', rewriteAssociationCheck );
    if (!elem.target)
      return;
    if (elem.on && !elem.on.$inferred) {
      const assoc = directType( elem );
      if (assoc && assoc.foreignKeys) {
        error( 'rewrite-key-for-unmanaged', [ elem.on.location, elem ],
               { keyword: 'on', art: assocWithExplicitSpec( assoc ) },
               // eslint-disable-next-line max-len
               'Do not specify an $(KEYWORD) condition when redirecting the managed association $(ART)' );
      }
    }
    else if (elem.foreignKeys && !inferredForeignKeys( elem.foreignKeys )) {
      const assoc = directType( elem );
      if (assoc && assoc.on) {
        error( 'rewrite-on-for-managed', [ dictLocation( elem.foreignKeys ), elem ],
               { art: assocWithExplicitSpec( assoc ) },
               'Do not specify foreign keys when redirecting the unmanaged association $(ART)' );
      }
      else if (assoc && assoc.foreignKeys) {
        // same sequence is not checked
        rewriteKeysMatch( elem, assoc );
        rewriteKeysCovered( assoc, elem );
      }
    }
  }

  function rewriteKeysMatch( thisAssoc, otherAssoc ) {
    const { foreignKeys } = thisAssoc;
    for (const name in foreignKeys) {
      if (otherAssoc.foreignKeys[name])
        continue;               // we would do a basic type check later
      const key = foreignKeys[name];
      const baseAssoc = assocWithExplicitSpec( otherAssoc );
      if (inferredForeignKeys( baseAssoc.foreignKeys )) { // still inferred = via target keys
        error( 'rewrite-key-not-matched-implicit', [ key.name.location, key ],
               { name, target: baseAssoc.target },
               'No key $(NAME) is defined in original target $(TARGET)' );
      }
      else {
        error( 'rewrite-key-not-matched-explicit', [ key.name.location, key ],
               { name, art: baseAssoc },
               'No foreign key $(NAME) is specified in association $(ART)' );
      }
    }
  }

  function rewriteKeysCovered( thisAssoc, otherAssoc ) {
    const names = [];
    const { foreignKeys } = thisAssoc;
    for (const name in foreignKeys) {
      if (!otherAssoc.foreignKeys[name])
        names.push( name );
    }
    if (names.length) {
      const location = dictLocation.end( otherAssoc.foreignKeys );
      const baseAssoc = assocWithExplicitSpec( thisAssoc );
      if (inferredForeignKeys( baseAssoc.foreignKeys )) { // still inferred = via target keys
        error( 'rewrite-key-not-covered-implicit', [ location, otherAssoc ],
               { names, target: baseAssoc.target },
               {
                 std: 'Specify keys $(NAMES) of original target $(TARGET) as foreign keys',
                 one: 'Specify key $(NAMES) of original target $(TARGET) as foreign key',
               } );
      }
      else {
        error( 'rewrite-key-not-covered-explicit', [ location, otherAssoc ],
               { names, art: otherAssoc },
               {
                 std: 'Specify foreign keys $(NAMES) of association $(ART)',
                 one: 'Specify foreign key $(NAMES) of association $(ART)',
               } );
      }
    }
  }

  function assocWithExplicitSpec( assoc ) {
    while (assoc.foreignKeys && inferredForeignKeys( assoc.foreignKeys, 'keys') ||
           assoc.on && assoc.on.$inferred)
      assoc = directType( assoc );
    return assoc;
  }

  function rewriteAssociation( element ) {
    let elem = element.items || element; // TODO v2: nested items
    if (elem.elements && enableExpandElements)
      forEachGeneric( elem, 'elements', rewriteAssociation );
    if (!originTarget( elem ))
      return;
    // console.log(message( null, elem.location, elem,
    // {art:assoc,target,ftype:JSON.stringify(ftype)}, 'Info','RA').toString())

    // With cyclic dependencies on select items, testing for the _effectiveType to
    // be 0 (test above) is not enough if we we have an explicit redirection
    // target -> avoid infloop ourselves with _status.
    const chain = [];
    while (!elem.on && !elem.foreignKeys) {
      chain.push( elem );
      if (elem._status === 'rewrite') { // circular dependency (already reported)
        for (const e of chain)
          setProp( e, '_status', null ); // XSN TODO: nonenum _status -> enum $status
        return;
      }
      setProp( elem, '_status', 'rewrite' );
      elem = directType( elem );
      if (!elem || elem.builtin) // safety
        return;
    }
    chain.reverse();
    for (const art of chain) {
      setProp( elem, '_status', null );
      if (elem.on)
        rewriteCondition( art, elem );
      else if (elem.foreignKeys)
        rewriteKeys( art, elem );
      elem = art;
    }
  }

  function originTarget( elem ) {
    const assoc = !elem.expand && directType( elem );
    const ftype = assoc && effectiveType( assoc );
    return ftype && ftype.target && ftype.target._artifact;
  }

  function inferredForeignKeys( foreignKeys, ignore ) {
    // TODO: better use a symbol $inferred for dictionaries later
    for (const name in foreignKeys)
      return foreignKeys[name].$inferred && foreignKeys[name].$inferred !== ignore;
    return false;
  }

  function rewriteKeys( elem, assoc ) {
    // TODO: split this function: create foreign keys without `targetElement`
    // already in Phase 2: redirectImplicitly()
    // console.log(message( null, elem.location, elem, {art:assoc,target:assoc.target},
    //  'Info','FK').toString())
    forEachInOrder( assoc, 'foreignKeys', ( orig, name ) => {
      const fk = linkToOrigin( orig, name, elem, 'foreignKeys', elem.location );
      fk.$inferred = 'rewrite'; // TODO: other $inferred value?
      // TODO: re-check for case that foreign key is managed association
      if ('_effectiveType' in orig)
        setProp( fk, '_effectiveType', orig._effectiveType);
      const te = copyExpr( orig.targetElement, elem.location );
      if (elem._redirected) {
        const i = te.path[0];   // TODO: or also follow path like for ON?
        const state = rewriteItem( elem, i, i.id, elem, true );
        if (state && state !== true && te.path.length === 1)
          setLink( te, state );
      }
      fk.targetElement = te;
    });
  }

  // TODO: there is no need to rewrite the on condition of non-leading queries,
  // i.e. we could just have on = {…}
  // TODO: re-check $self rewrite (with managed composition of aspects),
  // and actually also $self inside anonymous aspect definitions
  // (not entirely urgent as we do not analyse it further, at least sole "$self")
  function rewriteCondition( elem, assoc ) {
    if (enableExpandElements && elem._parent && elem._parent.kind === 'element') {
      // managed association as sub element not supported yet
      error( null, [ elem.location, elem ], {},
        // eslint-disable-next-line max-len
             'Rewriting the ON condition of unmanaged association in sub element is not supported' );
      return;
    }
    const nav = (elem._main && elem._main.query) ? pathNavigation( elem.value )
      : { navigation: assoc };
    const cond = copyExpr( assoc.on,
      // replace location in ON except if from mixin element
                           nav.tableAlias && elem.name.location );
    cond.$inferred = 'copy';
    elem.on = cond;
    // console.log(message( null, elem.location, elem, {art:assoc,target:assoc.target},
    //   'Info','ON').toString(), nav)
    const { navigation } = nav;
    if (!navigation) // TODO: what about $projection.assoc as myAssoc ?
      return;                 // should not happen: $projection, $magic, or ref to const
    // console.log(message( null, elem.location, elem, {art:assoc}, 'Info','D').toString())
    // Currently, having an unmanaged association inside a struct is not
    // supported by this function:
    if (navigation !== assoc && navigation._origin !== assoc) { // TODO: re-check
      // For "assoc1.assoc2" and "structelem1.assoc2"
      if (elem._redirected !== null) { // null = already reported
        error( 'rewrite-not-supported', [ elem.target.location, elem ], {},
               'The ON condition is not rewritten here - provide an explicit ON condition' );
      }
      return;
    }
    if (!nav.tableAlias || nav.tableAlias.path) {
      resolveExpr( cond, rewriteExpr, elem, nav.tableAlias );
    }
    else {
      // TODO: support that
      error( null, [ elem.value.location, elem ],
             'Selecting unmanaged associations from a sub query is not supported' );
    }
    cond.$inferred = 'rewrite';
  }

  function rewriteExpr( expr, assoc, tableAlias ) {
    // Rewrite ON condition (resulting in outside perspective) for association
    // 'assoc' in query or including entity from ON cond of mixin element /
    // element in included structure / element in source ref/d by table alias.

    // TODO: re-check args in references, forbid parameter use for the moment
    // TODO: complain about $self (unclear semantics)
    // console.log( info(null, [assoc.name.location, assoc],
    //       { art: expr._artifact, names: expr.path.map(i=>i.id) }, 'A').toString(), expr.path)

    if (!expr.path || !expr._artifact)
      return;
    if (!assoc._main)
      return;
    if (tableAlias) { // from ON cond of element in source ref/d by table alias
      const source = tableAlias._origin;
      const root = expr.path[0]._navigation || expr.path[0]._artifact;
      // console.log( info(null, [assoc.name.location, assoc],
      //                   { names: expr.path.map(i=>i.id), art: root }, 'TA').toString())
      if (!root || root._main !== source)
        return;                 // not $self or source element
      const item = expr.path[root.kind === '$self' ? 1 : 0];
      // console.log('YE', assoc.name, item, root.name, expr.path)
      rewritePath( expr, item, assoc,
                   navProjection( item && tableAlias.elements[item.id], assoc ),
                   assoc.value.location );
    }
    else if (assoc._main.query) { // from ON cond of mixin element in query
      const nav = pathNavigation( expr );
      if (nav.navigation || nav.tableAlias) { // rewrite src elem, mixin, $self[.elem]
        rewritePath( expr, nav.item, assoc,
                     navProjection( nav.navigation, assoc ),
                     nav.item ? nav.item.location : expr.path[0].location );
      }
    }
    else {                     // from ON cond of element in included structure
      const root = expr.path[0]._navigation || expr.path[0]._artifact;
      if (root.builtin || root.kind !== '$self' && root.kind !== 'element')
        return;
      const item = expr.path[root.kind === '$self' ? 1 : 0];
      if (!item)
        return;                                   // just $self
      const elem = assoc._main.elements[item.id]; // corresponding elem in including structure
      if (!(Array.isArray(elem) ||              // no msg for redefs
            elem === item._artifact ||          // redirection for explicit def
            elem._origin === item._artifact)) {
        const art = assoc._origin;
        warning( 'rewrite-shadowed', [ elem.name.location, elem ],
                 { art: art && effectiveType( art ) },
                 {
                   // eslint-disable-next-line max-len
                   std: 'This element is not originally referred to in the ON condition of association $(ART)',
                   // eslint-disable-next-line max-len
                   element: 'This element is not originally referred to in the ON condition of association $(MEMBER) of $(ART)',
                 } );
      }
      rewritePath( expr, item, assoc, (Array.isArray(elem) ? false : elem), null );
    }
  }

  function rewritePath( ref, item, assoc, elem, location ) {
    const { path } = ref;
    let root = path[0];
    if (!elem) {
      if (location) {
        error( 'rewrite-not-projected', [ location, assoc ],
               { name: assoc.name.id, art: item._artifact }, {
                 // eslint-disable-next-line max-len
                 std: 'Projected association $(NAME) uses non-projected element $(ART)',
                 // eslint-disable-next-line max-len
                 element: 'Projected association $(NAME) uses non-projected element $(MEMBER) of $(ART)',
               } );
      }
      delete root._navigation;
      setProp( root, '_artifact', elem );
      setProp( ref, '_artifact', elem );
      return;
    }
    if (item !== root) {
      root.id = '$self';
      setLink( root, assoc._parent.$tableAliases.$self, '_navigation' );
      setLink( root, assoc._parent );
    }
    else if (elem.name.id.charAt(0) === '$') {
      root = { id: '$self', location: item.location };
      path.unshift( root );
      setLink( root, assoc._parent.$tableAliases.$self, '_navigation' );
      setLink( root, assoc._parent );
    }
    else {
      setLink( root, elem, '_navigation' );
    }
    if (!elem.name)      // nothing to do for own $projection, $projection.elem
      return;            // (except having it renamed to $self)
    item.id = elem.name.id;
    let state = null;
    for (const i of path) {
      if (!state) {
        if (i === item)
          state = setLink( i, elem );
      }
      else if (i) {
        state = rewriteItem( state, i, i.id, assoc );
        if (!state || state === true)
          break;
      }
      else {
        return;
      }
    }
    if (state !== true)
      setLink( ref, state );
  }

  function rewriteItem( elem, item, name, assoc, forKeys ) {
    // TODO: for rewriting ON conditions of explicitly provided model targets,
    // we need to only rewrite the current element, not all sibling elements
    if (!elem._redirected)
      return true;
    for (const alias of elem._redirected) {
      // TODO: a message for the same situation as msg 'rewrite-shadowed'?
      if (alias.kind === '$tableAlias') { // _redirected also contains structures for includes
        // TODO: if there is a "multi-step" redirection, we should probably
        // consider intermediate "preferred" elements - not just `assoc`,
        // but its origins, too.
        const proj = navProjection( alias.elements[name], assoc );
        name = proj && proj.name && proj.name.id;
        if (!name) {
          if (!forKeys)
            break;
          setLink( item, null );
          error( 'rewrite-undefined-key', [ weakLocation( (elem.target || elem).location ), assoc ],
                 { id: item.id, art: alias._main },
                 'Foreign key $(ID) has not been found in target $(ART)' );
          return null;
        }
        item.id = name;
      }
    }
    const env = name && environment(elem);
    elem = setLink( item, env && env[name] );
    if (elem && !Array.isArray(elem))
      return elem;
    // TODO: better (extra message), TODO: do it
    error( 'query-undefined-element', [ item.location, assoc ], { id: name || item.id },
           // eslint-disable-next-line max-len
           'Element $(ID) has not been found in the elements of the query; please use REDIRECTED TO with an explicit ON condition' );
    return (elem) ? false : null;
  }

  //--------------------------------------------------------------------------
  // General resolver functions
  //--------------------------------------------------------------------------

  // Resolve the type and its arguments if applicable.
  function resolveTypeExpr( art, user ) {
    const typeArt = resolveType( art.type, user );
    if (typeArt)
      resolveTypeArguments( art, typeArt, user );
  }

  function resolveExpr( expr, expected, user, extDict, expandOrInline) {
    // TODO: extra "expected" 'expand'/'inline' instead o param `expandOrInline`
    if (!expr || typeof expr === 'string') // parse error or keywords in {xpr:...}
      return;
    if (Array.isArray(expr)) {
      expr.forEach( e => resolveExpr( e, expected, user, extDict ) );
      return;
    }

    if (expr.type) // e.g. cast( a as Integer )
      resolveTypeExpr( expr, user );

    if (expr.path) {
      if (expr.$expected === 'exists') {
        error( 'expr-unexpected-exists', [ expr.location, user ], {},
               'An EXISTS predicate is not expected here' );
        // We complain about the EXISTS before, as EXISTS subquery is also not
        // supported (avoid that word if you do not want to get tickets when it
        // will be supported), TODO: location of EXISTS
        expr.$expected = 'approved-exists'; // only complain once
      }
      if (expected instanceof Function) {
        expected( expr, user, extDict );
        return;
      }
      resolvePath( expr, expected, user, extDict );

      const last = !expandOrInline && expr.path[expr.path.length - 1];
      for (const step of expr.path) {
        if (step && (step.args || step.where || step.cardinality) &&
            step._artifact && !Array.isArray( step._artifact ) )
          resolveParamsAndWhere( step, expected, user, extDict, step === last );
      }
    }
    else if (expr.query) {
      const { query } = expr;
      if (query.kind || query._leadingQuery) { // UNION has _leadingQuery
        traverseQueryPost( query, false, resolveQuery );
      }
      else {
        error( 'expr-no-subquery', [ expr.location, user ], {},
               'Subqueries are not supported here' );
      }
    }
    else if (expr.op && expr.args) {
      const args = Array.isArray(expr.args) ? expr.args : Object.values( expr.args );
      args.forEach( e => e && resolveExpr( e, e.$expected || expected, user, extDict ) );
    }
    if (expr.suffix && !isBetaEnabled( options, 'windowFunctions' )) {
      const { location } = expr.suffix[0] || expr;
      error( null, [ location, user ], 'Window functions are not supported' );
    }
    if (expr.suffix)
      expr.suffix.forEach( s => s && resolveExpr( s, expected, user, extDict ) );
  }

  function resolveParamsAndWhere( step, expected, user, extDict, isLast ) {
    const alias = step._navigation && step._navigation.kind === '$tableAlias' && step._navigation;
    const type = alias || effectiveType( step._artifact );
    const art = (type && type.target) ? type.target._artifact : type;
    if (!art)
      return;
    const entity = (art.kind === 'entity') &&
      (!isLast || [ 'from', 'exists', 'approved-exists' ].includes( expected )) && art;
    if (step.args)
      resolveParams( step.args, art, entity, expected, user, extDict, step.location );
    if (entity) {
      if (step.where)
        resolveExpr( step.where, 'filter', user, environment( type ) );
    }
    else if (step.where && step.where.location || step.cardinality ) {
      const location = combinedLocation( step.where, step.cardinality );
      // XSN TODO: filter$location including […]
      message( 'expr-no-filter', [ location, user ], { '#': expected },
               {
                 std: 'A filter can only be provided when navigating along associations',
                 from: 'A filter can only be provided for the source entity or associations',
               } );
    }
  }

  function resolveParams( dict, art, entity, expected, user, extDict, stepLocation ) {
    if (!entity || !entity.params) {
      let first = dict[Object.keys(dict)[0]];
      if (Array.isArray(first))
        first = first[0];
      message( 'args-no-params',
               [ dictLocation( dict, first && first.name && first.name.location || stepLocation),
                 user ],
               { art, '#': (entity ? 'entity' : expected ) },
               {
                 std: 'Parameters can only be provided when navigating along associations',
                 from: 'Parameters can only be provided for the source entity or associations',
                 // or extra message id for entity?
                 entity: 'Entity $(ART) has no parameters',
               } );
      return;
    }
    const exp = (expected === 'from') ? 'expr' : expected;
    if (Array.isArray(dict)) {
      message( 'args-expected-named', [ dict[0] && dict[0].location || stepLocation, user ],
               'Named parameters must be provided for the entity' );
      for (const a of dict)
        resolveExpr( a, exp, user, extDict );
      return;
    }
    // TODO: allow to specify expected for arguments in in specExpected
    for (const name in dict) {
      const param = art.params[name];
      const arg = dict[name];
      for (const a of Array.isArray(arg) ? arg : [ arg ]) {
        setProp( a.name, '_artifact', param );
        if (!param) {
          message( 'args-undefined-param', [ a.name.location, user ], { art, id: name },
                   'Entity $(ART) has no parameter $(ID)' );
        }
        resolveExpr( a, exp, user, extDict );
      }
    }
  }
}

function copyExpr( expr, location, skipUnderscored, rewritePath ) {
  if (!expr || typeof expr !== 'object')
    return expr;
  else if (Array.isArray(expr))
    return expr.map( e => copyExpr( e, location, skipUnderscored, rewritePath ) );

  const proto = Object.getPrototypeOf( expr );
  if (proto && proto !== Object.prototype) // do not copy object from special classes
    return expr;
  const r = Object.create( proto );
  for (const prop of Object.getOwnPropertyNames( expr )) {
    const pd = Object.getOwnPropertyDescriptor( expr, prop );
    if (!pd.enumerable) { // should include all properties starting with _
      if (!skipUnderscored ||
          prop === '_artifact' || prop === '_navigation' || prop === '_effectiveType')
        Object.defineProperty( r, prop, pd );
    }
    else if (!proto) {
      r[prop] = copyExpr( pd.value, location, skipUnderscored, rewritePath );
    }
    else if (prop === 'location') {
      r[prop] = location || pd.value;
    }
    else if (prop.charAt(0) !== '$' || prop === '$inferred') {
      r[prop] = copyExpr( pd.value, location, skipUnderscored, rewritePath );
    }
    else if (!skipUnderscored) {  // skip $ properties
      Object.defineProperty( r, prop, pd );
    }
  }
  return r;
}

function testExpr( expr, pathTest, queryTest ) {
  // TODO: also check path arguments/filters
  if (!expr || typeof expr === 'string') { // parse error or keywords in {xpr:...}
    return false;
  }
  else if (Array.isArray(expr)) {
    return expr.some( e => testExpr( e, pathTest, queryTest ) );
  }
  else if (expr.path) {
    return pathTest( expr );
  }
  else if (expr.query) {
    return queryTest( expr.query );
  }
  else if (expr.op && expr.args) {
    // unnamed args => array
    if (Array.isArray(expr.args))
      return expr.args.some( e => testExpr( e, pathTest, queryTest ) );
    // named args => dictionary
    for (const namedArg of Object.keys(expr.args)) {
      if (testExpr(expr.args[namedArg], pathTest, queryTest))
        return true;
    }
  }
  return false;
}

// Return true if the path `item` with a final type `assoc` has a max target
// cardinality greater than one - either specified on the path item or assoc type.
function targetMaxNotOne( assoc, item ) {
  // Semantics of associations without provided cardinality: [*,0..1]
  const cardinality = item.cardinality || assoc.cardinality;
  return cardinality && cardinality.targetMax && cardinality.targetMax.val !== 1;
}

// Return condensed info about reference in select item
// - tableAlias.elem       -> { navigation: navElem, item: path[1], tableAlias }
// - sourceElem (in query) -> { navigation: navElem, item: path[0], tableAlias }
// - mixinElem             -> { navigation: mixinElement, item: path[0] }
// - $projection.elem      -> also $self.item -> { item: path[1], tableAlias: $self }
// - $self                 -> { item: undefined, tableAlias: $self }
// - $parameters.P, :P     -> {}
// - $now, current_date    -> {}
// - undef, redef          -> {}
// With 'navigation': store that navigation._artifact is projected
// With 'navigation': rewrite its ON condition
// With navigation: Do KEY propagation
function pathNavigation( ref ) {
  // currently, indirectly projectable elements are not included - we might
  // keep it this way!  If we want them to be included - be aware: cycles
  if (!ref._artifact)
    return {};
  let item = ref.path && ref.path[0];
  const root = item && item._navigation;
  if (!root)
    return {};
  if (root.kind === '$navElement')
    return { navigation: root, item, tableAlias: root._parent };
  if (root.kind === 'mixin')
    return { navigation: root, item };
  item = ref.path[1];
  if (root.kind === '$self')
    return { item, tableAlias: root };
  if (root.kind !== '$tableAlias' || ref.path.length < 2)
    return {};                // should not happen
  return { navigation: root.elements[item.id], item, tableAlias: root };
}

function navProjection( navigation, preferred ) {
  // TODO: Info if more than one possibility?
  // console.log(navigation,navigation._projections)
  if (!navigation)
    return {};
  else if (!navigation._projections)
    return null;
  return (preferred && navigation._projections.includes( preferred ))
    ? preferred
    : navigation._projections[0] || null;
}

// Query tree post-order traversal - called for everything which makes a query
// except "real ones": operands of UNION etc, JOIN with ON, and sub queries in FROM
function traverseQueryPost( query, simpleOnly, callback ) {
  while (Array.isArray(query)) // query in parentheses, TODO: remove
    query = query[0];
  if (!query)                   // parser error
    return;
  if (!query.op) {              // in FROM (not JOIN)
    if (query.query)            // subquery
      traverseQueryPost( query.query, simpleOnly, callback );
    return;
  }
  if (simpleOnly) {
    const { from } = query;
    if (!from || from.join)     // parse error or join
      return;                   // ok are: path or simple sub query (!)
  }
  if (query.from) {             // SELECT
    traverseQueryPost( query.from, simpleOnly, callback );
    // console.log('FC:')
    callback( query );
    // console.log('FE:')
  }
  else if (query.args) {             // JOIN, UNION, INTERSECT
    for (const q of query.args)
      traverseQueryPost( q, simpleOnly, callback );
    // The ON condition has to be traversed extra, because it must be evaluated
    // after the complete FROM has been traversed.  It is also not necessary to
    // evaluate it in populateQuery().
  }
  // else: with parse error (`select from <EOF>`, `select distinct from;`)
}

module.exports = resolve;
