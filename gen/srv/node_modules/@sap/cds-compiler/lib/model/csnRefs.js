// CSN functionality for resolving references

// Resolving references in a CSN can be a bit tricky, because the semantics of
// a reference is context-dependent, especially if queries are involved.  This
// module provides the corresponding resolve/inspect functions.
//
// See below for preconditions / things to consider – the functions in this
// module do not issue user-friendly messages for invalid references in a CSN,
// such messages are (hopefully) issued by the compile() function.

// The main export function `csnRefs` of this module is called with a CSN as
// input and returns functions which analyse references in the provided CSN:
//
//     const { csnRefs } = require('../model/csnRefs');
//     function myCsnAnalyser( csn ) {
//       const { inspectRef } = csnRefs( csn );
//         …
//         const { links, art } = inspectRef( csnPath );
//         // → art is the CSN node which is referred to by the reference
//         // → links provides some info about each reference path step
//         …
//     }
//
// You can see the results of the CSN refs functions by using our client tool:
//     cdsc --enrich-csn MyModel.cds
// It is also used by our references tests, for details see ./enrichCsn.js.

// Terminology used in this file:
//
// - ref (reference): a { ref: <path> } object (or sometimes also a string)
//   referring an artifact or member (element, …)
// - path: an array of strings or { id: … } objects for the dot-connected names
//   used as reference
// - csnPath: an array of strings and numbers (e.g. ['definitions', 'S.E',
//   'query', 'SELECT', 'from', 'ref', 0, 'where', 2]); they are the property
//   names and array indexes which navigate from the CSN root to the reference.

// ## PRECONDITIONS / THINGS TO CONSIDER -------------------------------------

// The functions in this module expect
//
// 1. a well-formed CSN with valid references;
// 2. a compiled model, i.e. a CSN with all inferred information provided by
//    the compile() function, including the (non-)enumerable `elements`
//    property of sub `SELECT`s in a FROM;
// 3. no (relevant) CSN changes between the calls of the same instance of
//    inspectRef() - to enable caching.
//
// If any of these conditions are not given, our functions usually simply
// throws an exception (which might even be a plain TypeError), but it might
// also jsut return any value.  CSN processors can provide user-friendly error
// messages by calling the Core Compiler in case of exceptions.  For details,
// see internalDoc/CoreCompiler.md#use-of-the-core-compiler-for-csn-processors.

// During a transformation, care must be taked to adhere to these conditions.
// E.g. a structure flattening function cannot create an element `s_x` and
// delete `s` and then still expects inspectRef() to be able to resolve a
// reference `['s', 'x']`.

// The functions in this module also use an internal cache.  The second call of
// inspectRef() in the following example might lead to a wrong result or an
// exception if the assignment to `inspectRef` is not uncommented:
//
//     let { inspectRef } = csnRefs( csn );
//     const csnPath = ['definitions','P','projection','columns',0];
//     const subElement = inspectRef( csnPath );  // type T is involved
//     csn.definitions.T.type = 'some.other.type';
//     // ({ inspectRef } = csnRefs( csn ));      // invalidate caches
//     … = inspectRef( csnPath );  // type T - using the cached or the new?
//
// On request, we might add a functions for individual cache invalidations or
// low-level versions of inspectRef() for performance.

// ## NAME RESOLUTION OVERVIEW -----------------------------------------------

// The most interesting part of a reference is always: where to search for the
// name in its first path item?  The general search is always as follows, with
// the exact behavior being dependent on the “reference context” (e.g. “reference
// in a `on` condition of a `mixin` definition”):
//
// 1. We search in environments constructed by “defining” names “around” the
//    lexical position of the reference.  In a CSN, these could be the
//    (explicit and implicit) table alias names and `mixin` definitions of the
//    current query and its parent queries (according to the query hiearchy).
// 2. If the search according to (1) was not successful and the name starts
//    with a `$`, we could consider the name to be a “magic” variable with
//    `$self` (and `$projection`) being a special magic variable.
// 3. Otherwise, we would search in a “dynamic” environment, which could be
//    `‹csn›.definitions` for global references like `type`, the elements of
//    the current element's parent, the combined elements of the query source
//    entities, the resulting elements of the current query, or something
//    special (elements of the association's target, …).
//
// The names in further path items are searched in the “navigation” environment
// of the path so far - it does not need to depend on the reference context (as
// we do not check the validility here):
//
// 1. We search in the elements of the target entity for associations and
//    compositions, and in the elements of the current object otherwise.
// 2. If there is an `items`, we check for `elements`/`target` inside `items`.
// 3. `elements`/`target`/`items` inherited from the “effective type” are also
//    considered.

// For details about the name resolution in CSN, see
// internalDoc/CsnSyntax.md#helper-property-for-simplified-name-resolution
// and doc/NameResolution.md.  Here comes a summary.

// ## IMPLEMENTATION OVERVIEW ------------------------------------------------

// The main function `inspectRef` works as follows:
//
// 1. For ease of use, the input is the “CSN path” as explained above, e.g.
//    ['definitions', 'P', 'query', 'SELECT', 'from', 'ref', 0, 'where', 2]
// 2. This is condensed into a “reference context” string, e.g. `ref_where`;
//    that might also depend on sibling properties along the way, e.g.
//    ['definitions', 'P', 'query', 'SELECT', 'columns', 0, 'expand', 0] leads
//    to `expand` if there is a `‹csn›.definitions.P.query.SELECT.columns[0].ref`
//    and to `columns` otherwise.
// 3. Additionally, other useful CSN nodes are collected like the current query;
//    the queries of a definition are also prepared for further inspection.
// 4. If applicable, a “base environment” is calculated; e.g. references in
//    `ref_where` are resolved against the elements of the entity referred to
//    by the outer `ref`.
// 5. We look up the “reference semantics” in constant `referenceSemantics`
//    using the “reference context” string as key.
// 6. The property `lexical` determines whether to search in “lexical
//    environments” (table aliases and `mixin`s) starting from which query, and
//    whether to do something special for names starting with `$`.
// 7. The property `dynamic` determines where to search if the lexical search
//    was not successful.
// 8. The remaining reference path is resolved as well - the final referred CSN
//    node is returned as well as information about each path step.

// We usually cache calculated data.  For the following reasons, we now use a
// WeakMap as cache instead of adding non-enumerable properties to the CSN:
//
// - CSN consumers should not have access to the cached data, as we might
//   change the way how we calculate things.
// - Avoid memory leaks.
// - Natural cache invalidation if there is no handle anymore to the functions
//   returned by `csnRefs`.

// Our cache looks like follows:

// - Each object in the CSN could have an cache entry which itself is an object
//   which contains cached data.  Such data can be a link to a CSN node (like
//   `_effectiveType`/`elements`), scalar (like `$queryNumber`) or link to
//   another cache object (like `$next`).
// - Usually, each CSN object has an individual cache object.
// - For CSN queries nodes, cache objects are _shared_: both the CSN nodes
//   `‹query› = { SELECT: ‹select›, … }` and `‹select›` share the same cache
//   object; a UNION `‹set_query› = { SET: args: [‹query1›, …] }` and ‹query1›
//   (which can itself be a `SELECT` or `SET`) share also the same cache
//   object; this way, the relevant query elements are directly available.
// - The cache objects for all queries of an entity are initialized as soon as
//   any reference in the entity is inspected: with data for the query
//   hierarchy, query number, table aliases and links from a column to its
//   respective inferred element.

'use strict';

const BUILTIN_TYPE = {};
const { locationString } = require('../base/location');

// Properties in which artifact or members are defined - next property in the
// "csnPath" is the name or index of that property; 'args' (its value can be a
// dictionary) is handled extra here, also 'expand' and 'inline'
const artifactProperties = [ 'elements', 'columns', 'keys', 'mixin', 'enum',
  'params', 'actions', 'definitions', 'extensions' ]; // + 'args', see above

// Mapping the “reference context string” to the reference semantics
// - lexical: false | Function - determines where to look first for “lexical names”
// - dynamic: String - describes the dynamic environment (if in query)
const referenceSemantics = {
  type: { lexical: false, dynamic: 'global' },
  includes: { lexical: false, dynamic: 'global' },
  target: { lexical: false, dynamic: 'global' },
  targetAspect: { lexical: false, dynamic: 'global' },
  from: { lexical: false, dynamic: 'global' },
  keys: { lexical: false, dynamic: 'target' },
  excluding: { lexical: false, dynamic: 'source' },
  expand: { lexical: justDollar, dynamic: 'expand' },   // ...using baseEnv
  inline: { lexical: justDollar, dynamic: 'inline' },   // ...using baseEnv
  ref_where: { lexical: justDollar , dynamic: 'ref-target'}, // ...using baseEnv
  on: { lexical: justDollar, dynamic: 'query' }, // assoc defs, redirected to
  // there are also 'join_on' and 'mixin_on' with default semantics
  orderBy: { lexical: query => query, dynamic: 'query' },
  orderBy_set: { lexical: query => query.$next, dynamic: 'query' }, // to outer SELECT (from UNION)
  // default: { lexical: query => query, dynamic: 'source' }
}

function justDollar() {
  return null;
}

/**
 * @param {CSN.Model} csn
 */
function csnRefs( csn ) {
  const cache = new WeakMap();

  // Functions which set the new `baseEnv`:
  resolveRef.expandInline = function resolve_expandInline( ref, ...args ) {
    return cached( ref, '_env', () => navigationEnv( resolveRef( ref, ...args ).art ) );
  }
  resolveRef.ref_where = function resolve_ref_where( pathItem, baseRef, ...args ) {
    return cached( pathItem, '_env', () => {
      resolveRef( baseRef, ...args ); // sets _env cache for non-string ref items
      return getCache( pathItem, '_env' );
    } );
  }
  return {
    effectiveType, artifactRef, getOrigin, inspectRef, queryOrMain,
    __getCache_forEnrichCsnDebugging: obj => cache.get( obj ),
  };

  /**
   * Return the type relevant for name resolution, i.e. the object which has a
   * `target`, `elements`, `enum` property, or no `type` property.
   * (This function could be simplified if we would use JS prototypes for type refs.)
   *
   * @param {CSN.ArtifactWithRefs} art
   */
  function effectiveType( art ) {
    const cachedType = getCache( art, '_effectiveType' );
    if (cachedType !== undefined)
      return cachedType;
    else if (!art.type && !art.$origin ||
             art.elements || art.target || art.targetAspect || art.enum)
      return setCache( art, '_effectiveType', art );

    const chain = [];
    while (getCache( art, '_effectiveType' ) === undefined && (art.type || art.$origin) &&
           !art.elements && !art.target && !art.targetAspect && !art.enum && !art.items) {
      chain.push( art );
      setCache( art, '_effectiveType', 0 ); // initial setting in case of cycles
      art = (art.$origin) ? getOrigin( art ) : artifactRef( art.type, BUILTIN_TYPE );
    }
    if (getCache( art, '_effectiveType' ) === 0)
      throw new Error( 'Circular type reference');
    const type = getCache( art, '_effectiveType' ) || art;
    chain.forEach( a => setCache( a, '_effectiveType', type ) );
    return type;
  }

  /**
   * @param {CSN.Artifact} art
   */
  function navigationEnv( art ) {
    let type = effectiveType( art );
    // here, we do not care whether it is semantically ok to navigate into sub
    // elements of array items (that is the task of the core compiler /
    // semantic check)
    while (type.items)
      type = effectiveType( type.items );
    // cannot navigate along targetAspect!
    return (type.target) ? csn.definitions[type.target] : type;
  }

  /**
   * Return the object pointing to by the artifact reference (in 'type',
   * 'includes', 'target', raw 'from').
   *
   * @param {CSN.ArtifactReferencePath|string} ref
   * @param {any} [notFound] Value that is returned in case the artifact reference
   *                         could not be found.
   */
  function artifactRef( ref, notFound ) {
    const art = (typeof ref === 'string')
      ? csn.definitions[ref]
      : cached( ref, '_ref', artifactPathRef );
    if (art)
      return art;
    if (notFound !== undefined)
      return notFound;
    throw new Error( 'Undefined reference' );
  }

  function artifactPathRef( ref ) {
    const [ head, ...tail ] = ref.ref;
    let art = csn.definitions[pathId( head )];
    for (const elem of tail)
      art = navigationEnv( art ).elements[pathId( elem )];
    return art;
  }

  function getOrigin( def ) {
    const art = cached( def, '_origin', originPathRef );
    if (art)
      return art;
    throw new Error( 'Undefined origin reference' );
  }

  function originPathRef( def ) {
    const [ head, ...tail ] = def.$origin;
    let art = csn.definitions[head];
    for (const elem of tail)
      art = originNavigation( art, elem );
    return art;
  }

  function originNavigation( art, elem ) {
    if (typeof elem !== 'string') {
      if (elem.action)
        return art.actions[elem.action]
      if (elem.param)
        return (elem.param ? art.params[elem.param] : art.returns);
    }
    if (art.returns)
      art = art.returns;
    while (art.items)
      art = art.items;
    return (art.elements || art.enum || (art.targetAspect || art.target).elements)[elem];
  }

  /**
   * Return the entity we select from
   *
   * @param {CSN.ArtifactReferencePath} ref
   * @returns {CSN.Definition}
   */
  function fromRef( ref ) {
    return navigationEnv( artifactRef( ref ));
  }

  /**
   * @param {CSN.Path} csnPath
   *
   * - return value `art`: the “resulting” CSN of the reference
   *
   * - return value `links`: array of { art, env } in length of ref.path where
   *   art = the definition or element reached by the ref path so far
   *   env = the “navigation environment” provided by `art`
   *         (not set for last item, except for `from` reference or with filter)
   *
   * - return value `scope`
   *   global: first item is name of definition
   *   param:  first item is parameter of definition (with param: true)
   *   parent: first item is elem of parent (definition or outer elem)
   *   target: first item is elem in target (for keys of assocs)
   *   $magic: magic variable (path starts with $magic, see also $self)
   *   $self:  first item is $self or $projection
   *   // now values only in queries:
   *   mixin:  first item is mixin
   *   alias:  first item is table alias
   *   source: first item is element in a source of the current query
   *   query:  first item is element of current query
   *   ref-target: first item is element of target of outer ref item
   *           (used for filter condition)
   *   expand: ref is "path continuation" of a ref with EXPAND
   *   inline: ref is "path continuation" of a ref with INLINE
   *
   * - return value `$env` is set with certain values of `scope`:
   *   with 'alias': the query number _n_ (the _n_th SELECT)
   *   with 'source': the table alias name for the source entity
   */
  function inspectRef( csnPath ) {
    return analyseCsnPath( csnPath, csn, resolveRef );
  }

  function resolveRef( ref, refCtx, main, query, parent, baseEnv ) {
    const path = (typeof ref === 'string') ? [ ref ] : ref.ref;
    if (!Array.isArray( path ))
      throw new Error( 'References must look like {ref:[...]}' );

    const head = pathId( path[0] );
    if (ref.param)
      return resolvePath( path, main.params[head], 'param' );

    const semantics = referenceSemantics[refCtx] || {};
    if (semantics.dynamic === 'global' || ref.global)
      return resolvePath( path, csn.definitions[head], 'global', refCtx === 'from' );

    cached( main, '$queries', allQueries );
    let qcache = query && cache.get( query.projection || query );
    // BACKEND ISSUE: you cannot call csnRefs(), inspect some refs, change the
    // CSN and again inspect some refs without calling csnRefs() before!
    // WORKAROUND: if no cached query, a backend has changed the CSN - re-eval cache
    if (query && !qcache) {
      setCache( main, '$queries', allQueries( main ) );
      qcache = cache.get( query.projection || query );
    }
    // first the lexical scopes (due to query hierarchy) and $magic: ---------
    if (semantics.lexical !== false) {
      const tryAlias = path.length > 1 || ref.expand || ref.inline;
      let cache = qcache && (semantics.lexical ? semantics.lexical( qcache ) : qcache);
      while (cache) {
        const alias = tryAlias && cache.$aliases[head];
        if (alias)
          return resolvePath( path, alias._select || alias, 'alias', cache.$queryNumber );
        const mixin = cache._select.mixin && cache._select.mixin[head];
        if (mixin && {}.hasOwnProperty.call( cache._select.mixin, head ))
          return resolvePath( path, mixin, 'mixin', cache.$queryNumber );
        cache = cache.$next;
      }
      if (head.charAt(0) === '$') {
        if (head !== '$self' && head !== '$projection')
          return { scope: '$magic' };
        const self = qcache && qcache.$queryNumber > 1 ? qcache._select : main;
        return resolvePath( path, self, '$self' );
      }
    }
    // now the dynamic environment: ------------------------------------------
    if (semantics.dynamic === 'target') { // ref in keys
      // not selecting the corresponding element for a select column works,
      // because explicit keys can only be provided with explicit redirection
      // target
      const target = csn.definitions[parent.target || parent.cast.target];
      return resolvePath( path, target.elements[head], 'target' );
    }
    if (baseEnv)                // ref-target (filter condition), expand, inline
      return resolvePath( path, baseEnv.elements[head], semantics.dynamic );
    if (!query)                 // outside queries - TODO: items?
      return resolvePath( path, parent.elements[head], 'parent' );

    if (semantics.dynamic === 'query')
      // TODO: for ON condition in expand, would need to use cached _element
      return resolvePath( path, qcache.elements[head], 'query' );
    for (const name in qcache.$aliases) {
      const found = qcache.$aliases[name].elements[head];
      if (found)
        return resolvePath( path, found, 'source', name )
    }
    // console.log(query.SELECT,qcache,qcache.$next,main)
    throw new Error ( `Path item ${ 0 }=${ head } refers to nothing, refCtx: ${ refCtx }` );
  }

  /**
   * @param {CSN.Path} path
   * @param {CSN.Artifact} art
   * @param {string} [scope]
   */
  function resolvePath( path, art, scope, extraInfo ) {
    /** @type {{idx, art?, env?}[]} */
    const links = path.map( (_v, idx) => ({ idx }) );
    // TODO: backends should be changed to enable uncommenting:
    // if (!art)    // does not work with test3/Associations/KeylessManagedAssociation/
    //   throw new Error ( `Path item ${ 0 }=${ pathId( path[0] ) } refers to nothing, scope: ${ scope }`);
    links[0].art = art;
    for (let i = 1; i < links.length; ++i) { // yes, starting at 1, links[0] is set above
      art = navigationEnv( art );
      links[i - 1].env = art;
      if (typeof path[i - 1] !== 'string')
        setCache( path[i - 1], '_env', art );
      art = art.elements[pathId( path[i] )];
      if (!art) {
        const env = links[i - 1].env;
        const loc = env.name && env.name.$location || env.$location;
        throw new Error ( `Path item ${ i }=${ pathId( path[i] ) } on ${ locationString( loc ) } refers to nothing` );
      }
      links[i].art = art;
    }
    const last = path[path.length - 1];
    const fromRef = scope === 'global' && extraInfo;
    if (fromRef || typeof last !== 'string') {
      const env = navigationEnv( art );
      links[links.length - 1].env = env;
      if (fromRef)
        art = env;
      if (typeof last !== 'string')
        setCache( last, '_env', env )
    }
    return (extraInfo && !fromRef)
      ? { links, art, scope, $env: extraInfo }
      : { links, art, scope };
  }

  /**
   * Get the array of all (sub-)queries (value of the `SELECT`/`projection`
   * property) inside the given `main` artifact (of `main.query`).
   *
   * @param {CSN.Definition} main
   * @returns {CSN.Query[]}
   */
  function allQueries( main ) {
    const all = [];
    const projection = main.query || main.projection && main;
    if (!projection)
      return null;
    traverseQuery( projection, null, null, function memorize( query, fromSelect, parentQuery ) {
      if (query.ref) {          // ref in from
        // console.log('SQ:',query,cache.get(query))
        const as = query.as || implicitAs( query.ref );
        getCache( fromSelect, '$aliases' )[as] = fromRef( query );
      }
      else {
        const qcache = getQueryCache( parentQuery );
        if (query !== main)
          cache.set( query, qcache );

        if (fromSelect)
          getCache( fromSelect, '$aliases' )[query.as] = qcache;
        const select = query.SELECT || query.projection;
        if (select) {
          cache.set( select, qcache ); // query and query.SELECT have the same cache qcache
          qcache._select = select;
          all.push( qcache );
        }
      }
    } );
    all.forEach( function initElements( qcache, index ) {
      qcache.$queryNumber = index + 1;
      qcache.elements = (index ? qcache._select : main).elements;
      const columns = qcache._select.columns;
      if (qcache.elements && columns)
        columns.map( c => initColumnElement( c, qcache ) );
    } );
    return all;
  }

  /**
   * Return the cache object for a new query.
   * Might re-use cache object with the `parentQuery`, or use `parentQuery`
   * for link to next lexical environment.
   */

  function getQueryCache( parentQuery ) {
    if (!parentQuery)
      return { $aliases: Object.create(null) };
    const pcache = cache.get( parentQuery );
    if (!parentQuery.SET)       // SELECT / projection: real sub query
      return { $aliases: Object.create(null), $next: pcache };
    // the parent query is a SET: that is not a sub query
    // (works, as no sub queries are allowed in ORDER BY)
    return (!pcache._select)    // no leading query yet
      ? pcache                  // share cache with parent query
      : { $aliases: Object.create(null), $next: pcache.$next };
  }

  function initColumnElement( col, parentElementOrQueryCache ) {
    if (col === '*')
      return;
    if (col.inline) {
      col.inline.map( c => initColumnElement( c, parentElementOrQueryCache ) );
      return;
    }
    setCache( col, '_parent',   // not set for query (has property _select)
              !parentElementOrQueryCache._select && parentElementOrQueryCache );
    const as = col.as || col.func || implicitAs( col.ref );
    let type = parentElementOrQueryCache;
    while (type.items)
      type = type.items;
    const elem = setCache( col, '_element', type.elements[as] );
    // if requested, we could set a _column link in element
    if (col.expand)
      col.expand.map( c => initColumnElement( c, elem ) );
  }

  // property name convention in cache:
  // - $name: to other cache object (with proto), dictionary (w/o proto), or scalar
  // - _name, name: to CSN object value (_name) or dictionary (name)

  function setCache( obj, prop, val ) {
    let hidden = cache.get( obj );
    if (!hidden) {
      hidden = {};
      cache.set( obj, hidden );
    }
    hidden[prop] = val;
    return val;
  }

  function getCache( obj, prop ) {
    const hidden = cache.get( obj );
    return hidden && hidden[prop];
  }

  function cached( obj, prop, calc ) {
    let hidden = cache.get( obj );
    if (!hidden) {
      hidden = {};
      cache.set( obj, hidden );
    }
    else if (hidden[prop] !== undefined)
      return hidden[prop];
    const val = calc( obj );
    hidden[prop] = val;
    return val;
  }
}

// Return value of a query SELECT for the query node, or the main artifact,
// i.e. a value with an `elements` property.
// TODO: only used in forHanaNew - move somewhere else
/**
 * @param {CSN.Query} query node (object with SET or SELECT property)
 * @param {CSN.Definition} main
 */
function queryOrMain( query, main ) {
  while (query.SET)
    query = query.SET.args[0];
  if (query.SELECT && query.SELECT.elements)
    return query.SELECT;
  let leading = main.query || main;
  while (leading.SET)
    leading = leading.SET.args[0];
  // If an entity has both a projection and query property, the param `query`
  // can be the entity itself (when inspect is called with a csnPath containing
  // 'projection'), but `leading` can be its `query` property:
  if ((leading === query || leading === query.query) && main.elements)
    return main;
  throw new Error( `Query elements not available: ${ Object.keys( query ).join('+') }`);
}

/**
 * Traverse query in pre-order
 *
 * @param {CSN.Query} query
 * @param {CSN.QuerySelect} fromSelect
 * @param {CSN.Query} parentQuery 
 * @param {(query: CSN.Query&CSN.QueryFrom, select: CSN.QuerySelectEnriched) => void} callback
 */
function traverseQuery( query, fromSelect, parentQuery, callback ) {
  const select = query.SELECT || query.projection;
  if (select) {
    callback( query, fromSelect, parentQuery );
    traverseFrom( select.from, select, parentQuery, callback );
    for (const prop of [ 'columns', 'where', 'having' ]) {
      // all properties which can have sub queries (`join-on` also can)
      const expr = select[prop];
      if (expr)
        expr.forEach( q => traverseExpr( q, query, callback ) );
    }
  }
  else if (query.SET) {
    callback( query, fromSelect, parentQuery );
    const { args } = query.SET;
    for (const q of args || [])
      traverseQuery( q, null, query, callback );
  }
}

/**
 * @param {CSN.QueryFrom} from
 * @param {CSN.QuerySelect} select
 * @param {(from: CSN.QueryFrom, select: CSN.QuerySelect) => void} callback
 */
function traverseFrom( from, fromSelect, parentQuery, callback ) {
  if (from.ref) {
    callback( from, fromSelect, parentQuery );
  }
  else if (from.args) {         // join
    from.args.forEach( arg => traverseFrom( arg, fromSelect, parentQuery, callback ) );
    if (from.on)                // join-on, potentially having a sub query
      from.on.forEach( arg => traverseQuery( arg, null, fromSelect, callback ) );
  }
  else {                        // sub query in FROM
    traverseQuery( from, fromSelect, parentQuery, callback );
  }
}

function traverseExpr( expr, parentQuery, callback ) {
  if (expr.SELECT || expr.SET)
    traverseQuery( expr, null, parentQuery, callback )
  for (const prop of [ 'args', 'xpr' ]) {
    // all properties which could have sub queries (directly or indirectly),
    const val = expr[prop];
    if (val && typeof val === 'object') {
      const args = Array.isArray( val ) ? val : Object.values( val );
      args.forEach( e => traverseExpr( e, parentQuery, callback ) );
    }
  }
}

function pathId( item ) {
  return (typeof item === 'string') ? item : item.id;
}

function implicitAs( ref ) {
  const id = pathId( ref[ref.length - 1] );
  return id.substring( id.lastIndexOf('.') + 1 );
}

/**
 * @param {CSN.Path} csnPath
 * @param {CSN.Model} csn
 */
function analyseCsnPath( csnPath, csn, resolve ) {
  if (csnPath[0] !== 'definitions')
    throw new Error( 'References outside definitions not supported yet');

  /** @type {object} */
  let obj = csn;
  let parent = null;
  let query = null;
  let refCtx = null;
  let art = null;
  /** @type {boolean|string|number} */
  let isName = false;
  let baseRef = null;
  let baseEnv = null;
  let main = csn.definitions[csnPath[1]];

  for (let index = 0; index < csnPath.length; index++) {
    const prop = csnPath[index];
    // array item, name/index of artifact/member, (named) argument
    if (isName || Array.isArray( obj )) {
      if (typeof isName === 'string') {
        parent = art;
        art = obj[prop];
      }
      isName = false;
    }
    else if (artifactProperties.includes( String(prop) )) {
      if (refCtx === 'target' || refCtx === 'targetAspect') { // with 'elements'
        main = art = obj;       // $self refers to the anonymous aspect
        parent = null;
      }
      isName = prop;
      refCtx = prop;
    }
    else if (prop === 'items' || prop === 'returns') {
      art = obj[prop];
    }
    else if (prop === 'args') {
      isName = true;            // for named arguments
    }
    else if (prop === 'SELECT' || prop === 'SET' || prop === 'projection') {
      query = obj;
      parent = null;
      baseEnv = null;
      refCtx = prop;
    }
    else if (prop === 'where' && refCtx === 'ref') {
      if (resolve)
        baseEnv = resolve.ref_where( obj, baseRef, refCtx, csn.definitions[csnPath[1]],
                                    query, parent, baseEnv );
      refCtx = 'ref_where';
    }
    else if (prop === 'expand' || prop === 'inline') {
      if (obj.ref) {
        if (resolve)
          baseEnv = resolve.expandInline( obj, refCtx, csn.definitions[csnPath[1]],
                                          query, parent, baseEnv );
        refCtx = prop;
      }
      if (prop === 'expand')
        isName = prop;
    }
    else if (prop === 'on') {
      if (refCtx === 'from')
        refCtx = 'join_on';
      else if (refCtx === 'mixin')
        refCtx = 'mixin_on';
      else
        refCtx = 'on';          // will use query elements with REDIRECTED TO
    }
    else if (prop === 'ref') {
      baseRef = obj;            // needs to be inspected for filter conditions
      refCtx = prop;
    }
    else if (prop === 'orderBy') {
      refCtx = (query.SET ? 'orderBy_set' : 'orderBy');
    }
    else if (prop !== 'xpr') {
      refCtx = prop;
    }

    obj = obj[prop];
    if (!obj && !resolve)
      // For the semantic location, use current object as best guess
      break;
  }
  // console.log( 'CPATH:', csnPath, refCtx, obj, parent.$location );
  if (!resolve)
    return { query };           // for constructSemanticLocationFromCsnPath
  return resolve( obj, refCtx, main, query, parent, baseEnv );
}

module.exports = {
  csnRefs,
  traverseQuery,
  artifactProperties,
  implicitAs,
  analyseCsnPath,
  pathId,
};
