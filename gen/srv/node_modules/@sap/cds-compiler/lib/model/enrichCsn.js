// For testing: reveal non-enumerable properties in CSN, display result of csnRefs

// Running `cdsc -E`, short for `cdsc --enrich-csn` displays additional
// information within the CSN, which might be useful for testing.

// An enumerable property `$location` appears in the JSON with the following value:

// * `File.cds:3:5` if the original CSN has a non-enumerable `$location` property
//   with value `{file: "File.cds", line: 3, col: 5}`.
// * `File.cds:3:5-1` if the original CSN has _no_ `$location` property, for an
//   inferred member of a main artifact or member with `$location: `File.cds:3:5`;
//   the number of digits in the `-1` suffix is the member depth.

// Other enumerable properties in the JSON for non-enumerable properties in the
// original CSN:

// * `$env` for the non-enumerable `$env` property in the original CSN.
// * `$elements` for a non-enumerable `elements` property for sub queries.

// The following properties in the JSON represent the result of the CSN API
// functions:

// * `_type`, `_includes` and `_targets` have as values the `$locations` of the
//   referred artifacts which are returned by function `artifactRef`.
// * `_links`, `_art` and `_scope` as sibling properties of `ref` have as values
//   the `$locations` of the artifacts/members returned by function `inspectRef`.

'use strict';

const { csnRefs, artifactProperties } = require('./csnRefs');
const { locationString } = require('../base/location');

function enrichCsn( csn, options = {} ) {
  const transformers = {
    // $env: reveal,
    elements: dictionary,
    definitions: dictionary,
    actions: dictionary,
    params: dictionary,
    enum: dictionary,
    mixin: dictionary,
    ref: pathRef,
    type: simpleRef,
    targetAspect: simpleRef,
    target: simpleRef,
    includes: simpleRef,
    $origin,
    // TODO: excluding
    '@': () => { /* ignore annotations */ },
  }
  setLocations( csn, false, null );
  const { inspectRef, artifactRef, getOrigin, __getCache_forEnrichCsnDebugging } =
      csnRefs( csn );
  let $$cacheObjectNumber = 0;   // for debugging
  const csnPath = [];
  if (csn.definitions)
    dictionary( csn, 'definitions', csn.definitions );
  if (csn.$location)
    reveal( csn, '$location', locationString( csn.$location ) );
  if (csn.$sources)
    reveal( csn, '$sources', csn.$sources );
  return csn;

  function standard( parent, prop, obj ) {
    if (!obj || typeof obj !== 'object' || !{}.propertyIsEnumerable.call( parent, prop ))
      return;

    csnPath.push( prop );
    if (Array.isArray(obj)) {
      obj.forEach( (n, i) => standard( obj, i, n ) );
    }
    else {
      for (let name of Object.getOwnPropertyNames( obj )) {
        const trans = transformers[name] || transformers[name.charAt(0)] || standard;
        trans( obj, name, obj[name] );
      }
      if (obj.$parens)
        reveal( obj, '$parens', obj.$parens );
      _cache_debug( obj );
    }
    csnPath.pop();
  }

  function dictionary( parent, prop, dict ) {
    csnPath.push( prop );
    for (let name of Object.getOwnPropertyNames( dict )) {
      standard( dict, name, dict[name] );
    }
    if (!Object.prototype.propertyIsEnumerable.call( parent, prop ))
      parent['$'+prop] = dict;
    csnPath.pop();
  }

  function refLocation( art ) {
    if (art)
      return art.$location || '<no location>';
    if (!options.testMode)
      return '<illegal link>';
    throw new Error( 'Undefined reference' );
  }

  function simpleRef( parent, prop, ref ) {
    // try {
    const notFound = (options.testMode) ? undefined : null;
    if (Array.isArray( ref )) {
      parent['_' + prop] = ref.map( r => refLocation( artifactRef( r, notFound ) ) );
    }
    else if (typeof ref === 'string') {
      if (!ref.startsWith( 'cds.'))
        parent['_' + prop] = refLocation( artifactRef( ref, notFound ) );
    }
    else if (!ref.elements) {
      parent['_' + prop] = refLocation( artifactRef( ref, notFound ) );
    }
    else {                      // targetAspect, target
      csnPath.push( prop );
      dictionary( ref, 'elements', ref.elements );
      csnPath.pop();
    }
    // } catch (e) {
    //   parent['_' + prop] = e.toString(); }
  }

  function $origin( parent, prop, ref ) {
    if (options.testMode) {
      if (Array.isArray( ref ))  // $origin: […], not $origin: {…}
        parent._origin = refLocation( getOrigin( parent ) );
    }
    else {
      try {
        if (Array.isArray( ref ))  // $origin: […], not $origin: {…}
          parent._origin = refLocation( getOrigin( parent ) );
      } catch (e) {
        parent._origin = e.toString();
      }
    }
  }

  function pathRef( parent, prop, path ) {
    const { links, art, scope, $env } = (() => {
      if (options.testMode)
        return inspectRef( csnPath );
      else {
        try {
          return inspectRef( csnPath );
        }
        catch (e) {
          return { scope: e.toString() };
        }
      }
    } )();
    if (links)
      parent._links = links.map( l => refLocation( l.art ) );
    if (links && links[links.length-1].art !== art)
      parent._art = refLocation( art );
    parent._scope = scope;
    if ($env)
      parent._env = $env;

    csnPath.push( prop );
    path.forEach( function step( s, i ) {
      if (s && typeof s === 'object') {
        csnPath.push( i );
        if (s.args)
          standard( s, 'args', s.args );
        if (s.where)
          standard( s, 'where', s.where );
        csnPath.pop();
      }
    } );
    csnPath.pop();
  }

  function _cache_debug( obj ) {
    if (options.enrichCsn !== 'DEBUG')
      return;
    const cache = __getCache_forEnrichCsnDebugging( obj );
    if (!cache)
      return;
    if (cache.$$objectNumber > 0) {
      obj.$$cacheObjectNumber = cache.$$objectNumber;
    }
    else {
      cache.$$objectNumber = (cache.$$objectNumber)
        ? -cache.$$objectNumber
        : ++$$cacheObjectNumber;
      obj.$$cacheObject = {};
      for (const name of Object.keys( cache )) {
        const val = cache[name];
        if (val === null || typeof val !== 'object') {
          obj.$$cacheObject[name] = val;
        }
        else if (name[0] === '_') {
          // _‹name›: link to CSN node, usually with kind & location
          obj.$$cacheObject[name]
            = (val.$location) ? locationString( val.$location ) : 'CSN node';
        }
        else if (name[0] !== '$' || !Object.getPrototypeOf( val )) {
          // ‹name›: dictionary of CSN nodes,
          // ‹$name›: dictionary of cache values if no prototype,
          obj.$$cacheObject[name] = Object.keys( val ); // TODO: or dict?
        }
        else if (Array.isArray( val )) {
          obj.$$cacheObject[name] = val.map( item => {
            if (!item.$$objectNumber)
              item.$$objectNumber = -(++$$cacheObjectNumber);
            return item.$$objectNumber;
          } );
        }
        else {
          if (!val.$$objectNumber)
            val.$$objectNumber = -(++$$cacheObjectNumber);
          obj.$$cacheObject[name] = val.$$objectNumber || -(++$$cacheObjectNumber);
        }
      }
    }
  }
}

function reveal( node, prop, value ) {
  Object.defineProperty( node, prop, {
    value,
    configurable: true,
    writable: true,
    enumerable: true,
  } );
}

function setLocations( node, prop, loc ) {
  if (!node || typeof node !== 'object')
    return;
  const isMember = artifactProperties.includes( prop );
  if (!isMember && node.$location) {
    const value = locationString( node.$location, true );
    reveal( node, '$location', value );
    loc = value + '-';
  }
  else if (prop === true) {
    loc += '1';
    node.$location = loc;
  }
  if (Array.isArray( node )) {
    for (const item of node)
      setLocations( item, isMember, loc );
  }
  else {
    for (const name of Object.getOwnPropertyNames( node ))
      setLocations( node[name], isMember || name, loc );
  }
}

module.exports = enrichCsn;
