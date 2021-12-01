// Main entry point for the CDS Compiler
//
// File for external usage = which is read in other modules with
//   require('cdsv');

// Proposed intra-module lib dependencies:
//  - lib/base/<file>.js: can be required by all others, requires no other
//    of this project
//  - lib/<dir>/<file>.js: can be required by other files lib/<dir>/,
//    can require other files lib/<dir>/ and lib/base/<file>.js
//  - lib/main.js (this file): can be required by none in lib/ (only in
//    bin/ and test/), can require any other

'use strict';

const backends = require('./backends');
const { odata, cdl, sql, hdi, hdbcds, edm, edmx } = require('./api/main');
const { getArtifactDatabaseNameOf, getElementDatabaseNameOf } = require('./model/csnUtils');
const { traverseCsn } = require('./model/api');
const { createMessageFunctions, sortMessages, sortMessagesSeverityAware, deduplicateMessages } = require('./base/messages');

const parseLanguage = require('./language/antlrParser');
const { parseX, compileX, compileSyncX, compileSourcesX, InvocationError } = require('./compiler');
const { define } = require('./compiler/definer');

// The compiler version (taken from package.json)
function version() {
  return require('../package.json').version;
}

const {
  CompilationError,
  messageString,
  messageStringMultiline,
  messageContext,
  hasErrors,
  explainMessage,
  hasMessageExplanation
} = require('./base/messages');

const { compactModel, compactQuery, compactExpr } = require('./json/to-csn')

function parseCdl( cdl, filename, options = {} ) {
  options = Object.assign( {}, options, { parseCdl: true } );
  const sources = Object.create(null);
  const model = { sources, options };
  const messageFunctions = createMessageFunctions( options, 'parse', model );
  model.$messageFunctions = messageFunctions;

  const xsn = parseLanguage( cdl, filename, Object.assign( { parseOnly: true }, options ),
                             messageFunctions );
  sources[filename] = xsn;
  define( model );
  messageFunctions.throwWithError();
  return compactModel( model );
}

function parseCql( cdl, filename = '<query>.cds', options = {} ) {
  const messageFunctions = createMessageFunctions( options, 'parse' );
  const xsn = parseLanguage( cdl, filename, Object.assign( { parseOnly: true }, options ),
                             messageFunctions, 'query' );
  messageFunctions.throwWithError();
  return compactQuery( xsn );
}

function parseExpr( cdl, filename = '<expr>.cds', options = {} ) {
  const messageFunctions = createMessageFunctions( options, 'parse' );
  const xsn = parseLanguage( cdl, filename, Object.assign( { parseOnly: true }, options ),
                             messageFunctions, 'expr' );
  messageFunctions.throwWithError();
  return compactExpr( xsn );
}

// FIXME: The implementation of those functions that delegate to 'backends' should probably move here
// ATTENTION: Keep in sync with main.d.ts!
module.exports = {
  // Compiler
  version,
  compile: (...args) => compileX(...args).then( compactModel ), // main function
  compileSync: (...args) => compactModel( compileSyncX(...args) ), // main function
  compileSources: (...args) => compactModel( compileSourcesX(...args) ), // main function
  compactModel: csn => csn,     // for easy v2 migration
  CompilationError,
  sortMessages,
  sortMessagesSeverityAware,
  deduplicateMessages,
  messageString,
  messageStringMultiline,
  messageContext,
  explainMessage,
  hasMessageExplanation,
  InvocationError,    // TODO: make it no error if same file name is provided twice
  hasErrors,

  // Backends
  // TODO: Expose when transformers are switched to CSN
  // toOdataWithCsn: backends.toOdataWithCsn,
  preparedCsnToEdmx : (csn, service, options) => { return backends.preparedCsnToEdmx(csn, service, options).edmx},
  preparedCsnToEdm :  (csn, service, options) => { return backends.preparedCsnToEdm(csn, service, options).edmj},

  // additional API:
  parse: { cdl: parseCdl, cql: parseCql, expr: parseExpr }, // preferred names
  /**
   * @deprecated Use parse.cql instead
   */
  parseToCqn: parseCql,
  parseToExpr: parseExpr,       // deprecated name
  // SNAPI
  for: { odata },
  to: { cdl, sql, hdi, hdbcds, edm, edmx },
  // Convenience for hdbtabledata calculation in @sap/cds
  getArtifactCdsPersistenceName: getArtifactDatabaseNameOf,
  getElementCdsPersistenceName: getElementDatabaseNameOf,

  // Other API functions:
  traverseCsn,

  // INTERNAL functions for the cds-lsp package and friends - before you use
  // it, you MUST talk with us - there can be potential incompatibilities with
  // new releases (even having the same major version):
  $lsp: { parse: parseX, compile: compileX },
};
