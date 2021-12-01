// Central registry for messages.

// `centralMessages` contains all details of a message-id except its standard texts
// (`standardTexts` exists for that).  Only `severity` is required, all other
// properties are optional.

// The user can specify "severity wishes" via the option `severities`.  Errors
// that don't have a `configurableFor` property cannot be reclassified by
// users.  If a module is used that is _not_ listed in `configurableFor` (if it
// is an array) property of the message then the message cannot be
// reclassified.

// We also allow `configurableFor` to have value `true` for errors which are
// always configurable; useful for issues like deprecated syntax variants which
// do not affect the compiler or CSN processors.  Temporarily, we also allow
// value `deprecated` for errors which are only configurable if the option
// `deprecated.downgradableErrors` is set.

// Messages other than errors can always be reclassified by the user except if
// the module is listed in the message's `errorFor` property.

// __NEW__: If the future `poc` (proof of concept) or `sloppy` option is set,
// the module name `compile` is added to all configurable messages, i.e. to all
// `configurableFor` arrays.  (module `compile` includes all parsers and the
// core compiler).  This allows creators of _non-productive models_ to
// reclassify errors which usually cannot be reclassified, and continue the
// compilation but has the side effect that the result may be unstable, hence
// "sloppy": with an upcoming _minor_ version of the compiler, the compilation
// might lead to an error anyway or the compiled CSN might look different.

'use strict';

/**
 * Central register of messages and their configuration.
 * Group by id-category.
 *
 * configurableFor: 'deprecated' = severity can only be changed with deprecated.downgradableErrors
 *
 * @type {Object<string, MessageConfig>}
 */
const centralMessages = {
  'anno-definition':        { severity: 'Warning' },
  'anno-duplicate':         { severity: 'Error', configurableFor: true }, // does not hurt us
  'anno-duplicate-unrelated-layer': { severity: 'Error', configurableFor: true }, // does not hurt us
  'anno-undefined-action':  { severity: 'Info' },
  'anno-undefined-art':     { severity: 'Info' }, // for annotate statement (for CDL path root)
  'anno-undefined-def':     { severity: 'Info' }, // for annotate statement (for CSN or CDL path cont)
  'anno-undefined-element': { severity: 'Info' },
  'anno-undefined-param':   { severity: 'Info' },

  'args-expected-named':  { severity: 'Error', configurableFor: 'deprecated' }, // future --sloppy
  'args-no-params':       { severity: 'Error', configurableFor: 'deprecated' }, // future --sloppy
  'args-undefined-param': { severity: 'Error', configurableFor: 'deprecated' }, // future --sloppy

  'assoc-in-array': { severity: 'Error', configurableFor: 'deprecated' }, // not supported yet
  'assoc-as-type':  { severity: 'Error', configurableFor: 'deprecated' }, // TODO: allow more, but not all

  'check-proper-type':    { severity: 'Error', configurableFor: [ 'compile' ] },
  'check-proper-type-of': { severity: 'Info',  errorFor: [ 'for.odata', 'to.edmx', 'to.hdbcds', 'to.sql', 'to.rename' ] },

  'expr-no-filter': { severity: 'Error', configurableFor: 'deprecated' },

  'empty-entity': { severity: 'Info', errorFor: [ 'to.hdbcds', 'to.sql', 'to.rename' ] },
  'empty-type':   { severity: 'Info' }, // only still an error in old transformers

  // Structured types were warned about but made CSN un-recompilable.
  'enum-invalid-type': { severity: 'Error', configurableFor: 'deprecated' },

  // TODO: rename to ref-expected-XYZ
  'expected-type': { severity: 'Error' },
  'ref-sloppy-type': { severity: 'Error' },
  'ref-invalid-typeof': { severity: 'Error', configurableFor: 'deprecated' }, // TODO: make it non-config
  'expected-actionparam-type': { severity: 'Error' },
  'ref-sloppy-actionparam-type': { severity: 'Error' },
  'expected-event-type': { severity: 'Error' },
  'ref-sloppy-event-type': { severity: 'Error' },
  'expected-struct': { severity: 'Error' },
  'expected-const': { severity: 'Error' },
  'expected-entity': { severity: 'Error' },
  'expected-source': { severity: 'Error' },
  'expected-target': { severity: 'Error' },
  'ref-sloppy-target': { severity: 'Warning' },

  'extend-repeated-intralayer': { severity: 'Warning' },
  'extend-unrelated-layer':     { severity: 'Warning' },

  'param-default': { severity: 'Error', configurableFor: 'deprecated' }, // not supported yet

  'query-undefined-element': { severity: 'Error' },
  'query-unexpected-assoc-hdbcds': { severity: 'Error' },
  'query-unexpected-structure-hdbcds': { severity: 'Error' },

  'recalculated-localized': { severity: 'Info' }, // KEEP: Downgrade in lib/transform/translateAssocsToJoins.js
  'redirected-implicitly-ambiguous': { severity: 'Error', configurableFor: true }, // does not hurt us - TODO: ref-ambiguous-target
  'type-ambiguous-target': { severity: 'Warning' },

  'ref-autoexposed': { severity: 'Error', configurableFor: 'deprecated' },
  'ref-undefined-art':    { severity: 'Error' },
  'ref-undefined-def':    { severity: 'Error' },
  'ref-undefined-var':    { severity: 'Error' },
  'ref-undefined-element': { severity: 'Error' },
  'ref-obsolete-parameters': { severity: 'Error', configurableFor: true }, // does not hurt us
  'ref-undefined-param': { severity: 'Error' },
  'ref-rejected-on': { severity: 'Error' },

  'rewrite-key-not-covered-explicit': { severity: 'Error', configurableFor: 'deprecated' },
  'rewrite-key-not-covered-implicit': { severity: 'Error', configurableFor: 'deprecated' },
  'rewrite-key-not-matched-explicit': { severity: 'Error', configurableFor: 'deprecated' },
  'rewrite-key-not-matched-implicit': { severity: 'Error', configurableFor: 'deprecated' },
  'rewrite-key-for-unmanaged': { severity: 'Error', configurableFor: 'deprecated' },
  'rewrite-not-supported': { severity: 'Error' },
  'rewrite-on-for-managed': { severity: 'Error', configurableFor: 'deprecated' },

  'service-nested-context': { severity: 'Error', configurableFor: true }, // does not hurt compile, TODO
  'service-nested-service': { severity: 'Error', configurableFor: 'deprecated' }, // not supported yet

  'syntax-anno-after-enum':   { severity: 'Error', configurableFor: true }, // does not hurt
  'syntax-anno-after-params': { severity: 'Error', configurableFor: true }, // does not hurt
  'syntax-anno-after-struct': { severity: 'Error', configurableFor: true }, // does not hurt
  'syntax-csn-expected-cardinality': { severity: 'Error' }, // TODO: more than 30 chars
  'syntax-csn-expected-translation': { severity: 'Error' }, // TODO: more than 30 chars
  'syntax-csn-required-subproperty': { severity: 'Error' }, // TODO: more than 30 chars
  'syntax-csn-unexpected-property': { severity: 'Error', configurableFor: true }, // is the removed
  'syntax-deprecated-ident': { severity: 'Error', configurableFor: true },
  'syntax-fragile-alias': { severity: 'Error', configurableFor: true },
  'syntax-fragile-ident': { severity: 'Error', configurableFor: true },

  'type-managed-composition': { severity: 'Error', configurableFor: 'deprecated' }, // TODO: non-config

  'unexpected-keys-for-composition': { severity: 'Error' }, // TODO: more than 30 chars
  'unmanaged-as-key': { severity: 'Error', configurableFor: 'deprecated' }, // is confusing
  'composition-as-key': { severity: 'Error', configurableFor: 'deprecated' }, // is confusing and not supported
  'odata-spec-violation-array':  { severity: 'Warning' }, // more than 30 chars
  'odata-spec-violation-constraints': { severity: 'Info' }, // more than 30 chars
  'odata-spec-violation-type': { severity: 'Error', configurableFor: [ 'to.edmx' ] },
  'odata-spec-violation-key-array':  { severity: 'Error' }, // more than 30 chars
  'odata-spec-violation-key-null': { severity: 'Error' }, // more than 30 chars
  'odata-spec-violation-key-type': { severity: 'Warning' }, // more than 30 chars
  'odata-spec-violation-property-name': { severity: 'Warning' }, // more than 30 chars
  'odata-spec-violation-namespace-name': { severity: 'Warning' }, // more than 30 chars
};

// For messageIds, where no text has been provided via code (central def)
const centralMessageTexts = {
  'anno-mismatched-ellipsis': 'An array with $(CODE) can only be used if there is an assignment below with an array value',
  'anno-unexpected-ellipsis': 'Unexpected $(CODE) in annotation assignment',
  'missing-type-parameter': 'Missing value for type parameter $(NAME) in reference to type $(ID)',
  'syntax-csn-expected-object': 'Expected object for property $(PROP)',
  'syntax-csn-expected-column': 'Expected object or string \'*\' for property $(PROP)',
  'syntax-csn-expected-natnum': 'Expected non-negative number for property $(PROP)',
  'syntax-csn-expected-cardinality': 'Expected non-negative number or string \'*\' for property $(PROP)',
  'syntax-csn-expected-reference': 'Expected non-empty string or object for property $(PROP)',
  'syntax-csn-expected-term': 'Expected non-empty string or object for property $(PROP)',
  'syntax-anno-after-struct': 'Avoid annotation assignments after structure definitions',
  'syntax-anno-after-enum': 'Avoid annotation assignments after enum definitions',
  'syntax-anno-after-params': 'Avoid annotation assignments after parameters',
  'syntax-dollar-ident': {
    std: 'An artifact starting with $(NAME) might shadow a special variable - replace by another name',
    $tableAlias: 'A table alias name starting with $(NAME) might shadow a special variable - replace by another name',
    $tableImplicit: 'The resulting table alias starts with $(NAME) and might shadow a special variable - specify another name with $(KEYWORD)',
    mixin: 'A mixin name starting with $(NAME) might shadow a special variable - replace by another name' ,
  },
  'ref-undefined-def': {
    std: 'Artifact $(ART) has not been found',
    // TODO: proposal 'No definition of $(NAME) found',
    element: 'Artifact $(ART) has no element $(MEMBER)'
  },
  'ref-undefined-art': 'No artifact has been found with name $(NAME)',
  // TODO: proposal 'No definition found for $(NAME)',
  'ref-undefined-element': {
    std: 'Element $(ART) has not been found',
    element: 'Artifact $(ART) has no element $(MEMBER)'
  },
  'ref-rejected-on': {
    std: 'Do not refer to a artefact like $(ID) in the explicit ON of a redirection', // Not used
    mixin: 'Do not refer to a mixin like $(ID) in the explicit ON of a redirection',
    alias: 'Do not refer to a source element (via table alias $(ID)) in the explicit ON of a redirection',
  },
  'ref-invalid-typeof': {
    std: 'Do not use $(KEYWORD) for the type reference here',
    type: 'Do not use $(KEYWORD) for the type of a type',
    event: 'Do not use $(KEYWORD) for the type of an event',
    param: 'Do not use $(KEYWORD) for the type of a parameter',
    select: 'Do not use $(KEYWORD) for type references in queries',
  },
  'anno-builtin': 'Builtin types should not be annotated. Use custom type instead',
  'anno-undefined-def': 'Artifact $(ART) has not been found',
  'anno-undefined-art': 'No artifact has been found with name $(NAME)',
  'anno-undefined-element': {
    std: 'Element $(ART) has not been found',
    element: 'Artifact $(ART) has no element $(MEMBER)',
    enum: 'Artifact $(ART) has no enum $(MEMBER)'
  },
  'anno-undefined-action': {
    std: 'Action $(ART) has not been found',
    action: 'Artifact $(ART) has no action $(MEMBER)'
  },
  'anno-undefined-param': {
    std: 'Parameter $(ART) has not been found',
    param: 'Artifact $(ART) has no parameter $(MEMBER)'
  },

  'duplicate-definition': {
    std: 'Duplicate definition of $(NAME)',
    absolute: 'Duplicate definition of artifact $(NAME)',
    namespace: 'Other definition blocks $(NAME) for namespace name',
    element: 'Duplicate definition of element $(NAME)',
    enum: 'Duplicate definition of enum $(NAME)',
    key: 'Duplicate definition of key $(NAME)',
    action: 'Duplicate definition of action or function $(NAME)',
    param: 'Duplicate definition of parameter $(NAME)',
    $tableAlias: 'Duplicate definition of table alias or mixin $(NAME)',
  },

  // TODO: rename to ref-expected-XYZ
  'expected-actionparam-type': 'A type, an element, or a service entity is expected here',
  'expected-const': 'A constant value is expected here',
  'expected-context': 'A context or service is expected here',
  'expected-event-type': 'A type, an element, an event, or a service entity is expected here',
  'expected-entity': 'An entity, projection or view is expected here',
  'expected-struct': 'A type, entity, aspect or event with direct elements is expected here',
  'expected-type': 'A type or an element is expected here',
  // TOODO: text variant if the association does not start an an entity
  'expected-source': 'A query source must be an entity or an association',
  'expected-target': 'An entity or an aspect is expected here',
  'extend-columns': 'Artifact $(ART) can\'t be extended with columns, only projections can',
  'extend-repeated-intralayer': 'Unstable element order due to repeated extensions in same layer',

  'query-unexpected-assoc-hdbcds': 'Publishing a managed association in a view is not possible for “hdbcds” naming mode',
  'query-unexpected-structure-hdbcds': 'Publishing a structured element in a view is not possible for “hdbcds” naming mode',

  'ref-sloppy-type': 'A type or an element is expected here',
  'ref-sloppy-actionparam-type': 'A type, an element, or a service entity is expected here',
  'ref-sloppy-target': 'An entity or an aspect (not type) is expected here',
  'ref-sloppy-event-type': 'A type, an element, an event, or a service entity is expected here',

  'type-managed-composition': {
    std: 'Managed compositions can\'t be used in types', // yet
    sub: 'Managed compositions can\'t be used in sub elements',
    aspect: 'Aspect $(ART) with managed compositions can\'t be used in types', // yet
    entity: 'Entity $(ART) with managed compositions can\'t be used in types', // yet
  },

  'i18n-different-value': 'Different translation for key $(PROP) of language $(OTHERPROP) in unrelated layers',

  // OData version dependent messages
  'odata-spec-violation-array': 'Unexpected array type for $(API)',
  'odata-spec-violation-param' : 'Expected parameter to be typed with either scalar or structured type for $(API)',
  'odata-spec-violation-returns': 'Expected $(KIND) to return one or many values of scalar, complex, entity or view type for $(API)',
  'odata-spec-violation-assoc': 'Unexpected association in structured type for $(API)',
  'odata-spec-violation-constraints': 'Partial referential constraints produced for $(API)',
  // version independent messages
  'odata-spec-violation-key-array': {
    std: 'Unexpected array type for element $(NAME)',
    scalar: 'Unexpected array type'
  },
  'odata-spec-violation-key-null': {
    std: 'Expected key element $(NAME) to be not nullable', // structured
    scalar: 'Expected key element to be not nullable' // flat
  },
  'odata-spec-violation-key-type': {
    std: 'Unexpected $(TYPE) mapped to $(ID) as type for key element $(NAME)', // structured
    scalar: 'Unexpected $(TYPE) mapped to $(ID) as type for key element' // flat
  },
  'odata-spec-violation-type': 'Expected element to have a type',
  'odata-spec-violation-property-name': 'Expected element name to be different from declaring $(KIND)',
  'odata-spec-violation-namespace': 'Expected service name not to be one of the reserved names $(NAMES)',
}

/**
 * Configuration for a message in the central message register.
 *
 * @typedef {object} MessageConfig
 * @property {CSN.MessageSeverity} severity Default severity for the message.
 * @property {string[]|'deprecated'|true} [configurableFor]
 *        Whether the error can be reclassified to a warning or lower.
 *        If not `true` then an array is expected with specified modules in which the error is downgradable.
 *        Only has an effect if default severity is 'Error'.
 *        'deprecated': severity can only be changed with deprecated.downgradableErrors.
 *        TODO: Value `true` is temporary. Use an array instead.
 * @property {string[]} [errorFor] Array of module names where the message shall be reclassified to an error.
 * @property {boolean} [throughMessageCall]
 *        If set, it means that a message-id was added to the registry in test-mode through a `message.<severity>()`
 *        call.  Used for ensuring that all calls with the same message-id have the same severity.
 */

module.exports = { centralMessages, centralMessageTexts };
