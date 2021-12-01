// Official cds-compiler API.
// All functions and namespace documented here are available when
// @sap/cds-compiler is required.
//
// These types are improved step by step and use a lot any types at the moment.

export = compiler;

declare namespace compiler {

  /**
   * Options used by the core compiler and all backends.
   */
  export type Options = {
    [option: string]: any,

    /**
     * Compiler and backend messages. Messages can be simple info messages but
     * also warnings and errors.  It is highly recommended to fix any warnings
     * and to not ignore them.
     * Errors stop the compilation process.
     */
    messages?: object[]
    /**
     * Dictionary of message-ids and their reclassified severity.  This option
     * can be used to increase the severity of messages.  The compiler will
     * ignore decreased severities as this may lead to issues during
     * compilation otherwise.
     */
    severities?: { [messageId: string]: MessageSeverity}
    /**
     * Dictionary of beta flag names.  This option allows fine-grained control
     * over which beta features should be enabled.
     * For a list of beta flag, please refer to `cdsc --help`.
     *
     * For backwards compatibility, this option may be `true` to indicate that
     * all beta features should be enabled.
     */
    beta?: { [betaFlag: string]: boolean } | boolean
    /**
     * If true, internal consistency checks are enabled and recompilation in
     * backends is disabled.
     *
     * @internal This is an internal option and should not be used by end-users.
     */
    testMode?: boolean
    /**
     * If true, CSN definitions are sorted by name.  Implicitly enabled when testMode is true.
     * `testMode` has higher priority, meaning if `testSortCsn` is `false` and `testMode` is true,
     * definitions will still be sorted.
     */
    testSortCsn?: boolean
    /**
     * A JS prototype that will be used for dictionaries created by the compiler.
     * Dictionaries are e.g. "definitions" and "elements".
     */
    dictionaryPrototype?: any
  }

  /**
   * The compiler's package version.
   * For more details on versioning and SemVer, see `doc/Versioning.md`
   */
  export function version(): string;

  /**
   * Main function: Compile the sources from the files given by the array of
   * `filenames`.  As usual with the `fs` library, relative file names are
   * relative to the working directory `process.cwd()`.  With argument `dir`, the
   * file names are relative to `process.cwd()+dir`.
   *
   * This function returns a Promise and can be used with `await`.  For an example
   * see `examples/api-usage/`.
   * See function {@link compileSync} or {@link compileSources} for alternative compile
   * functions.
   *
   * The promise is fulfilled if all files could be read and processed without
   * errors.  The fulfillment value is a CSN model.
   *
   * If there are errors, the promise is rejected.  If there was an invocation
   * error (repeated filenames or if the file could not be read), the rejection
   * value is an {@link InvocationError}.  Otherwise, the rejection value is a
   * {@link CompilationError} containing a vector of individual errors.
   *
   * @param filenames Array of files that should be compiled.
   * @param options  Compiler options. If you do not set `messages`, they will be printed to console.
   * @param fileCache A dictionary of absolute file names to the file content with values:
   *  - false: the file does not exist
   *  - true: file exists (fstat), no further knowledge yet - i.e. value will change!
   *  - 'string' or Buffer: the file content
   *  - { realname: fs.realpath(filename) }: if filename is not canonicalized
   */
  export function compile(filenames: string[], dir?: string, options?: Options, fileCache?: Record<string, any>): Promise<any>;
  export function compileSync(filenames: string[], dir?: string, options?: Options, fileCache?: Record<string, any>): any;
  export function compileSources(sourcesDict: any, options?: Options): any;

  /**
   * In version 2 of cds-compiler, this is an identity function and
   * is only kept for backwards compatibility.
   *
   * @deprecated
   * @returns The input parameter "csn".
   */
  export function compactModel(csn: CSN): any;

  export class CompilationError extends Error {
      constructor(messages: any, model: any, text: any, ...args);
      messages: any[];
      toString(): string;
      /**
       * If `options.attachValidNames` is set, this non-enumerable property holds the CSN model.
       * @internal
       */
      model?: CSN;
      /**
       * Used by `cdsc` to indicate whether the message was already printed to stderr.
       * @private
       */
      hasBeenReported: boolean;
  }

  /**
   * Sort the given messages according to their location.  Messages are sorted
   * in ascending order according to their:
   *
   *  - file name
   *  - start line
   *  - start column
   *  - end line
   *  - end column
   *  - semantic location (“home”)
   *  - message text
   *
   * If both messages do not have a location, they are sorted by their semantic
   * location and then by their message text.  If only one message has a file
   * location, that message is sorted prior to those that don't have one.
   *
   * _Note_: Sorting is done in-place.
   *
   * Example of sorted messages:
   * ```txt
   * A.cds:1:11: Info    id-3: First message text   (in entity:“E”/element:“c”)
   * A.cds:8:11: Error   id-5: Another message text (in entity:“C”/element:“g”)
   * B.cds:3:10: Debug   id-7: First message text   (in entity:“B”/element:“e”)
   * B.cds:3:12: Warning id-4: Message text         (in entity:“B”/element:“d”)
   * B.cds:3:12: Error   id-4: Message text         (in entity:“B”/element:“e”)
   * ```
   *
   * If you also want to sort according to message's severity,
   * see {@link sortMessagesSeverityAware}.
   *
   * @returns The same messages array as the input parameter.
   */
  export function sortMessages(messages: CompileMessage[]): CompileMessage[];

  /**
   * Sort the given messages in severity aware order.  Messages are sorted first
   * by severity where 'Error' comes first, then 'Warning' and so forth.
   * Messages of the same severity are sorted the same as by {@link sortMessages}.
   *
   * _Note_: Sorting is done in-place.
   *
   * @returns The same messages array as the input parameter.
   */
  export function sortMessagesSeverityAware(messages: CompileMessage[]): CompileMessage[];

  /**
   * Removes duplicate messages from the given messages array without destroying
   * references to the array, i.e. removes them in-place.
   *
   * _Note_: Does NOT keep the original order!
   *
   * Two messages are the same if they have the same message hash (see below).
   * If one of the two is more precise, then it replaces the other.
   * A message is more precise if it is contained in the other or if
   * the first does not have an `endLine`/`endCol`.
   *
   * A “message hash” is the string representation of the message.  If the
   * message does not have a semantic location (“home”), the message hash
   * is the result of {@link messageString}.  If the message has a semantic
   * location, the file location is stripped before being passed to
   * {@link messageString}.
   */
  export function deduplicateMessages(messages: CompileMessage[]): void;

  /**
   * Returns a message string with file- and semantic location if present in compact
   * form (i.e. one line)
   *
   * Example:
   * ```txt
   * <source>.cds:3:11: Error message-id: Can't find type `nu` in this scope (in entity:“E”/element:“e”)
   * ```
   *
   * @param normalizeFilename If true, the file path will be normalized to use `/` as the path separator.
   * @param noMessageId       If true, the message ID will _not_ be part of the string.
   * @param noHome            If true, the semantic location will _not_ be part of the string.
   */
  export function messageString(msg: CompileMessage, normalizeFilename?: boolean, noMessageId?: boolean, noHome?: boolean): string;

  /**
   * Returns a message string with file- and semantic location if present
   * in multiline form.
   * The error (+ message id) will be colored according to their severity if
   * run on a TTY.
   *
   * Example:
   * ```txt
   * Error[message-id]: Can't find type `nu` in this scope (in entity:“E”/element:“e”)
   *    |
   *   <source>.cds:3:11, at entity:“E”
   * ```
   *
   * @param config.normalizeFilename If true, the file path will be normalized to use `/` as the path separator.
   * @param config.noMessageId       If true, no messages id (in brackets) will be shown.
   * @param config.hintExplanation   If true, messages with explanations will get a "…" marker, see {@link hasMessageExplanation}.
   * @param config.withLineSpacer    If true, an additional line (with `|`) will be inserted between message and location.
   */
  export function messageStringMultiline(msg: CompileMessage, config?: {
    normalizeFilename?: boolean
    noMessageId?: boolean
    hintExplanation?: boolean
    withLineSpacer?: boolean
  }): string;

  /**
   * Returns a context (code) string for the given message that is human readable.
   *
   * The message context can be used to indicate to users where an error occurred.
   * The line length is limited to 100 characters.  If the message spans more than three
   * lines, only the first three lines are printed and an ellipsis will be appended in the next line.
   * If only one line is to be shown, the affected columns will be highlighted by a caret (`^`).
   * All lines are prepended by a pipe (`|`) and show the corresponding line number.
   *
   * Example Output:
   * ```txt
   *     |
   *  13 |     num * nu
   *     |           ^^
   * ```
   *
   * @param sourceLines The source code split up into lines, e.g. by `str.split(/\r\n?|\n/);`.
   * @param msg         Message whose location is used to print the message context.
   */
  export function messageContext(sourceLines: string[], msg: CompileMessage): string;

  /**
   * Get an explanatory text for a complicated compiler message with ID
   * messageId.  This function does a file lookup in `share/messages`.
   * If the message explanation does not exist, an exception is thrown.
   *
   * @throws May throw an ENOENT error if the message explanation cannot be found.
   * @see {@link hasMessageExplanation}
   */
  export function explainMessage(messageId: string): string;
  /**
   * Returns `true` if the given messageId has an explanatory text.
   * Contrary to {@link explainMessage}, this function does not make
   * a file lookup.
   */
  export function hasMessageExplanation(messageId: string): boolean;

  export class InvocationError extends Error {
      constructor(errs: any, ...args);
      errors: any[]
  }

  /**
   * Returns true if at least one of the given messages is of severity "Error"
   */
  export function hasErrors(messages: CompileMessage[]): boolean;

  export function preparedCsnToEdm(csn: CSN, service: string, options: any): any;
  export function preparedCsnToEdmx(csn: CSN, service: string, options: any): any;

  export namespace parse {
      /**
       * Parse the given CDL in parseCdl mode and return its corresponding CSN representation.
       *
       * @param cdl      CDL source as string.
       * @param filename Filename to be used in compiler messages.
       * @param options  Compiler options. Note that if `options.messages` is not set, messages will be printed to stderr.
       */
      function cdl(cdl: string, filename: string, options?: Options): any;

      /**
       * Parse the given CQL and return its corresponding CSN representation.
       *
       * @param cdl      CDL source as string.
       * @param filename Filename to be used in compiler messages, default is '<query>.cds'
       * @param options  Compiler options. Note that if `options.messages` is not set, messages will be printed to stderr.
       */
      function cql(cdl: string, filename?: string, options?: Options): any;

      /**
       * Parse the given CDL expression and return its corresponding CSN representation.
       *
       * @param cdl      CDL source as string.
       * @param filename Filename to be used in compiler messages, default is '<expr>.cds'
       * @param options  Compiler options. Note that if `options.messages` is not set, messages will be printed to stderr.
       */
      function expr(cdl: string, filename?: string, options?: Options): any;
  }

  /**
   * @deprecated Use {@link parse.cql} instead.
   */
  export function parseToCqn(cdl: string, filename?: string, options?: Options): any;
  /**
   * @deprecated Use {@link parse.expr} instead.
   */
  export function parseToExpr(cdl: string, filename?: string, options?: Options): any;

  /**
   * @todo Actual name is "for" which isn't used in the doc as it is a reserved name.
   * @alias for
   */
  export namespace For {
    function odata(): any;
  }

  export namespace to {
      function cdl(csn: CSN, options: Options): object;
      function sql(csn: CSN, options: Options): any;

      function edm(csn: CSN, options: Options): any;
      namespace edm {
          function all(csn: CSN, options: Options): any;
      }

      function edmx(csn: CSN, options: Options): any;
      namespace edmx {
          function all(csn: CSN, options: Options): any;
      }

      function hdbcds(csn: CSN, options: Options): any;
      function hdi(csn: CSN, options: Options): any;
      namespace hdi {
          function migration(csn: CSN, options: Options, beforeImage: any): any;
      }
  }

  export function getArtifactCdsPersistenceName(artifactName: string, namingConvention: any, csn: CSN): any;
  export function getElementCdsPersistenceName(elemName: string, namingConvention: any): any;

  /**
   * @private
   */
  export namespace $lsp {
      function compile(filenames: string[], dir?: string, options?: Options, fileCache?: Record<string, any>): Promise<any>;
      function parse(source: string, filename?: string, options?: Options): any;
  }

  /**
   * CSN object. Not yet specified in this TypeScript declaration file.
   */
  export type CSN = any;

  export class CompileMessage {
    constructor(location: Location, msg: string, severity?: MessageSeverity, id?: string | null, home?: string | null, moduleName?: string | null);

    /**
     * Optional ID of the message.  Can be used to reclassify messages.
     *
     * @note This property is non-enumerable as message IDs are not finalized, yet.
     */
    messageId?: string

    severity: MessageSeverity

    /**
     * @deprecated Use `$location` instead.
     */
    location: Location

    /**
     * Location information like file and line/column of the message.
     */
    $location: Location & {
      address?: {
        /**
         * Fully qualified name of the affected definition.
         */
        definition?: string
      }
    }
    /**
     * String representation of the message.  May be a multi-line message in the future.
     */
    message: string

    /**
     * A string describing the path to the artifact, e.g. `entity:"E"/element:"x"`.
     */
    home?: string
    /**
     * Array of names that are valid at the specified position.
     * Contains values if the message describes an "artifact not found" message.
     *
     * @internal Only to be used by the LSP implementation for CDS.
     */
    validNames: string[] | null
    /**
     * If `internalMsg` is set, then this property will have an error object with a stack trace.
     */
    error?: Error

    /**
     * Returns a human readable string of the compiler message. Uses {@link messageString} to render
     * the message without filename normalization and without a message ID.
     */
    toString(): string;
  }

  /**
   * Severities a compiler message can have.
   */
  export type MessageSeverity = 'Error' | 'Warning' | 'Info' | 'Debug';

  /**
   * CSN Location, often exposed by `$location` in CSN.
   * Columns and lines are 1-based, i.e. value `0` is an invalid value and
   * indicates absence of the property.
   *
   * All properties are optional, even `file`.
   */
  export type Location = {
    file?:    string
    line?:    number
    col?:     number
    endLine?: number
    endCol?:  number
  }

}
