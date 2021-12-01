const { fs } = require('@sap/cds-foss')

class MigrationTableParser {
  constructor() {
  }

  async read(filePath, options = { encoding: 'utf8' }) {
    if (await fs.pathExists(filePath)) {
      return this.parse((await fs.readFile(filePath, options)).replace(/\r\n/g, '\n'))
    }
    return null
  }

  /**
   * Parses the given .hdbmigrationtable file content and returns the @see MigrationTableModel representation.
   *
   * @param {String} content The .hdbmigrationtable file content.
   * @returns {MigrationTableModel} The migration table model representation of the given .hdbmigrationtable  file content.
   */
  parse(content) {
    const lines = content.split('\n')
    this._validate(lines)
    const table = this._parseTable(lines)
    const migrations = this._parseMigrations(lines, table)
    return new MigrationTableModel(table, migrations)
  }

  _validate(lines) {
    let isTableBegin = false, isTableEnd = false, isMigration = false
    let tVersion, mVersion = -1
    for (let idx = 0; idx < lines.length; idx++) {
      if (MigrationTableParser._isVersionMarker(lines[idx])) {
        tVersion = MigrationTableParser._parseVersionNumber(lines[idx])
        if (isTableBegin || isTableEnd || isMigration) {
          throw new Error(`Invalid format, version defintion must be very first statement`)
        }
      } else if (/^\s*COLUMN TABLE\s/.test(lines[idx])) {
        if (tVersion === -1) {
          throw new Error(`Invalid format, version entry not complying to format '^== version=d+'`)
        }
        if (isTableBegin) {
          throw new Error(`Invalid format, multiple COLUMN TABLE definitions found`)
        }
        if (isMigration) {
          throw new Error(`Invalid format, migrations must not be mixed with COLUMN TABLE definitions`)
        }
        isTableBegin = true
      } else if (MigrationTableParser._isMigrationMarker(lines[idx])) {
        const version = MigrationTableParser._parseVersionNumber(lines[idx])
        if (version === -1) {
          throw new Error(`Invalid format, migration entry not complying to format '^== version=d+'`)
        }
        if (version > mVersion) {
          mVersion = version
        }
        if (!isMigration) {
          if (!isTableBegin) {
            throw new Error(`Invalid format, COLUMN TABLE statement missing`)
          }
          // back search for end table
          for (let tIdx = idx - 1; tIdx > 0; tIdx--) {
            if (MigrationTableParser._isDDL(lines[tIdx])
              || MigrationTableParser._isComment(lines[tIdx])) {
              isTableEnd = true
              break
            }
          }
          isMigration = true
        }
      } else if (isTableBegin && !isMigration && idx + 1 === lines.length) {
        isTableEnd = true
      }
    }
    if (!isTableBegin) {
      throw new Error(`Invalid format, COLUMN TABLE statement missing`)
    }
    if (!isTableEnd) {
      throw new Error(`Invalid format, COLUMN TABLE statement not correctly terminated`)
    }
    if (!isMigration && tVersion > 1) {
      throw new Error(`Invalid format, '== migration=${tVersion}' entry missing`)
    }
    if (mVersion !== -1 && mVersion !== tVersion) {
      throw new Error(`Invalid format, migration version ${mVersion} does not match table version ${tVersion}`)
    }
  }

  _parseTable(lines) {
    const format = { startLine: -1, endLine: -1 }
    for (let idx = 0; idx < lines.length; idx++) {
      if (format.startLine === -1) {
        if (MigrationTableParser._isVersionMarker(lines[idx])) {
          format.startLine = idx
        }
      } else if (format.endLine === -1) {
        let tIdx = -1
        if (MigrationTableParser._isMigrationMarker(lines[idx])) {
          tIdx = idx - 1
        } else if (idx + 1 === lines.length) {
          tIdx = idx
        }
        // back search for end of table, comments belong to table
        for (; tIdx > format.startLine; tIdx--) {
          if (MigrationTableParser._isDDL(lines[tIdx])
            || MigrationTableParser._isComment(lines[tIdx])) {
            format.endLine = tIdx
            break
          }
        }
      } else {
        break
      }
    }
    if (format.startLine === -1) {
      throw new Error(`Invalid format, '== version=' entry missing`)
    }
    return new MigrationTable(lines, format)
  }

  _parseMigrations(lines, table) {
    const migrations = []
    let format = { startLine: -1, endLine: -1 }
    for (let idx = table.lines.length; idx < lines.length; idx++) {
      let nextMigration = false
      if (MigrationTableParser._isMigrationMarker(lines[idx])) {
        if (format.startLine === -1) {
          format.startLine = idx
        } else {
          nextMigration = true
        }
      }
      if (format.startLine !== -1 && (nextMigration || (idx + 1) === lines.length)) {
        // back search for end of migration, comments belong to former migration
        for (let mIdx = nextMigration ? idx - 1 : idx; mIdx > format.startLine; mIdx--) {
          if (MigrationTableParser._isDDL(lines[mIdx])
            || MigrationTableParser._isComment(lines[mIdx])) {
            format.endLine = mIdx
            break
          }
        }
        migrations.push(new Migration(lines, format))
        if (nextMigration) {
          format = { startLine: idx, endLine: -1 }
        }
      }
    }
    return new Migrations(migrations)
  }

  // any lines that do not start with a comment or conflict marker and do not represent version tags
  static _isDDL(line) {
    return !/^\s*--|^\s*==|^\s*$|^\s*>>>>>/.test(line)
  }

  static _isComment(line) {
    return /^\s*--/.test(line)
  }

  static _isConflictMarker(line) {
    return /^\s*>>>>>/.test(line)
  }

  static _isVersionMarker(line) {
    return /^\s*== version=\d+\s*$/.test(line)
  }

  static _isMigrationMarker(line) {
    return /^\s*== migration=\d+\s*$/.test(line)
  }

  static _parseVersionNumber(line) {
    let version = -1;
    const match = line.match(/(^\s*== version=|^\s*== migration=)(\d+)\s*$/)
    if (match && match.length === 3) {
      version = parseInt(match[2])
    }
    if (version === -1) {
      throw new Error(`Invalid format - ${line} is malformed, format '^== version=d+'|'^== migration=d+' expected`)
    }
    return version
  }
}

/**
 * Model representation of an entire .hdbmigrationtable file.
 * <p>
 * The MigrationTableModel provides access to underlying file contents using a well-defined API.
 */
class MigrationTableModel {
  constructor(table, migrations) {
    this._table = table
    this._migrations = migrations
  }

  get versionNumber() {
    return this._table.versionNumber
  }

  get table() {
    return this._table
  }

  get migrations() {
    return this._migrations
  }

  toString() {
    return `${this._table.toString()}${(this._migrations.entries.length > 0 ? '\n\n' : '') + this._migrations.toString()}`
  }
  /**
   * Incorporates migration versions from base (since the time their histories diverged from the current migration version)
   * into the current branch representing the extension.
   * <code>extension.merge(base, targetTable)</code>
   *
   *    A---B---E---F extension
   *   /
   *  A---B---C       base
   *
   *      ||  upgrade extension with base - migration version C is inserted into extension, its version number is updated,
   *      ||  its version number is rebased on the version number of the extension
   *      \/
   *
   *    A---B---E---F---C extension
   *   /
   *  A---B---C           base
   *
   * @param {MigrationTableModel} base The new base migration table version from which all missing migration versions
   * will be taken and merged into the new migration table result.
   * @param {MigrationTable} targetTable The column table representing the merge result. Technically speaking - it is
   * retrieved from the migration table file created by cds build based on the final CDS model version.
   * @returns {MigrationTableModel} The merge result containg missing migrations from base.
   */
  merge(base, targetTable) {
    let idxMaster = base.migrations.entries.findIndex(baseMigration => {
      return this.migrations.entries.some(extensionMigration => this._compareMigrations(extensionMigration, baseMigration))
    })
    const mergeResult = new MigrationTableModel(targetTable, this.migrations.clone())
    // insert all migration versions of master
    for (let idx = idxMaster === - 1 ? base.migrations.entries.length - 1 : idxMaster - 1; idx >= 0; idx--) {
      const migration = base.migrations.entries[idx].clone();
      // update migration version number and insert at beginning
      migration.versionNumber = this.versionNumber + 1
      mergeResult.table.versionNumber = this.versionNumber + 1
      mergeResult.migrations.entries.splice(0, 0, migration)
    }
    return mergeResult
  }

  clone() {
    return new MigrationTableModel(this.table.clone(), this.migrations.clone())
  }

  // migrations are identical if the underlying changesets are identical
  _compareMigrations(migration1, migration2) {
    return this._compactStr(migration1.changeset) === this._compactStr(migration2.changeset)
  }

  _compactStr(array) {
    return JSON.stringify(array.map(change => change.replace(/ /g, '')))
  }
}

/**
 * Representation of the table SQL statement within an .hdbmigrationtable file.
 */
class MigrationTable {
  /**
   * Constructor
   * @param {*} lines If the format parameter is passed the given lines represent all lines of the migration table file where format defines start and the end line.
   * If the format is ommitted the lines parameter only contains the lines of this table definition.
   * @param {*} format The format defines the start and end line of the table definition within the given lines.
   */
  constructor(lines, format) {
    lines = Array.isArray(lines) ? lines : lines.split('\n')
    if (format) {
      if (format.startLine < 0 || format.startLine > format.endLine) {
        throw Error("Invalid format of DDL table statement - end line has to be larger that start line")
      }
      this._lines = lines.slice(format.startLine, format.endLine + 1)
    } else {
      this._lines = lines
    }
    this._versionNumber = MigrationTableParser._parseVersionNumber(this._lines[0])
  }

  get versionNumber() {
    return this._versionNumber
  }

  set versionNumber(newVersion) {
    if (newVersion < 1) {
      throw new Error(`Invalid migration table version number ${newVersion} `)
    }
    this._lines[0] = this._lines[0].replace(this.versionNumber, newVersion)
    this._versionNumber = newVersion
  }

  get lines() {
    return this._lines
  }

  toString() {
    return this.lines.join('\n')
  }

  clone() {
    return new MigrationTable(JSON.parse(JSON.stringify(this.lines)))
  }
}

class Migrations {
  constructor(migrations = []) {
    this._migrations = migrations.sort((a, b) => b.versionNumber - a.versionNumber)
  }
  get versionNumber() {
    return this._migrations.length > 0 ? this._migrations[0].versionNumber : 1
  }
  get entries() {
    return this._migrations
  }
  toString() {
    return this._migrations.map(migration => migration.toString()).join('\n\n')
  }
  clone() {
    return new Migrations(this._migrations.map(migration => migration.clone()))
  }
}
/**
 * Representation of a migration version within an .hdbmigrationtable file.
 * <p>
 * The first line of a migration represents the version definition.
 * A migration may contain multiple line comments and any number of DDL statements.
 * The latter can be accessed using the changeset method.
 */
class Migration {
  /**
   * Constructor
   * @param {*} lines If the format parameter is passed the given lines represent all lines of the migration table file where format defines start and the end line.
   * If the format is ommitted the lines parameter only contains the lines of this migration version.
   * @param {*} format The format defines the start and end line of the migration version within the given lines.
   */
  constructor(lines, format) {
    if (format) {
      if (format.startLine < 0 || format.startLine > format.endLine) {
        throw Error("Invalid migration format")
      }
      this._lines = lines.slice(format.startLine, format.endLine + 1)
    } else {
      this._lines = lines
    }
    this._versionNumber = MigrationTableParser._parseVersionNumber(this.lines[0])
    this._changeset = this._lines.filter(line => !MigrationTableParser._isMigrationMarker(line))
  }

  /**
   * Returns the version number of this migration.
   */
  get versionNumber() {
    return this._versionNumber
  }

  set versionNumber(newVersion) {
    if (newVersion < 1) {
      throw new Error(`Invalid migration table version number ${newVersion} `)
    }
    this._lines[0] = this._lines[0].replace(this.versionNumber, newVersion)
    this._versionNumber = newVersion
  }

  /**
   * Returns the entire content of this migration including version and any line comments.
   */
  get lines() {
    return this._lines
  }

  /**
   * Returns the changeset containing the DDL statements of this migration.
   */
  get changeset() {
    return this._changeset
  }

  /**
   * Returns the DDL statements of this changeset. Any lines that do not start with a comment or conflict marker
   * and do not represent version tags are treated as valid DDL statements.
   */
  get ddl() {
    return this.changeset.filter(line => MigrationTableParser._isDDL(line))
  }

  /**
   * Returns the string representation of this migration.
   */
  toString() {
    return this.lines.join('\n')
  }

  clone() {
    return new Migration(JSON.parse(JSON.stringify(this.lines)))
  }
}

module.exports = new MigrationTableParser()