const { resolve, dirname } = require('path')

class Test extends require('./axios') {

  static run(..._) { return (new Test).run(..._) }
  get cds() { return require('../index') }
  get Test() { return Test }

  /**
   * Launches a cds server with arbitrary port and returns a subclass which
   * also acts as an axios lookalike, providing methods to send requests.
   */
  run (cmd='.', ...args) {
    initLogging()

    // launch cds server...
    global.before (`launching ${cmd} ${args.join(' ')}...`, () => { // NOSONAR

      const {cds} = this
      if (!/^(serve|run)$/.test(cmd)) try {
        const project = cds.utils.isdir (cmd) || dirname (require.resolve (cmd+'/package.json'))
        cmd='serve'; args.push ('--project', project, '--in-memory?')
      } catch(e) {
        throw cds.error (`No such folder or package '${process.cwd()}' -> '${cmd}'`)
      }

      cds.once ('listening', ({server,url}) => {
        const axp = Reflect.getOwnPropertyDescriptor(this,'axios')
        if (axp) axp.value.defaults.baseURL = url
        this.server = server
        this.url = url
      })

      try { return cds.exec (cmd, ...args, '--port','0') }
      catch (e) { if (is_mocha) console.error(e) }
    })

    global.beforeEach (async () => {
      if (this.data._autoReset)  await this.data.reset()
    })

    // shutdown cds server...
    global.after (done => {
      this.server ? this.server.close (done) : done()
    })

    return this
  }

  /**
   * Serving projects from subfolders under the root specified by a sequence
   * of path components which are concatenated with path.resolve().
   */
  in (...paths) {
    const {cds} = this; cds.root = resolve (cds.root, ...paths)
    // Checking conflicts with global.cds loaded in other folder before
    const ep = Reflect.getOwnPropertyDescriptor(global.cds,'env')
    if (ep && ep.value && ep.value._home !== cds.root) throw new Error (`[cds.test] - 'cds.env' was invoked before 'cds.test.in' from a different home:

      cds.env._home: ${cds.env._home}
      cds.test.in:   ${cds.root}

    > throwing this as tests would likely behave erratically.
    `)
    return this
  }

  /**
   * Switch on/off console log output.
   */
  verbose(v) {
    v === false ? delete process.env.CDS_TEST_VERBOSE : process.env.CDS_TEST_VERBOSE=v
    initLogging()
    return this
  }

  /**
   * Lazily loads and returns an instance of chai
   */
  get chai() {
    const require = (mod) => { try { return module.require(mod) } catch(e) {
      if (e.code === 'MODULE_NOT_FOUND') throw new Error (`
      Failed to load required package '${mod}'. Please add it thru:
      npm add -D chai chai-as-promised chai-subset
    `)}}
    const chai = require('chai')
    chai.use (require('chai-subset'))
    chai.use (require('chai-as-promised'))
    return super.chai = chai
  }
  get expect(){ return this.chai.expect }
  get assert(){ return this.chai.assert }
  get sleep(){ return require('util').promisify(setTimeout) }
  get data() { return this._data || (this._data = new (require('./data')))}

}

// harmonizing jest and mocha
const is_jest = !!global.beforeAll
const is_mocha = !!global.before
if (is_mocha) {
  const { format } = require('util')
  global.beforeAll = global.before
  global.afterAll = global.after
  global.test = global.it
  global.it.each = (table) => (title,fn) => Promise.all (table.map(each => {
      if (!Array.isArray(each)) each = [each]
      return it (format(title,...each), ()=> fn(...each))
  }))
} else if (is_jest) { // it's jest
  global.before = (msg,fn) => global.beforeAll(fn||msg)
  global.after = (msg,fn) => global.afterAll(fn||msg)
} else { // it's none of both
  global.before = global.beforeAll = (_,fn) => fn()
  global.beforeEach = ()=>{}
  global.afterEach = ()=>{}
  global.after = global.afterAll = ()=>{}
}

function initLogging() {
  const levels = process.env.CDS_TEST_VERBOSE
    ? { deploy:'info', serve:'info', server:'info',cds:'info' }
    : { deploy:'warn', serve:'warn', server:'warn',cds:'silent'/*silences provoked request errors */ }

  const env = Reflect.getOwnPropertyDescriptor(global.cds,'env')
  for (const id of Object.keys(levels)) {
    if (env && env.value)
      global.cds.log(id, { level:levels[id] })
    else // uninitialized cds.env -> set env variables to avoid initializing cds.env eagerly
      process.env['CDS_LOG_LEVELS_'+id.toUpperCase()] = levels[id]
  }
}

/** @type Test.run & Test */
module.exports = Object.setPrototypeOf (Test.run, Test.prototype)

/* eslint no-console: off */