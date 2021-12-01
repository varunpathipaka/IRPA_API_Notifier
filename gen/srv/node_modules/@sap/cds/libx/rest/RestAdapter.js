const cds = require('../_runtime/cds')

const express = require('express')

const auth = require('./middleware/auth')
const content = require('./middleware/content')
const parse = require('./middleware/parse')

const create = require('./middleware/create')
const read = require('./middleware/read')
const update = require('./middleware/update')
const deleet = require('./middleware/delete')
const operation = require('./middleware/operation')

const error = require('./middleware/error')

// REVISIT: _commit_attempted workaround to avoid double rollback leading to release error
const _commit_attempted = Symbol()

class RestAdapter extends express.Router {
  constructor(srv) {
    super()

    this.use(express.json())

    // pass srv-reated stuff to middlewares via req
    this.use('/', (req, res, next) => {
      req._srv = srv
      next()
    })

    // check @requires as soon as possible (DoS)
    this.use('/', auth)

    // content-type check
    this.use('/', content)

    // parse
    this.use('/', parse)

    // begin tx
    this.use('/', (req, res, next) => {
      // create tx and set as cds.context
      // REVISIT: _model should not be necessary
      req._tx = cds.context = srv.tx({ user: req.user, req, res, _model: req._srv.model })
      next()
    })

    // POST
    this.post('/*', (req, res, next) => {
      if (req._operation) operation(req, res, next)
      else create(req, res, next)
    })

    // GET
    this.get('/*', (req, res, next) => {
      if (req._operation) operation(req, res, next)
      else read(req, res, next)
    })

    // PUT, PATCH, DELETE
    this.put('/*', update)
    this.patch('/*', update)
    this.delete('/*', deleet)

    // end tx (i.e., commit or rollback)
    this.use('/', async (req, res, next) => {
      const { result, status, location } = req._result

      // unfortunately, express doesn't catch async errors -> try catch needed
      try {
        req._tx[_commit_attempted] = true
        await req._tx.commit(result)
      } catch (e) {
        return next(e)
      }

      // TODO: cf. bufferToBase64() in old rest adapter

      // only set status if not yet modified
      if (status && res.statusCode === 200) res.status(status)
      if (location) res.set('location', location)
      res.send(result)
    })
    this.use('/', (err, req, res, next) => {
      // request may fail during processing or during commit -> both caught here

      // ignore rollback error, which should never happen
      if (req._tx && !req._tx[_commit_attempted]) req._tx.rollback(err).catch(() => {})

      next(err)
    })

    /*
     * error handling
     */
    this.use(error)
  }
}

module.exports = RestAdapter
