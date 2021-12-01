const measurePerformance = require('./performance')

let dynatrace
try {
  const sdk = require('@dynatrace/oneagent-sdk')
  const api = sdk.createInstance()
  dynatrace = {
    sdk,
    api
  }
} catch (_) {
  // ignore
}

const sapStatistics = (req, res, next) => {
  measurePerformance(req, res)
  next()
}

const useDynatrace = (req, res, next) => {
  req.dynatrace = dynatrace
  next()
}

const performanceMeasurement = app => {
  app.use(sapStatistics)

  if (dynatrace) {
    app.use(useDynatrace)
  }
}

module.exports = performanceMeasurement
