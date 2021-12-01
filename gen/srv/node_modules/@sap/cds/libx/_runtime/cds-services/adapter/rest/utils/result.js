/**
 * Convert any result to the format expected of rest.
 *
 * @param {*} result
 * @returns {string | object | boolean}
 */
const toRestResult = result => {
  let restResult = result

  // compat for mtx returning strings instead of objects
  if (typeof result === 'object' && '$count' in result) {
    restResult = {
      count: result.$count,
      value: result
    }
  } else if (typeof result === 'number') {
    restResult = result.toString()
  }

  return restResult
}

module.exports = {
  toRestResult
}
