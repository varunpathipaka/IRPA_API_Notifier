function _flattenDeep(arr) {
  return arr.reduce((acc, val) => acc.concat(Array.isArray(val) ? _flattenDeep(val) : val), [])
}

/*
 * flatten with a dfs approach. this is important!!!
 */
function getFlatArray(arg) {
  if (!Array.isArray(arg)) return [arg]
  return _flattenDeep(arg)
}

async function _processChunk(processFn, model, dbc, cqns, user, locale, ts, indexes, results) {
  const promises = []
  for (const i of indexes) promises.push(processFn(model, dbc, cqns[i], user, locale, ts))
  const promisesResults = await Promise.allSettled(promises)
  const firstRejected = promisesResults.find(r => r.status === 'rejected')
  if (firstRejected) throw firstRejected.reason
  // put results of queries into correct place of return results
  for (let i = 0; i < promisesResults.length; i++) results[indexes[i]] = promisesResults[i].value
}

async function processCQNs(processFn, cqns, model, dbc, user, locale, ts, chunks) {
  /*
   * execute deletes first, but keep results in order of cqns
   */

  const results = new Array(cqns.length)

  const deletes = []
  const updatesBeforeDelete = []
  const others = []
  for (let i = 0; i < cqns.length; i++) {
    if (cqns[i].DELETE) deletes.push(i)
    else if (cqns[i].UPDATE && cqns[i].UPDATE._beforeDelete) updatesBeforeDelete.push(i)
    else others.push(i)
  }

  // UPDATEs to SET null parent's foreign keys of one compositions
  // which are otherwise violate foreign key constraints
  if (updatesBeforeDelete.length) {
    await _processChunk(processFn, model, dbc, cqns, user, locale, ts, updatesBeforeDelete, results)
  }

  if (deletes.length > 0) {
    if (chunks) {
      let offset = 0
      for (const amount of chunks) {
        const indexes = deletes.slice(offset, offset + amount)
        await _processChunk(processFn, model, dbc, cqns, user, locale, ts, indexes, results)
        offset += amount
      }
    } else {
      await _processChunk(processFn, model, dbc, cqns, user, locale, ts, deletes, results)
    }
  }

  if (others.length > 0) {
    await _processChunk(processFn, model, dbc, cqns, user, locale, ts, others, results)
  }

  return results
}

module.exports = {
  getFlatArray,
  processCQNs
}
