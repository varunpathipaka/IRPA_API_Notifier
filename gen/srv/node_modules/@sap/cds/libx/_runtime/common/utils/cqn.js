const getEntityNameFromDeleteCQN = cqn => {
  let from
  if (cqn && cqn.DELETE && cqn.DELETE.from) {
    if (typeof cqn.DELETE.from === 'string') {
      from = cqn.DELETE.from
    } else if (cqn.DELETE.from.name) {
      from = cqn.DELETE.from.name
    } else if (cqn.DELETE.from.ref && cqn.DELETE.from.ref.length === 1) {
      from = cqn.DELETE.from.ref[0]
    }
  }
  return from
}

const getEntityNameFromUpdateCQN = cqn => {
  return (cqn.UPDATE.entity.ref && cqn.UPDATE.entity.ref[0]) || cqn.UPDATE.entity.name || cqn.UPDATE.entity
}

// scope: simple wheres à la "[{ ref: ['foo'] }, '=', { val: 'bar' }, 'and', ... ]"
function where2obj(where, target = null) {
  const data = {}
  for (let i = 0; i < where.length; i++) {
    const whereEl = where[i]
    const colName = whereEl.ref && whereEl.ref[whereEl.ref.length - 1]
    // optional validation if target is passed
    if (target) {
      const colEl = target.elements[colName]
      if (!colEl || !colEl.key) continue
    }
    const opWhere = where[i + 1]
    const valWhere = where[i + 2]
    if (opWhere === '=' && valWhere && 'val' in valWhere) data[colName] = valWhere.val
  }
  return data
}

module.exports = {
  getEntityNameFromDeleteCQN,
  getEntityNameFromUpdateCQN,
  where2obj
}
