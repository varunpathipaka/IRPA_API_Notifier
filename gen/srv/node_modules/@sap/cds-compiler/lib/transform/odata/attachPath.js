// The module traverses over a CSN or partial one and sets $path on the non-structural nodes and references.

const structuralNodeHandlers = {
  definitions: traverseDict,
  elements: traverseDict,
  actions: traverseDict,
  params: traverseDict,
  items: traverseTyped,
  enum: traverseDict,
  returns: traverseTyped,
  on: traverseArray,
  keys: traverseArray,
  ref: traverseRef,
  query: traverseTyped,
  SELECT: traverseTyped,
  SET: traverseTyped,
  args: traverseArray,
  columns: traverseArray,
  projection: traverseTyped,
  from: traverseTyped,
  mixin: traverseDict,
  where: traverseArray,
  orderBy: traverseArray,
  groupBy: traverseArray,
  having: traverseArray,
  xpr: traverseArray,
  expand: traverseArray,
  inline: traverseArray,
}

function attachPath(csn) {
  traverseDict(csn.definitions, ['definitions']);
}

function attachPathOnPartialCSN(csnPart, pathPrefix) {
  if(Array.isArray(csnPart))
    traverseArray(csnPart, pathPrefix);
  else
    traverseDict(csnPart, pathPrefix);
}

function traverseRef(obj, path) {
  if(!obj) return;
  setPath(obj, path);
  traverseArray(obj, path);
}

function traverseArray(obj, path) {
  if(!Array.isArray(obj)) return;
  obj.forEach( ( element, index ) => traverseTyped(element, path.concat(index)));
}

function traverseDict(obj, path) {
  if(!obj || typeof obj !== 'object') return;
  forAllEnumerableProperties(obj, name => {
    const ipath = path.concat(name);
    setPath(obj[name], ipath);
    traverseTyped(obj[name], ipath);
  })
}

function traverseDictArray(obj, path) {
  if(!obj || typeof obj !== 'object') return;
  forAllEnumerableProperties(obj, name => {
    const ipath = path.concat(name);
    setPath(obj[name], ipath);
    traverseArray(obj[name], ipath);
  })
}

function traverseTyped(obj, path) {
  if(!obj || typeof obj !== 'object') return;
  forAllEnumerableProperties(obj, name => {
    if(name[0]==='@') return; // skip annotations
    const func = structuralNodeHandlers[name];
    if(func)
      func(obj[name], path.concat(name));
    else if(path[path.length-2] === 'columns')
      traverseDictArray(obj[name], path.concat(name)); // for columns
  })
}

function setPath(obj, path) {
  if(!obj || typeof obj !== 'object') return;
  if(path.length>0)
    Object.defineProperty( obj, '$path', { value: path, configurable: true, writable: true, enumerable: false } );
}

function forAllEnumerableProperties(obj, callback) {
  Object.keys(obj).forEach(callback);
}

module.exports = {
  attachPath,
  attachPathOnPartialCSN,
}
