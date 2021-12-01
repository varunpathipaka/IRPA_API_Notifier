// The module traverses a given CSN using a specific path, collects structural node names and returns them.

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
  ref: traverseArray,
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
  cast: traverseTyped,
}

function structuralPath(csn, path) {
  return traverseDict(csn.definitions, path, 1, ['definitions']);
}

function traverseArray(obj, path, index, typeStack) {
  if(!Array.isArray(obj)) return typeStack;
  const name = path[index];
  const element = obj[name];
  return traverseTyped(element, path, index+1, typeStack);
}

function traverseDict(obj, path, index, typeStack) {
  if(typeof obj !== 'object') return typeStack;
  const name = path[index];
  if(name === undefined) return typeStack;
  return traverseTyped(obj[name], path, index+1, typeStack);
}

function traverseDictArray(obj, path, index, typeStack) {
  if(typeof obj !== 'object') return typeStack;
  const name = path[index];
  if(name === undefined) return typeStack;
  return traverseArray(obj[name], path, index+1, typeStack);
}

function traverseTyped(obj, path, index, typeStack) {
  if(!obj) return typeStack;
  const name = path[index];
  if(name === undefined) return typeStack;
  if(name[0] === '@') return typeStack; // skip annotations
  const func = structuralNodeHandlers[name];
  if(func) return func(obj[name], path, index+1, typeStack.concat(name));
  // not typed -> columns?
  if(typeStack[typeStack.length-1] === 'columns')
    return traverseDictArray(obj, path, index, typeStack);
  return typeStack;
}

module.exports = {
  structuralPath
}
