'use strict';

// We only care about the "wild" ones - $at is validated by the compiler
const magicVariables = {
  $user: [
    'id',     // $user.id
    'locale', // $user.locale
  ],
  $session: [
    // no valid ways for this
  ],
};

/**
 * Check that the given ref does not use magic variables for which we don't have
 * a valid way of rendering.
 *
 * Valid ways:
 * - We know what to do -> $user.id on HANA
 * - The user tells us what to do -> options.magicVars
 *
 * @param {object} parent Object with the ref as a property
 * @param {string} name Name of the ref property on parent
 * @param {Array} ref to check
 */
function unknownMagicVariable(parent, name, ref) {
  if (parent.$scope && parent.$scope === '$magic') {
    const [ head, ...rest ] = ref;
    const tail = rest.join('.');
    const magicVariable = magicVariables[head];
    if (magicVariable && magicVariable.indexOf(tail) === -1)
      this.error(null, parent.$location, { id: tail, elemref: parent }, 'Magic variable is not supported - path $(ELEMREF), step $(ID)');
  }
}

module.exports = {
  ref: unknownMagicVariable,
};
