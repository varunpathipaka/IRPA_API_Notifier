'use strict';

/**
 * If a mixin association is published, return the mixin association.
 *
 * @param {CSN.Query} query Query of the artifact to check
 * @param {object} association Association (Element) published by the view
 * @param {string} associationName
 * @returns {object} The mixin association
 */
function getMixinAssocOfQueryIfPublished(query, association, associationName) {
  if (query && query.SELECT && query.SELECT.mixin) {
    const aliasedColumnsMap = Object.create(null);
    if (query.SELECT.columns) {
      for (const column of query.SELECT.columns) {
        if (column.as && column.ref && column.ref.length === 1)
          aliasedColumnsMap[column.as] = column;
      }
    }

    for (const elem of Object.keys(query.SELECT.mixin)) {
      const mixinElement = query.SELECT.mixin[elem];
      let originalName = associationName;
      if (aliasedColumnsMap[associationName])
        originalName = aliasedColumnsMap[associationName].ref[0];

      if (elem === originalName)
        return { mixinElement, mixinName: originalName };
    }
  }
  return {};
}

/**
 * Check wether the given artifact uses the given mixin association.
 *
 * @param {CSN.Query} query Query of the artifact to check
 * @param {object} association Mixin association (Element) to check for
 * @param {string} associationName
 * @returns {boolean} True if used
 */
function usesMixinAssociation(query, association, associationName) {
  if (query && query.SELECT && query.SELECT.columns) {
    for (const column of query.SELECT.columns) {
      if (typeof column === 'object' && column.ref && column.ref.length > 1 && (column.ref[0] === associationName || column.ref[0].id === associationName)) {
        // FIXME: This is not necessarily correct: the assoc name needs not be the first component, as e.g. $projection.assoc
        // would be also valid. Check other paths like $self.assoc ....
        return true;
      }
    }
  }
  return false;
}

module.exports = {
  usesMixinAssociation,
  getMixinAssocOfQueryIfPublished,
};
