const cds = require('../../../../cds')

const getCredentials = uaa => {
  uaa =
    uaa && uaa.credentials
      ? uaa
      : cds.env.requires.uaa && cds.env.requires.uaa.credentials
      ? cds.env.requires.uaa
      : cds.env.requires.xsuaa && cds.env.requires.xsuaa.credentials
      ? cds.env.requires.xsuaa
      : {}

  if (!uaa.credentials)
    throw Object.assign(new Error('No or malformed uaa credentials'), { credentials: uaa.credentials })

  return uaa.credentials
}

module.exports = {
  getCredentials
}
