const cds = require('../cds')
const LOG = cds.log('audit-log')

const v2utils = require('./utils/v2')

const ANONYMOUS = 'anonymous'

const _getTenantAndUser = () => ({
  user: (cds.context && cds.context.user && cds.context.user.id) || ANONYMOUS,
  tenant: (cds.context && cds.context.tenant) || ANONYMOUS
})

module.exports = class AuditLogService extends cds.MessagingService {
  async init() {
    // call MessagingService's init, which handles outboxing
    await super.init()
    // connect to audit log service
    // REVISIT for GA: throw error on connect issue instead of warn and this.ready?
    this.alc = await v2utils.connect(this.options.credentials)
    this.ready = !!this.alc
  }

  async emit(first, second) {
    const { event, data } = typeof first === 'object' ? first : { event: first, data: second }
    if (!this.options.outbox) return this.send(event, data)

    if (this.ready && this[event]) {
      // best effort until persistent outbox -> only log the failures
      try {
        await this[event](data)
      } catch (e) {
        LOG._warn && LOG.warn(e)
      }
    }
  }

  async send(event, data) {
    if (this.ready && this[event]) return this[event](data)
  }

  async dataAccessLog({ accesses }) {
    if (!this.ready) throw new Error('AuditLogService not connected')

    const { tenant, user } = _getTenantAndUser()

    // build the logs
    const { entries, errors } = v2utils.buildDataAccessLogs(this.alc, accesses, tenant, user)
    if (errors.length) {
      throw errors.length === 1 ? errors[0] : Object.assign(new Error('MULTIPLE_ERRORS'), { details: errors })
    }

    // write the logs
    await Promise.all(
      entries.map(entry => {
        v2utils.sendDataAccessLog(entry).catch(err => errors.push(err))
      })
    )
    if (errors.length) {
      throw errors.length === 1 ? errors[0] : Object.assign(new Error('MULTIPLE_ERRORS'), { details: errors })
    }
  }

  // REVISIT: modification.action not used in auditlog v2
  async dataModificationLog({ modifications }) {
    if (!this.ready) throw new Error('AuditLogService not connected')

    const { tenant, user } = _getTenantAndUser()

    // build the logs
    const { entries, errors } = v2utils.buildDataModificationLogs(this.alc, modifications, tenant, user)
    if (errors.length) {
      throw errors.length === 1 ? errors[0] : Object.assign(new Error('MULTIPLE_ERRORS'), { details: errors })
    }

    // write the logs
    await Promise.all(
      entries.map(entry => {
        v2utils.sendDataModificationLog(entry).catch(err => errors.push(err))
      })
    )
    if (errors.length) {
      throw errors.length === 1 ? errors[0] : Object.assign(new Error('MULTIPLE_ERRORS'), { details: errors })
    }
  }

  async securityLog({ action, data }) {
    if (!this.ready) throw new Error('AuditLogService not connected')

    // cds.context not always set on auth-related errors -> try to extract from data
    let user, tenant
    if (cds.context) {
      const tenantAndUser = _getTenantAndUser()
      tenant = tenantAndUser.tenant
      user = tenantAndUser.user
    } else {
      try {
        const parsed = JSON.parse(data)
        if (parsed.tenant) {
          tenant = parsed.tenant
          delete parsed.tenant
        }
        if (parsed.user && typeof parsed.user === 'string') {
          user = parsed.user
          delete parsed.user
        }
        data = JSON.stringify(parsed)
      } catch (e) {}
    }
    if (!tenant) tenant = ANONYMOUS
    if (!user) user = ANONYMOUS

    // build the log
    const entry = v2utils.buildSecurityLog(this.alc, action, data, tenant, user)

    // write the log
    await v2utils.sendSecurityLog(entry)
  }

  // REVISIT: action and success not used in auditlog v2
  async configChangeLog({ action, success, configurations }) {
    if (!this.ready) throw new Error('AuditLogService not connected')

    const { tenant, user } = _getTenantAndUser()

    // build the logs
    const { entries, errors } = v2utils.buildConfigChangeLogs(this.alc, configurations, tenant, user)
    if (errors.length) {
      throw errors.length === 1 ? errors[0] : Object.assign(new Error('MULTIPLE_ERRORS'), { details: errors })
    }

    // write the logs
    await Promise.all(
      entries.map(entry => {
        v2utils.sendConfigChangeLog(entry).catch(err => errors.push(err))
      })
    )
    if (errors.length) {
      throw errors.length === 1 ? errors[0] : Object.assign(new Error('MULTIPLE_ERRORS'), { details: errors })
    }
  }
}
