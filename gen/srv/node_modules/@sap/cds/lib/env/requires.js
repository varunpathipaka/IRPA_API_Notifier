const _runtime = '../../libx/_runtime'

module.exports = {
  "db": undefined,
  "multitenancy": undefined,
  "app-service": {
    // this is the default implementation used for provided services
    impl: `${_runtime}/cds-services/services/Service.js`
  },
  "auth": {
    '[development]': { kind: 'mocked-auth' },
    '[production]': { kind: 'jwt-auth' }
  },
  "dummy-auth": {
    strategy: 'dummy',
  },
  "basic-auth": {
    kind: "mocked-auth"
  },
  "mocked-auth": {
    strategy: 'mock',
    users: {
      alice: { roles: ['admin'] },
      bob: { roles: ['builder'] },
      '*': true
    }
  },
  "jwt-auth": {
    strategy: 'JWT',
  },
  "xsuaa-auth": {
    strategy: 'xsuaa',
  },
  destinations: {
    vcap: {
      label: 'destination'
    }
  },
  xsuaa: {
    vcap: {
      label: 'xsuaa'
    }
  },
  monitoring: undefined,
  logging: undefined,
  audit: undefined,
  "sql": {
    '[development]': { kind: 'sqlite', credentials: { url: ':memory:' } },
    '[production]': { kind: 'hana' },
  },
  "sqlite": _compat_to_use({
    dialect: 'sqlite', credentials: { url: 'sqlite.db' },
    impl: `${_runtime}/sqlite/Service.js`,
  }),
  "hana": _compat_to_use ({
    dialect: 'hana',
    impl: `${_runtime}/hana/Service.js`,
  }),
  "rest": {
    impl: `${_runtime}/remote/Service.js`
  },
  "odata": {
    impl: `${_runtime}/remote/Service.js`
  },
  "odata-v2": {
    kind: 'odata'
  },
  "odata-v4": {
    kind: 'odata'
  },
  "local-messaging": {
    impl: `${_runtime}/messaging/service.js`,
    local: true
  },
  "file-based-messaging": {
    outbox: true,
    impl: `${_runtime}/messaging/file-based.js`,
    credentials: { file:'~/.cds-msg-box' }
  },
  "default-messaging": {
    "[development]": { "kind": "local-messaging" },
    "[hybrid]": { "kind": "enterprise-messaging-amqp" },
    "[production]": {
      "kind": "enterprise-messaging-amqp",
      "[multitenant]": { "kind": "enterprise-messaging-http" }
    }
  },
  "enterprise-messaging": {
    kind: "enterprise-messaging-http",
  },
  "enterprise-messaging-shared": { // for temporary compat only
    kind: "enterprise-messaging-amqp",
  },
  "enterprise-messaging-http": {
    outbox: true,
    impl: `${_runtime}/messaging/enterprise-messaging.js`,
    vcap: { label: "enterprise-messaging" },
  },
  "enterprise-messaging-amqp": {
    outbox: true,
    impl: `${_runtime}/messaging/enterprise-messaging-shared.js`,
    vcap: { label: "enterprise-messaging" },
  },
  'message-queuing': {
    outbox: true,
    impl: `${_runtime}/messaging/message-queuing.js`
  },
  "composite-messaging": {
    impl: `${_runtime}/messaging/composite.js`
  },
  "audit-log": {
    impl: `${_runtime}/audit/Service.js`,
    // REVISIT: how to load model?
    // model: 'AuditLogService.cds',
    outbox: true,
    vcap: { label: "auditlog" },
  },

  _prototypes: {
    "uiflex": {
      "model": "@sap/cds/libx/_runtime/fiori/uiflex/extensibility"
    },
  }
}


function _compat_to_use(o) { return Object.defineProperties (o,{
  // NOTE: Property .use is for compatibility only -> use .dialect instead!
  use: { get(){ return this.dialect }, enumerable:true },
})}
