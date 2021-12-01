namespace cds_r; //> cds system tables

service ExtensibilityService @(path:'/-/cds/extensibility') {
  action addExtension(extensions : array of String); // TODO: change to array of CSN extensions
}

entity Extensions {
  key ID : UUID;
  csn    : String;
}

// @open type Extension {
//   extend: String;
//   elements:{ /*  */ }
// }
