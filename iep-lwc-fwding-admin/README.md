## LWC Metrics Forwarding Admin Service

Service for managing the forwarding configurations.

- Expression validation API that will be hooked on to Configbin Create/Update 
  API.
- Mark the expressions that are not forwarding any data and the ones with 
  no scaling policy attached.
- Have support for removing the flagged expressions.
 