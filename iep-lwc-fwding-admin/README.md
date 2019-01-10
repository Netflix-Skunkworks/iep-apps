**LWC Metrics Forwarding Admin Service**

Service for managing the forwarding configurations.

- Expression validation API that will be hooked on to Configbin Create/Update API.
- Cross verify expressions with Spinnaker and AWS to determine whether the corresponding cluster is available and scaling policies are created. Flag the ones that needs attention.
- Have support for notifying the user.
- Have support for removing the flagged expressions.
 