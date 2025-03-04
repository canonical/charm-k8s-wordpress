# Wordpress Operator

A [Juju](https://juju.is/) [charm](https://juju.is/docs/olm/charmed-operators) deploying and managing WordPress on Kubernetes. [WordPress](https://wordpress.com/) is the world's most popular website builder, and it's free and open-source.

This charm simplifies initial deployment and operations of WordPress on Kubernetes, including scaling the number of instances, integration with SSO, access to OpenStack Swift object storage for redundant file storage, and more. It allows for deployment on many different Kubernetes platforms, from [MicroK8s](https://microk8s.io/) to [Charmed Kubernetes](https://ubuntu.com/kubernetes) to public cloud Kubernetes offerings.

This charm will make operating WordPress simple and straightforward for DevOps or SRE teams through Juju's clean interface. It will allow easy deployment into multiple environments to test changes and support scaling out for enterprise deployments.

## In this documentation

| | |
|--|--|
|  [Tutorials](https://charmhub.io/wordpress-k8s/docs/tutorials-getting-started)</br>  Get started - a hands-on introduction to using the Charmed WordPress operator for new users </br> |  [How-to guides](https://charmhub.io/wordpress-k8s/docs/how-to-contribute) </br> Step-by-step guides covering key operations and common tasks |
| [Reference](https://charmhub.io/wordpress-k8s/docs/reference-actions) </br> Technical information - specifications, APIs, architecture | [Explanation](https://charmhub.io/wordpress-k8s/docs/explanation-overview) </br> Concepts - discussion and clarification of key topics  |

## Contributing to this documentation

Documentation is an important part of this project, and we take the same open-source approach to the documentation as the code. As such, we welcome community contributions, suggestions, and constructive feedback on our documentation. Our documentation is hosted on the [Charmhub forum](https://discourse.charmhub.io/t/wordpress-documentation-overview/4052) to enable easy collaboration. Please use the "Help us improve this documentation" links on each documentation page to either directly change something you see that's wrong, ask a question, or make a suggestion about a potential change via the comments section.

If there's a particular area of documentation that you'd like to see that's missing, please [file a bug](https://github.com/canonical/wordpress-k8s-operator/issues).

## Project and community

The WordPress Operator is a member of the Ubuntu family. It's an open-source project that warmly welcomes community projects, contributions, suggestions, fixes, and constructive feedback.

- [Code of conduct](https://ubuntu.com/community/code-of-conduct)
- [Get support](https://discourse.charmhub.io/)
- [Join our online chat](https://matrix.to/#/#charmhub-charmdev:ubuntu.com)
- [Contribute](https://github.com/canonical/wordpress-k8s-operator/blob/main/CONTRIBUTING.md)

Thinking about using the WordPress Operator for your next project? [Get in touch](https://matrix.to/#/#charmhub-charmdev:ubuntu.com)!

# Contents

1. [Tutorial](tutorial.md)
1. [How-to](how-to/index.md)
  1. [Retrieve initial credentials](how-to/retrieve-initial-credentials.md)
  1. [Configure initial settings](how-to/configure-initial-settings.md)
  1. [Configure hostname](how-to/configure-hostname.md)
  1. [Configure object storage](how-to/configure-object-storage.md)
  1. [Install plugins](how-to/install-plugins.md)
  1. [Install themes](how-to/install-themes.md)
  1. [Integrate with COS](how-to/integrate-with-cos.md) 
  1. [Enable antispam](how-to/enable-antispam.md)
  1. [Enable WAF](how-to/enable-waf.md)  
  1. [Rotate secrets](how-to/rotate-secrets.md)
  1. [Upgrade WordPress charm](how-to/upgrade-wordpress-charm.md)
  1. [Redeploy](how-to/redeploy.md)
  1. [Contribute](how-to/contribute.md)
1. [Reference](reference)
  1. [Actions](reference/actions.md)
  1. [Configurations](reference/configurations.md)
  1. [Integrations](reference/integrations.md)
  1. [Plugins](reference/plugins.md)
  1. [Themes](reference/themes.md)
1. [Explanation](explanation)
  1. [Overview](explanation/overview.md)
  1. [Containers](explanation/containers.md)
  1. [Lifecycle events](explanation/lifecycle-events.md)
  1. [OCI image](explanation/oci-image.md)
  1. [Relations](explanation/relations.md)
