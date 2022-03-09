package io.digdag.standards.command.kubernetes;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultKubernetesClientFactory
        implements KubernetesClientFactory
{
    @Override
    public KubernetesClient newClient(final KubernetesClientConfig kubernetesClientConfig)
    {
        Logger logger = LoggerFactory.getLogger(DefaultKubernetesClientFactory.class);
        final Config clientConfig = new ConfigBuilder()
                .withMasterUrl(kubernetesClientConfig.getMaster())
                .withNamespace(kubernetesClientConfig.getNamespace())
                .withCaCertData(kubernetesClientConfig.getCertsCaData())
                .withOauthToken(kubernetesClientConfig.getOauthToken())
                .build();
        logger.debug("###clientConfig:{}",clientConfig);
        return new DefaultKubernetesClient(kubernetesClientConfig,
                new io.fabric8.kubernetes.client.DefaultKubernetesClient(clientConfig));
    }
}
