package org.fao.ess.uploader.core.metadata;

import org.fao.ess.uploader.core.init.UploaderConfig;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

@ApplicationScoped
public class MetadataStorageFactory {
    @Inject private UploaderConfig config;
    @Inject private Instance<MetadataStorage> instanceFactory;

    private MetadataStorage instance;

    public MetadataStorage getInstance() throws Exception {
        if (instance==null) {
            String pluginClassName = config.get("metadata.plugin");
            if (pluginClassName!=null)
                instance = (MetadataStorage) instanceFactory.select((Class<? extends MetadataStorage>)Class.forName(pluginClassName));
            instance.init();
        }
        return instance;
    }
}
