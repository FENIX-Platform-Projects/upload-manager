package org.fao.ess.uploader.core.storage;

import org.fao.ess.uploader.core.init.UploaderConfig;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

@ApplicationScoped
public class BinaryStorageFactory {
    @Inject private UploaderConfig config;
    @Inject private Instance<BinaryStorage> instanceFactory;

    private BinaryStorage instance;

    public BinaryStorage getInstance() throws Exception {
        if (instance==null) {
            String pluginClassName = config.get("storage.plugin");
            if (pluginClassName!=null)
                instance = (BinaryStorage) instanceFactory.select((Class<? extends BinaryStorage>)Class.forName(pluginClassName));
            instance.init();
        }
        return instance;
    }
}
