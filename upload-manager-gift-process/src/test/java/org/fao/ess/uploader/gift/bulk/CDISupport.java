package org.fao.ess.uploader.gift.bulk;

import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;

public class CDISupport {
    static WeldContainer weld = new Weld().initialize();

    public static <T> T getInstance(Class<T> beanClass) {
        return weld.select(beanClass).get();
    }
}
