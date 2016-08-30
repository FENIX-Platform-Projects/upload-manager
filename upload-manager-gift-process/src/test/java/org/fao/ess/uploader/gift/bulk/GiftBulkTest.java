package org.fao.ess.uploader.gift.bulk;

import org.fao.ess.uploader.core.init.UploaderConfig;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class GiftBulkTest {
    GiftBulk giftBulkManager;

    @Before
    public void setUp() throws Exception {
        CDISupport.getInstance(UploaderConfig.class).init(this.getClass().getResourceAsStream("/mainConfig.properties"));
        giftBulkManager = CDISupport.getInstance(GiftBulk.class);
    }

    @Test
    public void mainLogic1() throws Exception {
        giftBulkManager.mainLogic(this.getClass().getResourceAsStream("/test/burkina_test1.zip"));
    }

}