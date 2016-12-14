import org.fao.ess.uploader.adam.bulk.AdamBulk;
import org.fao.ess.uploader.adam.bulk.AdamDS;
import org.fao.ess.uploader.core.init.UploaderConfig;
import org.junit.Before;
import org.junit.Test;

public class AdamBulkTest  {
    AdamBulk adamBulkManager;
    //AdamDS adamBulkManager;

    //private static final String RESOURCE_TEST = "testResources/country_indicators/country_indicators.csv";
    private static final String RESOURCE_TEST = "testResources/priorities/test_priorities.zip";


    @Before
    public void setUp() throws Exception {
        CDISupport.getInstance(UploaderConfig.class).init(this.getClass().getResourceAsStream("/mainConfig.properties"));
        adamBulkManager = CDISupport.getInstance(AdamBulk.class);
       // adamBulkManager = CDISupport.getInstance(AdamDS.class);
    }

    @Test
    public void mainLogic1() throws Exception {

        System.out.println("here");
        adamBulkManager.mainLogic(this.getClass().getResourceAsStream(RESOURCE_TEST));

        // adamBulkManager.mainLogic(this.getClass().getResourceAsStream(RESOURCE_TEST),"country_indicators");
        //adamBulkManager.mainLogic(this.getClass().getResourceAsStream(RESOURCE_TEST),"donors_gni");

    }
}
