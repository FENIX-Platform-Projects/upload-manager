import org.fao.ess.uploader.adam.bulk.AdamBulk;
import org.fao.ess.uploader.core.init.UploaderConfig;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.FileInputStream;
import java.io.IOException;

public class AdamBulkTest  {
    AdamBulk adamBulkManager;

    private static final String RESOURCE_TEST = "testResources/priorities/test_priorities.zip";

    @Before
    public void setUp() throws Exception {
        CDISupport.getInstance(UploaderConfig.class).init(this.getClass().getResourceAsStream("/mainConfig.properties"));
        adamBulkManager = CDISupport.getInstance(AdamBulk.class);
    }

    @Test
    public void mainLogic1() throws Exception {

        System.out.println("here");
        adamBulkManager.mainLogic( this.getClass().getResourceAsStream(RESOURCE_TEST));
    }
}
