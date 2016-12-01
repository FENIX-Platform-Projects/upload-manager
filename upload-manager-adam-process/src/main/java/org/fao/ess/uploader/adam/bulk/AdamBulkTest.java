package org.fao.ess.uploader.adam.bulk;


import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.FileInputStream;
import java.io.IOException;

@WebServlet(urlPatterns = "/testAdamBulk")
public class AdamBulkTest extends HttpServlet {
    @Inject AdamBulk adamBulkManager;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        System.out.println("HERER!");
        try {
            adamBulkManager.mainLogic( new FileInputStream("/media/meco/Develop/Projects/FAO/Fenix/gitRepo/upload-manager/upload-manager-policy-process/test/test.zip"));
            resp.getWriter().print("<html><body><h1>Done...</h1></body></html>");
        } catch (Exception ex) {
            ex.printStackTrace(resp.getWriter());
        }
    }
}
