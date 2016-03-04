package sftp;

import com.jcraft.jsch.SftpProgressMonitor;

import java.util.List;

public class CustomProgressMonitor implements SftpProgressMonitor {

    int count = 0;
    long max = 0;

    private String fileName ="";
    private String policyId = "";

    public CustomProgressMonitor(String fileName, String policyId){
        this.fileName = fileName;
        this.policyId = policyId;
    }

    public void init(int op, String src, String dest, long max){
        this.max = max;
        System.out.println("!op:"+op+", src= "+src+", dest="+dest+", max="+max);
    }

    public boolean count(long count){
        this.count+=count;
        System.out.println("count: "+count);
        return true;
    }

    public void end(){
        System.out.println("end");
        System.out.println((int)this.max);
        PolicyDataManager policyDataManager = new PolicyDataManager();
        try {
            //policyDataManager.updateLinkPdf(policyDataManager.dbConnect(), this.fileName, this.policyId);
        } catch (Exception e) {
            e.printStackTrace();
        }

//        String linkPdfToAdd = "";
//        try {
//            System.out.println("CustomProgressMonitor before connect");
//            String selectQuery = "SELECT link_pdf FROM policytable where policy_id = "+ Integer.parseInt(this.policyId);
//            List<String> selectValues = policyDataManager.select(policyDataManager.dbConnect(), selectQuery);
//            for(int i=0; i< selectValues.size(); i++){
//                String value = selectValues.get(i);
//                System.out.println("i= "+i );
//                if((value!=null)&&(value.length()>1)){
//                    System.out.println("i= "+i + " value = "+ value);
//                    //Add another link to the list of link
//                    linkPdfToAdd = value +";";
//                }
//                linkPdfToAdd += this.fileName;
//            }
//            String updateQuery = "UPDATE policytable SET link_pdf= '"+linkPdfToAdd+"' where policy_id = "+ Integer.parseInt(this.policyId);
//            policyDataManager.update(policyDataManager.dbConnect(), updateQuery);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getPolicyId() {
        return policyId;
    }

    public void setPolicyId(String policyId) {
        this.policyId = policyId;
    }
}
