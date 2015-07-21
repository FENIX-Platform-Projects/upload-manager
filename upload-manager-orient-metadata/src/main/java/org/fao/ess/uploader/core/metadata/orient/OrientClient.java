package org.fao.ess.uploader.core.metadata.orient;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class OrientClient {

    //DATABASE CONNECTION

    private String url,usr,psw;
    private ODatabaseDocumentPool pool;

    public void initPool(String url,String usr,String psw) {
        this.url = url;
        this.usr = usr!=null ? usr : "admin";
        this.psw = psw!=null ? psw : "admin";
        pool = ODatabaseDocumentPool.global(10,300);
    }

    public ODatabaseDocumentTx getConnection() {
        return pool.acquire(url,usr,psw);
    }


}
