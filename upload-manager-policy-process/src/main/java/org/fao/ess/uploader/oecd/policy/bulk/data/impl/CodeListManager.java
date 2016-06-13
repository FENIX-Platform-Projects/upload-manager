package org.fao.ess.uploader.oecd.policy.bulk.data.impl;

import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.Code;
import org.fao.fenix.commons.msd.dto.full.DSDCodelist;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.utils.FileUtils;
import org.fao.fenix.commons.utils.JSONUtils;
import org.fao.fenix.commons.utils.Language;
import org.fao.fenix.commons.utils.database.DatabaseUtils;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.LinkedList;

public class CodeListManager {
    @Inject FileUtils fileUtils;
    @Inject JSONUtils jsonUtils;
    @Inject DatabaseUtils databaseUtils;


    public Resource<DSDCodelist, Code> getSimpleCodeList(Connection connection, CodeList codeList) throws Exception {
        //Create metadata
        MeIdentification<DSDCodelist> metadata = new MeIdentification<>();
        metadata.setUid(codeList.getUid());
        metadata.setVersion(codeList.getVersion());
        //Load codes data
        ResultSet data = connection.createStatement().executeQuery(codeList.getQuery());
        //Parse data
        Collection<Code> codes = new LinkedList<>();
        while (data.next()) {
            Code code = new Code();
            code.setCode(data.getString(1));
            code.addTitle(Language.english.getCode(), data.getString(2));
            codes.add(code);
        }
        //Return resource
        return new Resource<>(metadata, codes);
    }
}
