package org.fao.ess.uploader.oecd.policy.bulk.utils;

import org.fao.fenix.commons.find.dto.filter.FieldFilter;
import org.fao.fenix.commons.find.dto.filter.IdFilter;
import org.fao.fenix.commons.find.dto.filter.StandardFilter;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;

@ApplicationScoped
public class D3SClient {

    public Collection<MeIdentification<DSDDataset>> retrieveMetadata(String baseUrl) throws Exception {
        //Create filter
        StandardFilter filter = new StandardFilter();

        FieldFilter fieldFilter = new FieldFilter();
        fieldFilter.enumeration = Arrays.asList("oecd");
        filter.put("dsd.contextSystem", fieldFilter);

        fieldFilter = new FieldFilter();
        fieldFilter.enumeration = Arrays.asList("dataset");
        filter.put("meContent.resourceRepresentationType", fieldFilter);

        //Send request
        Response response = sendRequest(baseUrl+"msd/resources/find", filter, "post", null);
        if (response.getStatus() != 200 && response.getStatus() != 201 && response.getStatus() != 204)
            throw new Exception("Error from D3S requiring existing datasets metadata");

        //Parse response
        return response.getStatus()!=204 ? response.readEntity(new GenericType<Collection<MeIdentification<DSDDataset>>>(){}) : new LinkedList<MeIdentification<DSDDataset>>();
    }

    public void insertMetadata (String baseUrl, Collection<MeIdentification<DSDDataset>> metadataList) throws Exception {
        if (metadataList==null || metadataList.size()==0)
            return;
        //Send request
        Response response = sendRequest(baseUrl+"msd/resources/massive", metadataList, "post", null);
        if (response.getStatus() != 200 && response.getStatus() != 201)
            throw new Exception("Error from D3S adding datasets metadata");
    }

    public void updateMetadata (String baseUrl, Collection<MeIdentification<DSDDataset>> metadataList) throws Exception {
        if (metadataList==null || metadataList.size()==0)
            return;
        //Send request
        Response response = sendRequest(baseUrl+"msd/resources/massive", metadataList, "put", null);
        if (response.getStatus() != 200 && response.getStatus() != 201)
            throw new Exception("Error from D3S adding datasets metadata");
    }

    public void deleteMetadata (String baseUrl, Collection<MeIdentification<DSDDataset>> metadataList) throws Exception {
        if (metadataList==null || metadataList.size()==0)
            return;

        //Create filter
        FieldFilter fieldFilter = new FieldFilter();
        fieldFilter.ids = new LinkedList<>();
        for (MeIdentification<DSDDataset> metadata : metadataList)
            fieldFilter.ids.add(new IdFilter(metadata.getUid(), metadata.getVersion()));
        StandardFilter filter = new StandardFilter();
        filter.put("id", fieldFilter);

        //Send request
        Response response = sendRequest(baseUrl+"msd/resources/massive/delete", filter, "post", null);
        if (response.getStatus() != 200 && response.getStatus() != 201)
            throw new Exception("Error from D3S requiring existing datasets metadata");
    }

    private Response sendRequest(String url, Object entity, String method, Map<String,String> parameters) throws Exception {
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(url);
        return target.request(MediaType.APPLICATION_JSON_TYPE).build(method.trim().toUpperCase(), javax.ws.rs.client.Entity.json(entity)).invoke();
    }

}






/*
    public static void main(String[] args) throws Exception {
        D3SClient client = new D3SClient();
        Collection<MeIdentification<DSDDataset>> metadata = client.retrieveMetadata("http://localhost:7777/v2/");
        System.out.println(metadata.size());
//        client.deleteMetadata("http://localhost:7777/v2/",metadata);
    }
*/
