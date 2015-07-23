package org.fao.ess.uploader.rlm.processor;

import org.fao.ess.uploader.core.init.UploaderConfig;
import org.fao.fenix.commons.find.dto.filter.FieldFilter;
import org.fao.fenix.commons.find.dto.filter.StandardFilter;
import org.fao.fenix.commons.msd.dto.data.ReplicationFilter;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.msd.dto.full.MeMaintenance;
import org.fao.fenix.commons.msd.dto.full.SeUpdate;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.swing.text.html.parser.Entity;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Date;

@ApplicationScoped
public class D3SClient {

    private String url;

    @Inject
    UploaderConfig config;


    public void sendDataUpdatedSignal() throws Exception {
        //Init
        if (url == null) {
            url = config.get("d3s.url");
            url = url + (url.charAt(url.length() - 1) != '/' ? "/" : "") + "msd/resources/replication";
        }

        //Create resources filter
        StandardFilter filter = new StandardFilter();

        FieldFilter fieldFilter = new FieldFilter();
        fieldFilter.enumeration = Arrays.asList("RLM");
        filter.put("dsd.contextSystem", fieldFilter);

        fieldFilter = new FieldFilter();
        fieldFilter.enumeration = Arrays.asList("dataset");
        filter.put("meContent.resourceRepresentationType", fieldFilter);

        //Create metadata update content
        SeUpdate seUpdate = new SeUpdate();
        seUpdate.setUpdateDate(new Date());
        MeMaintenance meMaintenance = new MeMaintenance();
        meMaintenance.setSeUpdate(seUpdate);
        MeIdentification<DSDDataset> meIdentification = new MeIdentification<>();
        meIdentification.setMeMaintenance(meMaintenance);

        //Create replication filter
        ReplicationFilter<DSDDataset> replicationFilter = new ReplicationFilter();
        replicationFilter.setFilter(filter);
        replicationFilter.setMetadata(meIdentification);

        //Send REST request
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(url);
        Response response = target.request(MediaType.APPLICATION_JSON_TYPE).build("PATCH", javax.ws.rs.client.Entity.json(replicationFilter)).invoke();
        if (response.getStatus() != 200)
            throw new Exception("Error from D3S during datasets metadata update");
    }
}