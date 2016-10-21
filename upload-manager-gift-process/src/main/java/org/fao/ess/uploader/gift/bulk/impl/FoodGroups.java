package org.fao.ess.uploader.gift.bulk.impl;

import org.fao.ess.uploader.core.init.UploaderConfig;
import org.fao.ess.uploader.gift.bulk.dto.Queries;
import org.fao.ess.uploader.gift.bulk.utils.D3SClient;
import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.Code;
import org.fao.fenix.commons.msd.dto.full.DSDCodelist;

import javax.inject.Inject;
import javax.ws.rs.NotAcceptableException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;

public class FoodGroups {
    @Inject private UploaderConfig config;
    @Inject D3SClient d3SClient;

    public void fillFoodGroupsTable (Connection connection) throws Exception {
        String d3sBaseURL = config.get("gift.d3s.url");
        d3sBaseURL = d3sBaseURL + (d3sBaseURL.charAt(d3sBaseURL.length() - 1) != '/' ? "/" : "");

        Resource<DSDCodelist, Code> foodexCodelist = d3SClient.getCodelist(d3sBaseURL,"GIFT_Foods", null);
        Resource<DSDCodelist, Code> foodGroupsCodelist = d3SClient.getCodelist(d3sBaseURL,"GIFT_FoodGroups", null);

        fillFoodGroupsTable(connection,foodexCodelist,foodGroupsCodelist);
    }

    private void fillFoodGroupsTable(Connection connection, Resource<DSDCodelist, Code> foodexCodelist, Resource<DSDCodelist, Code> foodGroupsCodelist) throws Exception {
        //Look at food groups codelist
        Set<String> subgroupsCollisions = new HashSet<>();
        Map<String, String> subgroupToGroupMap = new HashMap<>();
        Map<String, String> originalFoodToSubgroupMap = new HashMap<>();
        Map<String, String> foodToSubgroupMap = new HashMap<>();
        StringBuilder alone = new StringBuilder();
        for (Code group : foodGroupsCodelist.getData()) {
            Collection<Code> subGroups = group.getChildren();
            if (subGroups==null || subGroups.size()==0)
                alone.append(',').append(group.getCode());
            else
                for (Code subGroup : subGroups) {
                    Collection<Code> foods = subGroup.getChildren();
                    if (foods==null || foods.size()==0)
                        alone.append(',').append(subGroup.getCode());
                    else {
                        subgroupToGroupMap.put(subGroup.getCode(), group.getCode());
                        for (Code food : subGroup.getChildren())
                            originalFoodToSubgroupMap.put(food.getCode(), subGroup.getCode());
                    }
                }
        }
        //Manage food groups codelist errors
        if (alone.length()>0)
            throw new NotAcceptableException("Food groups codelist have groups or subgroups with no children: "+alone.substring(1));

        //Create food->group correspondence table
        for (Code food : foodexCodelist.getData())
            completeFoodToSubgroupMap(food, null, originalFoodToSubgroupMap, foodToSubgroupMap, subgroupsCollisions);
        if (subgroupsCollisions.size()>0) {
            StringBuilder error = new StringBuilder("There are subgroups assignment collisions:");
            for (String collision : subgroupsCollisions)
                error.append('\n').append(collision);
            throw new Exception(error.toString());
        }

        //Store correspondence table
        PreparedStatement statement = connection.prepareStatement(Queries.insertFoodGroups.getQuery());
        for (Map.Entry<String,String> entry : foodToSubgroupMap.entrySet()) {
            statement.setString(1, subgroupToGroupMap.get(entry.getValue()));
            statement.setString(2, entry.getValue());
            statement.setString(3, entry.getKey());
            statement.addBatch();
        }
        statement.executeBatch();
    }
    private void completeFoodToSubgroupMap(Code food, String currentSubGroup, Map<String, String> originalFoodToSubgroupMap, Map<String, String> foodToSubgroupMap, Set<String> subgroupsCollisions) {
        //Add current code
        String originalSubGroup = originalFoodToSubgroupMap.get(food.getCode());
        if (originalSubGroup!=null)
            currentSubGroup = originalSubGroup;
        if (currentSubGroup!=null) {
            String existingSubgroup = foodToSubgroupMap.put(food.getCode(), currentSubGroup);
            if (existingSubgroup!=null)
                subgroupsCollisions.add(existingSubgroup+ " and "+currentSubGroup+" on code "+food.getCode());
        }
        //Add children
        if (food.getChildren()!=null && (food.getSupplemental()==null || food.getSupplemental().size()!=1 || !food.getSupplemental().values().iterator().next().equalsIgnoreCase("N")))
            for (Code child : food.getChildren())
                completeFoodToSubgroupMap(child,currentSubGroup, originalFoodToSubgroupMap, foodToSubgroupMap, subgroupsCollisions);
    }


}
