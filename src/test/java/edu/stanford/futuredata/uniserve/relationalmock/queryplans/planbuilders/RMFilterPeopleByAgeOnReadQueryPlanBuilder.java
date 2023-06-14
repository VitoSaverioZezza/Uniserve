package edu.stanford.futuredata.uniserve.relationalmock.queryplans.planbuilders;

import edu.stanford.futuredata.uniserve.relationalmock.queryplans.RMFilterPeopleByAgeOnReadQueryPlan;
import edu.stanford.futuredata.uniserve.relationalmock.queryplans.RMFilterPeopleByAgeOnWriteQueryPlan;

public class RMFilterPeopleByAgeOnReadQueryPlanBuilder{
    int maxAge = 999;
    int minAge = 0;
    String tableName = "People";

    public RMFilterPeopleByAgeOnReadQueryPlanBuilder setTableName(String tableName){
        this.tableName = tableName;
        return this;
    }

    public RMFilterPeopleByAgeOnReadQueryPlanBuilder setMaxAge(Integer maxAge){
        if(maxAge != null){
            this.maxAge = maxAge;
        }
        return this;
    }
    public RMFilterPeopleByAgeOnReadQueryPlanBuilder setMinAge(Integer minAge){
        if (minAge != null){
            this.minAge = minAge;
        }
        return this;
    }

    public int getMinAge() {
        return minAge;
    }

    public int getMaxAge() {
        return maxAge;
    }

    public String getTableName() {
        return tableName;
    }

    public RMFilterPeopleByAgeOnReadQueryPlan build(){
        return new RMFilterPeopleByAgeOnReadQueryPlan(this);
    }
}
