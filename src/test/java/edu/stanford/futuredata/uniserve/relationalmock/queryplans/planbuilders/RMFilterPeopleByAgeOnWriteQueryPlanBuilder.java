package edu.stanford.futuredata.uniserve.relationalmock.queryplans.planbuilders;

import edu.stanford.futuredata.uniserve.relationalmock.queryplans.RMFilterPeopleByAgeOnWriteQueryPlan;

public class RMFilterPeopleByAgeOnWriteQueryPlanBuilder {
    private int minAge = 0;
    private int maxAge = 9999;

    public int getMaxAge() {
        return maxAge;
    }

    public int getMinAge() {
        return minAge;
    }

    public RMFilterPeopleByAgeOnWriteQueryPlanBuilder setMaxAge(Integer maxAge){
        if(maxAge != null){
            this.maxAge = maxAge;
        }
        return this;
    }
    public RMFilterPeopleByAgeOnWriteQueryPlanBuilder setMinAge(Integer minAge){
        if (minAge != null){
            this.minAge = minAge;
        }
        return this;
    }


    public RMFilterPeopleByAgeOnWriteQueryPlan build(){
        return new RMFilterPeopleByAgeOnWriteQueryPlan(this);
    }
}
