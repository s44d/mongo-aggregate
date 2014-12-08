package com.accidentologie;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;


public class AggregateAccidents {

    public static void main(String[] args) {
        MongoClient client = null;
        try {
            client = new MongoClient(new ServerAddress("localhost", 27017));
            DB database = client.getDB("stats");
            final DBCollection collection = database.getCollection("accidents");

            //$project
            DBObject fields = new BasicDBObject("arrondissement", "$fields.com");
            DBObject project = new BasicDBObject("$project", fields );

            //$group
            DBObject arrondissementId = new BasicDBObject("arrondissement","$arrondissement");
            DBObject groupFields = new BasicDBObject( "_id", arrondissementId);
            groupFields.put("count", new BasicDBObject( "$sum", 1));
            DBObject group = new BasicDBObject("$group", groupFields);

            //$sort
            DBObject sort = new BasicDBObject("$sort", new BasicDBObject("count", -1));

            // run aggregation
            List<DBObject> pipeline = Arrays.asList(project,group,sort);
            AggregationOutput output = collection.aggregate(pipeline);

            //display output
            for (DBObject result : output.results()) {
                System.out.println(result);
            }

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
