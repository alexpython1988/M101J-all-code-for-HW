package edu.ufl.alexgre.mongotest.M101J.mongodbjavadrivertest;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

import edu.ufl.alexgre.mongotest.M101J.Helpers;

public class MongodbAggregate {
	
	@Test
	public void test6(){
		MongoClient client = new MongoClient(new MongoClientURI( "mongodb://localhost:27017"));	
		MongoCollection<Document> coll = client.getDatabase("blog").getCollection("posts"); 
		
		List<Bson> pipeline = Arrays.asList(
				Aggregates.unwind("$tags"),
				Aggregates.group("$tags", Accumulators.sum("count", 1)),
				Aggregates.sort(Sorts.descending("count")),
				Aggregates.limit(5),
				Aggregates.project(Projections.fields(
						Projections.include("count"), Projections.exclude("_id"), Projections.computed("tag", "$_id")))
				);
		
		AggregateIterable<Document> res = coll.aggregate(pipeline);
		
//		AggregateIterable<Document> res = coll.aggregate(Arrays.asList(
//				new BasicDBObject("$unwind", "$tags"),
//				new BasicDBObject("$group", new BasicDBObject("_id", "$tags").
//						append("count", new BasicDBObject("$sum",1))),
//				new BasicDBObject("$sort", new BasicDBObject("count", -1)),
//				new BasicDBObject("$limit", 5),
//				new BasicDBObject("$project", new BasicDBObject("count",1).
//						append("tag", "$_id").append("_id", 0))
//				));
//		
		for(Document doc: res)
			Helpers.printJson(doc);
		
		client.close();
	}
	
	@Test
	public void test5(){
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("course");
		MongoCollection<Document> coll = db.getCollection("zips");
		
		//DBObject groupFields = new BasicDBObject("state", "$states").append("city", "$city");
//		List<Bson> pipeline = Arrays.asList(
//				Aggregates.group(groupFields.toString(), Accumulators.sum("population", "$pop")),
//				Aggregates.sort(Sorts.orderBy(Sorts.descending("population"), Sorts.ascending("_id.state"))),
//				Aggregates.group("$_id.state", Arrays.asList(
//						Accumulators.first("population", "$population"), Accumulators.first("city", "$_id.city"))),
//				Aggregates.sort(Sorts.ascending("_id")),
//				Aggregates.project(Projections.fields(Projections.computed("state", "$_id"), 
//						Projections.include("population"), Projections.include("city"), Projections.exclude("_id")))
//				
//		);
//		AggregateIterable<Document> res1 = coll.aggregate(pipeline);
		
		AggregateIterable<Document> res1 = coll.aggregate(Arrays.asList(
				new BasicDBObject("$group", new BasicDBObject("_id", new BasicDBObject("state", "$state").
						append("city", "$city")).append("population", new BasicDBObject("$sum","$pop"))),
				new BasicDBObject("$sort", new BasicDBObject("_id.state",1).append("population", -1)),
				new BasicDBObject("$group", new BasicDBObject("_id", "$_id.state").
						append("city", new BasicDBObject("$first", "$_id.city")).
						append("population", new BasicDBObject("$first", "$population"))),
				new BasicDBObject("$sort", new BasicDBObject("_id",1)),
				new BasicDBObject("$project", new BasicDBObject("state","$_id").append("_id", 0).
						append("population", 1).append("city", 1))
				));
		
		for(Document doc: res1){
			Helpers.printJson(doc);
		}
		
		client.close();
	}
	
	@Test
	public void test4(){
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("course");
		MongoCollection<Document> coll = db.getCollection("zips");
		
		AggregateIterable<Document> res1 = coll.aggregate(Arrays.asList(
				new BasicDBObject("$match", new BasicDBObject("state", "FL")),
				new BasicDBObject("$group", new BasicDBObject("_id", "$city").
						append("population", new BasicDBObject("$sum", "$pop")).
						append("zip_codes", new BasicDBObject("$addToSet", "$_id"))),
				new BasicDBObject("$project", new BasicDBObject("_id",0).
						append("population", 1).
						append("zip_codes", 1).
						append("city", "$_id")),
				new BasicDBObject("$sort", new BasicDBObject("population", -1)),
				new BasicDBObject("$skip", 2),
				new BasicDBObject("$limit", 2)
				));
		
		for(Document doc: res1){
			Helpers.printJson(doc);
		}
		
		client.close();
	}

	@Test
	public void test3(){
		MongoClient client = new MongoClient();
		
		MongoDatabase db = client.getDatabase("course");
		
		MongoCollection<Document> coll = db.getCollection("filterTest");
		
		AggregateIterable<Document> res = coll.aggregate(Arrays.asList(new BasicDBObject("$project",
				new BasicDBObject("_id",0).append("x", 1).append("i", 1).append("mm", "$y")), 
				new BasicDBObject("$group", new BasicDBObject("_id", "i").append("sum", new BasicDBObject("$sum", "$mm")))));
		
		MongoCursor<Document> cur = res.iterator();
		while(cur.hasNext())
			Helpers.printJson(cur.next());
		
		client.close();
	}
	
	@Test
	public void test2(){
		//Helpers.importJsonFile("E:\\CSRelatedCourses\\MongoDB class\\w5\\zips.json", "course", "zips");
		
		MongoClient client = new MongoClient();
		
		MongoDatabase db = client.getDatabase("course");
		
		MongoCollection<Document> coll = db.getCollection("zips");
		
		AggregateIterable<Document> result = coll.aggregate(Arrays.asList(new BasicDBObject("$group",
				new BasicDBObject("_id","$state").append("pop_avg", new BasicDBObject("$avg", "$pop")))));
		
		for(Document doc: result)
			Helpers.printJson(doc);
		
		client.close();
	}
	
	@Test
	public void test1(){
		MongoClient client = new MongoClient();
		
		MongoDatabase db = client.getDatabase("course");
		
		MongoCollection<Document> coll = db.getCollection("filterTest");
		
		
		//ArrayList<Bson> arg = new ArrayList<Bson>();
		//arg.add(new BasicDBObject("$group", new BasicDBObject("_id", "a").append("num", new BasicDBObject("$sum",1))));
		
		AggregateIterable<Document> result = coll.aggregate(Arrays.asList(new Document("$group", 
				new Document("_id", "$a").append("num_a", new Document("$sum", 1)))));
		
		for(Document doc: result){
			Helpers.printJson(doc);
		}
		
//		for(Document doc: coll.find().into(new ArrayList<Document>()))
//			Helpers.printJson(doc);
		
		client.close();
	}
}
