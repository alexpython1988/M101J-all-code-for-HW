package edu.ufl.alexgre.mongotest.M101J.mongodbjavadrivertest;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;

import edu.ufl.alexgre.mongotest.M101J.Helpers;

public class MongodbIndexTest {

	@Test
	public void test_geoIndex(){
		//check geoJSON
		//key word: near, type, point, polygon, geometry coordinates maxDistance
		//db.stores.find({"loc":{$near:{"geometric":{"type":"point", $coordinates:[39, -130], $maxDistance: 1000000}}}})
	}
	
	@Test
	public void test_unique_sparse(){
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("course");
		MongoCollection<Document> coll = db.getCollection("filterTest");
		
//		coll.createIndex(new BasicDBObject("a", 1), new IndexOptions().unique(true).sparse(true));
//		coll.createIndex(new BasicDBObject("i", -1), new IndexOptions().unique(true).background(true));
		
		//when create index on multiple fields, be careful with order:
		// make sure to create the index based on order equality > sort > range
		
		//coll.insertOne(new Document("x",1).append("y", 100).append("i", 12).append("a", 37));
		
		ArrayList<Document> arr = coll.find().sort(Sorts.descending("i")).projection(Projections.exclude("_id")).into(new ArrayList<Document>());
		
		for(Document doc: arr){
			Helpers.printJson(doc);
		}
		
//		ArrayList<Document> res = coll.find().into(new ArrayList<Document>());
		
//		for(int i = 0; i < res.size(); i++){
//			if(i % 3 == 0){
//				coll.updateOne(Filters.eq("i", i), Updates.set("a", i*3));
//			}
//		}
//		
//		Helpers.printJson(coll.find(Filters.eq("i", 6)).first());

		
		client.close();
	}
	
	@Test
	public void test_queryWitheleMatch(){
		//task: find the records in which the type is exam and its exam score is larger than 99
		//use $eleMatch instead of $and operator
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("school");
		MongoCollection<Document> coll = db.getCollection("students");
		
		Bson filter = Filters.elemMatch("scores", Filters.and(
				Filters.eq("type", "exam"), Filters.gt("score", 99)));
		
		ArrayList<Document> res = coll.find(filter).into(new ArrayList<Document>());
		System.out.println("find: " + res.size());
		
		int j = 0;
		for(Document doc: res){
			if(j < 10){
				Helpers.printJson(doc);
				j++;
			}else{
				break;
			}
		}
		
		client.close();
	}
	
	@Test
	public void test_queryWithIndex(){
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("school");
		MongoCollection<Document> coll = db.getCollection("students");
		
		//since the scores.score have index, the search is fast
		List<Document> res = coll.find(Filters.gte("scores.score", 99)).into(new ArrayList<Document>());
		
		//very slow because it will go through every record
//		MongoCursor<Document> cur = coll.find(Filters.gte("scores.score", 99)).iterator();
//		int i = 0;
//		while(cur.hasNext()){
//			i++;
//		}
		
		int i = res.size();
			
		System.out.println("total: " + coll.count());
		System.out.println("find: " + i);
		
		int j = 0;
		for(Document doc: res){
			if(j < 10){
				Helpers.printJson(doc);
				j++;
			}else{
				break;
			}
		}
		
		client.close();
	}

	@Test
	public void test_createIndex(){
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("school");
		MongoCollection<Document> coll = db.getCollection("students");
		
		coll.drop();
		
		String[] type = {"exam", "quiz", "homework", "homework"};
		
		for(int i = 0; i < 10000; i++){
			ArrayList<Document> scores = new ArrayList<Document>();
			for(int j = 0; j < 4; j++){
				scores.add(new Document("type",type[j]).append("score", Math.random()*100));
			}
			
			coll.insertOne(new Document("student_id", i).append("scores", scores));
		}
		
		//coll.createIndex(new Document("scores.score", 1));
		BasicDBObject index = new BasicDBObject("scores.score", 1);
		coll.createIndex(index);

		Helpers.printJson(coll.find().first());
		
		client.close();
	}
}
