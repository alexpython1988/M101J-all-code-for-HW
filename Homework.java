package edu.ufl.alexgre.mongotest.M101J.mongodbjavadrivertest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.print.Doc;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import com.mongodb.util.JSON;

import edu.ufl.alexgre.mongotest.M101J.Helpers;

public class Homework {
	
	@Test
	public void final_7(){
		//import data
		String parentPath = "E:\\CSRelatedCourses\\MongoDB class\\w7\\final7__f7_m101_52e000fde2d423744501d031\\final7";
		File f1 = new File(parentPath, "albums.json");
		File f2 = new File(parentPath, "images.json");
	
		Helpers.importJsonFile(f1.getAbsolutePath(), "course", "albums");
		Helpers.importJsonFile(f2.getAbsolutePath(), "course", "images");
		
		MongoClient c = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
		MongoCollection<Document> coll_al = c.getDatabase("course").getCollection("albums");
		MongoCollection<Document> coll_im = c.getDatabase("course").getCollection("images");
		MongoCollection<Document> coll_tmp = c.getDatabase("course").getCollection("temp");
		
		coll_tmp.drop();
		
		coll_al.createIndex(new BasicDBObject("images", 1));
		
		List<BasicDBObject> pipeline = new ArrayList<BasicDBObject>();
		pipeline.add(new BasicDBObject("$unwind", "$images"));
		pipeline.add(new BasicDBObject("$group", new BasicDBObject("_id", 
				new BasicDBObject("album_id", "$_id").append("img_id", "$images"))));
		AggregateIterable<Document> albumRecords = coll_al.aggregate(pipeline).allowDiskUse(true);	
		
		for(Document eachAlbum: albumRecords){
			coll_tmp.insertOne(eachAlbum);
			System.out.println("insert 1 record.");
		}
		
		coll_tmp.createIndex(new BasicDBObject("_id.img_id", 1));

//		//test
//		if(coll_im.find(Filters.eq("_id", 2)).first() != null){
//			System.out.println(1);
//		}else{
//			System.out.println(2);
//		}
		
//		int count = 0;
		
		coll_im.createIndex(new BasicDBObject("tags", 1));
		
		List<Document> imgs = coll_im.find().into(new ArrayList<Document>());
		
		for(Document eachImg: imgs){
			int id = eachImg.getInteger("_id");
			if(coll_tmp.find(Filters.eq("_id.img_id", id)).first() == null){
				coll_im.deleteOne(Filters.eq("_id", id));
//				System.out.println(id);
//				count++;
			}
		}
		
		System.out.println(coll_im.find(Filters.eq("tags", "sunrises")).into(new ArrayList<Document>()).size());
		
//		System.out.println(count);
		
		coll_tmp.drop();
		
		c.close();
	}

	@Test
	public void final_3(){
		MongoClient c = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
		MongoCollection<Document> coll = c.getDatabase("course").getCollection("emails");
		
		coll.dropIndex(new BasicDBObject("headers.From", "text").append("headers.To", "text"));
		coll.createIndex(new BasicDBObject("headers.Message-ID", "text"));
		
		Bson filter = Filters.eq("headers.Message-ID", "<8147308.1075851042335.JavaMail.evans@thyme>");
		
		Helpers.printJson(coll.find(filter).first());
		
		coll.updateOne(filter, 
				Updates.push("headers.To", "mrpotatohead@mongodb.com"));
		
		Helpers.printJson(coll.find(filter).first());
		
		
//		List<Document> list = coll.find().limit(10).into(new ArrayList<Document>());
//		
//		for(Document doc: list){
//			Helpers.printJson(doc);
//		}
	
		c.close();
	}
	
	@Test
	public void final_2(){
		MongoClient c = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
		MongoCollection<Document> coll = c.getDatabase("course").getCollection("emails");
		
//		List<Document> list = coll.find().limit(10).into(new ArrayList<Document>());
		
//		//create  text index on two fields that we are going to search 
//		coll.createIndex(new BasicDBObject("headers.From", "text").append("headers.To", "text"));
		
		List<BasicDBObject> pipeline = new ArrayList<BasicDBObject>();
		pipeline.add(new BasicDBObject("$unwind", "$headers.To"));
		pipeline.add(new BasicDBObject("$group", new BasicDBObject("_id", 
				new BasicDBObject("_id", "$_id").append("To", "$headers.To").append("From", "$headers.From"))));
		pipeline.add(new BasicDBObject("$group", new BasicDBObject("_id", new BasicDBObject("To", "$_id.To").append("From", "$_id.From")).
				append("times", new BasicDBObject("$sum", 1))));
		pipeline.add(new BasicDBObject("$sort", new BasicDBObject("times", -1)));
		pipeline.add(new BasicDBObject("$limit", 5));
		
		AggregateIterable<Document> res = coll.aggregate(pipeline).allowDiskUse(true);
		
		for(Document doc: res){
			Helpers.printJson(doc);
		}
		
//		for(Document doc: list){
//			Helpers.printJson(doc);
//		}
		
		c.close();
	}
	
	@Test
	public void final_1(){
		MongoClient c = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
		MongoCollection<Document> coll = c.getDatabase("course").getCollection("emails");
		
		//create  text index on two fields that we are going to search 
		coll.createIndex(new BasicDBObject("headers.From", "text").append("headers.To", "text"));
		
//		Bson filter = Filters.and(Filters.eq("headers.From", "andrew.fastow@rnron.com"), 
//				Filters.eq("headers.To", "jeff.skilling@enron.com"));
//		
//		Bson filter1 = Filters.and(Filters.eq("headers.From", "andrew.fastow@rnron.com"), 
//				Filters.eq("headers.To", "john.lavorato@enron.com"));

		List<Document> list = coll.find().into(new ArrayList<Document>());
		
		List<BasicDBObject> pipeline = new ArrayList<BasicDBObject>();
		pipeline.add(new BasicDBObject("$match", new BasicDBObject("headers.From", "andrew.fastow@enron.com")));
		pipeline.add(new BasicDBObject("$unwind", "$headers.To"));
		pipeline.add(new BasicDBObject("$match", new BasicDBObject("headers.To", "jeff.skilling@enron.com")));
		
		AggregateIterable<Document> res = coll.aggregate(pipeline);
		
		for(Document doc: res){
			Helpers.printJson(doc);
		}
		
		c.close();
	}
	
	@Test
	public void hw5_4(){
		MongoClient c = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
		MongoCollection<Document> coll = c.getDatabase("test").getCollection("hw5_4");
		
		//Helpers.printJson(coll.find().first());
		
		List<Bson> pipeline = new ArrayList<Bson>();
		
		//pipeline.add(Aggregates.project(Projections.computed("first", Projections.slice("city", 1))));
		
		pipeline.add(Aggregates.match(Filters.regex("city", "^[0-9]")));
		
		pipeline.add(Aggregates.group("total", Accumulators.sum("population", "$pop")));
		
		AggregateIterable<Document> res = coll.aggregate(pipeline);
		
		for(Document doc: res)
			Helpers.printJson(doc);
		
		c.close();
	}
	
	@Test
	public void hw5_3(){
		MongoClient c = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
		MongoCollection<Document> coll = c.getDatabase("students").getCollection("small_grades");
		
		//Helpers.printJson(coll.find().first());
		
		List<BasicDBObject> pipeline = new ArrayList<BasicDBObject>();
		
		pipeline.add(new BasicDBObject("$unwind", "$scores"));
		
		pipeline.add(new BasicDBObject("$match", new BasicDBObject("scores.type", 
				new BasicDBObject("$in", Arrays.asList("homework", "exam")))));
		
		pipeline.add(new BasicDBObject("$group", new BasicDBObject("_id", new BasicDBObject("sid", "$student_id").
				append("cid", "$class_id")).
				append("tscore", new BasicDBObject("$avg", "$scores.score"))));
		
		pipeline.add(new BasicDBObject("$group", new BasicDBObject("_id", "$_id.cid").
				append("avg_score", new BasicDBObject("$avg", "$tscore"))));
		
		pipeline.add(new BasicDBObject("$sort", new BasicDBObject("avg_score", 1)));
		
		AggregateIterable<Document> res = coll.aggregate(pipeline);
		
		for(Document doc: res)
			Helpers.printJson(doc);
		
		c.close();
	}
	
	@Test
	public void hw5_dataimport(){
		Helpers.importJsonFile("E:\\CSRelatedCourses\\MongoDB class\\w5\\Small_grades_file\\grades.json", "students", "small_grades");
		Helpers.importJsonFile("E:\\CSRelatedCourses\\MongoDB class\\w5\\hw5_4.json", "course", "hw5_4");
	}
	
	@Test
	public void hw5_2(){
		//Helpers.importJsonFile("E:\\CSRelatedCourses\\MongoDB class\\w5\\small_zips.json", "course", "small_zips");
		
		MongoClient c = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
		MongoCollection<Document> coll = c.getDatabase("course").getCollection("small_zips");
		
		//Helpers.printJson(coll.find().first());
		
		List<Bson> pipeline = new ArrayList<Bson>();
		pipeline.add(Aggregates.match(Filters.or(Filters.eq("state", "CA"), Filters.eq("state", "NY"))));
		
		pipeline.add(Aggregates.group("$city", Accumulators.sum("total", "$pop")));
		
		pipeline.add(Aggregates.match(Filters.gte("total", 25000)));
		
		pipeline.add(Aggregates.group("avg", Accumulators.avg("result", "$total")));	
		
		
		AggregateIterable<Document> res = coll.aggregate(pipeline);
		
		for(Document doc: res)
			Helpers.printJson(doc);
		
		c.close();
	}
	
	@Test
	public void hw5_1(){
		//Helpers.importJsonFile("C:\\Users\\xiyang\\Desktop\\1.json", "blog", "posts");
		
		MongoClient c = new MongoClient();
		MongoDatabase db = c.getDatabase("blog");
		MongoCollection<Document> coll = db.getCollection("posts");
		
		//List<BasicDBObject> pipeline = new ArrayList<BasicDBObject>();
		List<Bson> pipeline = new ArrayList<Bson>();
		
		pipeline.add(Aggregates.unwind("$comments"));
		
		pipeline.add(Aggregates.group("$comments.author", Accumulators.sum("comment_num", 1)));
		
		pipeline.add(Aggregates.sort(Sorts.descending("comment_num")));
		
		pipeline.add(Aggregates.limit(10));
		
		pipeline.add(Aggregates.project(Projections.fields(
				Projections.exclude("_id"),
				Projections.computed("name", "$_id"),
				Projections.include("comment_num"))
		));
		
		AggregateIterable<Document> res = coll.aggregate(pipeline);
		
		for(Document doc: res)
			Helpers.printJson(doc);
		
		c.close();
	}
	
	@Test
	public void hw4_3createIndex(){
		MongoClient c = new MongoClient();
		MongoDatabase db = c.getDatabase("blog");
		MongoCollection<Document> coll = db.getCollection("posts");
		
		BasicDBObject index1 = new BasicDBObject("permalink", 1);
		coll.createIndex(index1);
		BasicDBObject index2 = new BasicDBObject("date", -1);
		coll.createIndex(index2);
		BasicDBObject index3 = new BasicDBObject("tags", 1).append("date", 1);
		coll.createIndex(index3);
			
		c.close();
	}
	
	@Test
	public void hw4_3import(){
		MongoClient c = new MongoClient();
		MongoDatabase db = c.getDatabase("blog");
		MongoCollection<Document> coll = db.getCollection("posts");
		
		coll.drop();
		
		long t1 = System.nanoTime();
		
		File f = new File("E:\\CSRelatedCourses\\MongoDB class\\w4\\m101j-blog-indexes\\m101j-blog-indexes\\posts.json");
		
		BufferedReader br = null;
		ArrayList<Document> al = new ArrayList<Document>();
		try{
			br = new BufferedReader(new FileReader(f));
			String line;
			while((line = br.readLine()) != null){
				 Document doc = Document.parse(line);
				al.add(doc);
			}
			
			coll.insertMany(al);
		}catch(Exception e){
			
		}finally{
			if(br != null){
				try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			long t2 = System.nanoTime();
			System.out.println((t2-t1)/1000000000.0); //second
			
			c.close();
		}
	}
	
	@Test
	public void hw3_1(){
		MongoClient c = new MongoClient();
		MongoDatabase db = c.getDatabase("school");
		MongoCollection<Document> coll = db.getCollection("students");
		
		List<Document> all = coll.find().into(new ArrayList<Document>());
		
		for(Document doc: all){
			double low = 101;
			int id = doc.getInteger("_id");
			
			Object obj = doc.get("scores");
			ArrayList<Document> al = (ArrayList<Document>) obj;
			
			for(Document each: al){
				if(each.get("type").equals("homework")){
					double d = each.getDouble("score");
					if(d < low)
						//System.out.println(d);
						low = d;
				}
			}
			
			//System.out.println(low);
			
			//Bson filter = Filters.and(Filters.eq("scores.score", low), Filters.eq("type", "homework"));
			
			Bson filter = Filters.and(Filters.eq("scores", 
					Filters.and(Filters.eq("type", "homework"), Filters.eq("score", low))));
			coll.updateOne(Filters.eq("_id", id), Updates.pullByFilter(filter));
		}
		
		c.close();
	}
	
	@Test
	public void test1(){
		MongoClient c = new MongoClient();
		MongoDatabase db = c.getDatabase("course");
		MongoCollection<Document> coll = db.getCollection("filterTest");
		
		
		coll.updateOne(Filters.eq("i", 7), Updates.push("z", new Document("1", 1).append("2", 2)));
		coll.updateOne(Filters.eq("i", 7), Updates.push("z", new Document("3", 3).append("4", 4)));
		
//		Object d = coll.find(Filters.eq("i", 4)).first().get("z");
//		System.out.println(d.toString());
//		if(coll.find(Filters.eq("i", 5)).first().get("z") == null)
//			coll.updateOne(Filters.eq("i", 5), Updates.set("z", new Document("num", 1)));
//		
//		
//		ArrayList<Document> al = new ArrayList<Document>();
//		al.add(new Document("num", 1));
//		if(coll.find(Filters.eq("i", 6)).first().get("z") == null)
//			coll.updateOne(Filters.eq("i", 6), Updates.set("z", al));
		
//		coll.updateOne(Filters.eq("i", 5), Updates.push("z", new Document("num1", 2)));  //cannot use push in this case
//		coll.updateOne(Filters.eq("i", 6), Updates.push("z", new Document("num1", 2)));
		
		//Document doc = new Document("aaa", 3).append("bbb", 4);
		
		//ArrayList<Document> z = new ArrayList<Document>();
		
//		Document z = new Document("num", 1);
//		Document x = new Document("num1", 2);
		
//		Document doc = coll.find(Filters.eq("i", 4)).first();
//		
//		doc.append("z", z);
		
//		coll.updateOne(Filters.eq("i", 4), Updates.push("z", z));
//		coll.updateOne(Filters.eq("i", 4), Updates.push("z", x));
		//coll.updateOne(Filters.eq("i", 4), Updates.push("z", doc));
		
//		ArrayList<Document> al = (ArrayList<Document>) coll.find(Filters.eq("i", 4)).first().get("z");
//		Document x = new Document("num2", 3);
//		al.add(x);
//		
//		coll.updateOne(Filters.eq("i", 4), Updates.set("z", al));
//		
		List<Document> all = coll.find().into(new ArrayList<Document>());
		
		//double low = 49.43132782777443;
		//Bson filter = Filters.eq("_id", 201);
//		Bson filter = Filters.and(Filters.eq("scores", 
//				Filters.and(Filters.eq("type", "homework"), Filters.eq("score", low))));
		
		//Document doc = coll.find(filter).first();
		
//		Document doc = coll.find().first();
//		
//		System.out.println(doc.get("scores").toString());
//		
//		Object obj = doc.get("scores");
//		
//		ArrayList<Document> al = (ArrayList<Document>) obj;
//		
//		for(Document each: al){
//			if(each.get("type").equals("homework"))
//				System.out.println(each.getDouble("score"));
//		}
//		
//		System.out.println(al.toString());
		for(Document each: all)
			Helpers.printJson(each);
		c.close();;
	}
	
	@Test
	public void hw2_3(){
		MongoClient c = new MongoClient();
		MongoDatabase db = c.getDatabase("students");
		MongoCollection<Document> coll = db.getCollection("grades");
		
		Bson sort = Sorts.orderBy(Sorts.ascending("student_id"), Sorts.ascending("score"));
		
		List<Document> all = coll.find().sort(sort).into(new ArrayList<Document>());
		
		int id = -1;
		
		for(Document each: all){
			String type = (String) each.get("type");
			int sid = (Integer) each.get("student_id");
			if(type.equals("homework")){
				if(sid != id){
					id = sid;
					coll.deleteOne(each);
				}else{
					continue;
				}
			}else{
				continue;
			}
		}
		
		Helpers.printJson(coll.find().first());
		System.out.println(coll.count());
		
		c.close();
	}
	
	@Test
	public void hw2_2(){
		MongoClient c = new MongoClient();
		MongoDatabase db = c.getDatabase("students");
		MongoCollection<Document> coll = db.getCollection("grades");
		
		Bson filter = Filters.and(Filters.gte("score", 65), Filters.eq("type", "exam"));
		Bson sort = Sorts.ascending("score");
		
		List<Document> all = coll.find(filter).sort(sort).into(new ArrayList<Document>());
		
		for(Document doc: all)
			Helpers.printJson(doc);
		
		c.close();
	}
	
	@Test
	public void hw2_5$6(){
		MongoClient c = new MongoClient();
		MongoDatabase db = c.getDatabase("video");
		MongoCollection<Document> coll = db.getCollection("movieDetails");
		
		Bson filter1 = Filters.and(Filters.eq("year", 2013), Filters.eq("rated", "PG-13"),
								Filters.eq("awards.wins", 0));
		
		Bson proj = Projections.include("title");
		
		List<Document> all = coll.find(filter1).projection(proj).into(new ArrayList<Document>());
		
		for(Document doc: all)
			Helpers.printJson(doc);
		
		Bson filter2 = Filters.eq("countries.1", "Sweden");
		
		ArrayList<Document> al = coll.find(filter2).into(new ArrayList<Document>());
		System.out.println(al.size());
		
		c.close();
	}
}
