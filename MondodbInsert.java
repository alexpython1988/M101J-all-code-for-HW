package edu.ufl.alexgre.mongotest.M101J.mongodbjavadrivertest;

import java.util.Arrays;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import edu.ufl.alexgre.mongotest.M101J.Helpers;

public class MondodbInsert {

	/*
	common way to create a java diver, mongodb client and use document to exchange data:
	
	//set up mongodb java driver
	
	//ways to create single mongo client
	//MongoClient c1 = new MongoClient(new ServerAddress("localhost", 10000));
	//MongoClient c1 = new MongoClient(new MongoClientURI("mongodb://localhost:10000"));
 	//MongoClient client = new MongoClient("localhost", 10000);
    
    MongoClientOptions options = new MongoClientOptions.Builder().connectionsPerHost(100).build();
    MongoClient client = new MongoClient(new ServerAddress(), options);
    
    MongoDatabase db = client.getDatabase("test");
    
    //document is bson document
    //MongoCollection<Document> coll = db.getCollection("test");
    MongoCollection<BsonDocument> coll = db.getCollection("test", BsonDocument.class);
    
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("str", "Mongodb");
    
    Document doc = new Document(map);
    
    doc.append("int", 42);
    doc.append("l", 1L);
    doc.append("date", new Date());
    doc.append("objectId", new ObjectId());
    doc.append("b", true);
    doc.append("list", Arrays.asList(1,2,3));
    
    String str = doc.getString("str");
    System.out.println(str);
    
    Helpers.printJson(doc);
    
    //we can also use BsonDocument to replace the document to make sure the data we put or querry is Bson supported
    // type safe concer
    */
    
	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		
		MongoDatabase db = client.getDatabase("course");
		
		MongoCollection<Document> coll = db.getCollection("insertTest");
		//for practice prospers
		coll.drop();
		
		Document smith = new Document().append("name", "smith")
									.append("age", 38)
									.append("job", "programmer");
		
		Document jones = new Document().append("name", "jones")
									.append("age", 30)
									.append("job", "hecker");
		
		Document john = new Document().append("name", "john")
									.append("age", 28)
									.append("job", "UI designer");
		
		//diver will create _id auto
		//insert one document each time
		coll.insertOne(smith);
		Helpers.printJson(jones);
		
		//insert many document at same time
		coll.insertMany(Arrays.asList(jones, john));
						
		Helpers.printJson(jones);
		jones.remove("_id");
		coll.insertOne(jones);
		
		//close client
		client.close();
	}

}
