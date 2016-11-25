package edu.ufl.alexgre.mongotest.M101J.mongodbjavadrivertest;


import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import edu.ufl.alexgre.mongotest.M101J.Helpers;

public class MongodbFindTest {

	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		
		 MongoDatabase db = client.getDatabase("course");
		 
		 MongoCollection<Document> coll = db.getCollection("findTest");
		 
		 coll.drop();
		 
		 for(int i = 0; i < 10; i++){
			 coll.insertOne(new Document("X" + i, i));
		 }
		 
		 //find one
		 System.out.println("fing one");
		 Document first = coll.find().first();
		 Helpers.printJson(first);
		 
		 System.out.println("===========================================================");
		 
		 //find all (return a list)
		 System.out.println("find all");
		 List<Document> all = coll.find().into(new ArrayList<Document>());
		 for(Document doc: all){
			 Helpers.printJson(doc);
		 }
		 
		 System.out.println("===========================================================");
		 
		 //find all iteratably (use try finally to close cursor at the end)
		  MongoCursor<Document> cur = coll.find().iterator();
		  int count = 0;
		  try{
			  while(cur.hasNext()){
				  Helpers.printJson(cur.next());
				  System.out.println(++count);
			  }
		  }finally{
			  cur.close();
		  }
		  
		  System.out.println("===========================================================");
		  
		  System.out.println(coll.count());
		  
		  client.close();
	}
}
