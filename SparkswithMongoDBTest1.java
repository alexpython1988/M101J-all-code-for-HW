package edu.ufl.alexgre.mongotest.M101J.mongodbjavadrivertest;

import java.io.File;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import freemarker.template.Configuration;
import freemarker.template.Template;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;

public class SparkswithMongoDBTest1 {

	public static void main(String[] args) {
		Spark.port(10001);
		
		//freeMaker setup
		final Configuration config = new Configuration();
		
		//Mongodb set up
		final MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("course");
		final MongoCollection<Document> coll = db.getCollection("hello");
		//only for practice prospers
		coll.drop();
		
		coll.insertOne(new Document("name","gainesville").append("person", "da xi xi"));
		
		Route route_hello = new Route() {
			public Object handle(Request arg0, Response arg1){
				System.out.println("connecting to server...");
				StringWriter sw = new StringWriter();
				MongoCursor<Document> cur = null;
				try {
					config.setDirectoryForTemplateLoading(new File("static"));
					Template hellow = config.getTemplate("hellow.ftl");
					
					System.out.println("loading webpage...");
					Map<String,Object> map = new HashMap<String, Object>();
					
					cur = coll.find().iterator();
					while(cur.hasNext()){
						Document doc = cur.next();
						for(Iterator<Entry<String, Object>> itr =  doc.entrySet().iterator();itr.hasNext();){
							Entry<String, Object>  en = itr.next();
							map.put(en.getKey(), en.getValue());
						}
					}
					
					hellow.process(map, sw);
					
				} catch (Exception e) {
					Spark.halt(500);
					e.printStackTrace();
				} finally{
					cur.close();
					client.close();
				}
				
				return sw;
			}
		};
		
		Spark.get("/", route_hello);
	}

}
