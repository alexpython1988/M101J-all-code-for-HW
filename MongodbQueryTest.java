package edu.ufl.alexgre.mongotest.M101J.mongodbjavadrivertest;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;

import edu.ufl.alexgre.mongotest.M101J.Helpers;

public class MongodbQueryTest {
	
	@Test
	public void testDelete(){
		MongoClient c = new MongoClient();
		MongoDatabase db = c.getDatabase("course");
		MongoCollection<Document> coll = db.getCollection("updateTest");
		
		coll.drop();
		
		for(int i = 0; i < 8; i++)
			coll.insertOne(new Document("_id", (i+1)).append("x", 1));
		
		coll.deleteMany(Filters.and(Filters.gte("_id", 2), Filters.lte("_id", 5)));
		coll.deleteOne(Filters.eq("_id", 7));
		
		//coll.deleteOne(Filters.eq("x", 1));
		//coll.deleteMany(Filters.eq("x", 1));
		
		
		List<Document> doc = coll.find().into(new ArrayList<Document>());
		for(Document each: doc)
			Helpers.printJson(each);
		
		c.close();
	}
	
	@Test
	public void testUpdateandReplace(){
		MongoClient c = new MongoClient();
		MongoDatabase db = c.getDatabase("course");
		MongoCollection<Document> coll = db.getCollection("updateTest");
		
		coll.drop();
		
		for(int i = 0; i < 8; i++){
			coll.insertOne(new Document("_id", (i+1))
										.append("x", i)
										.append("y", true));
		}
		
		//update by replacement of whole record
		coll.replaceOne(Filters.eq("_id", 8), new Document("x", 8).append("y", false));
		
		//update exist records in data base
		//use $set will change the exist fields and add unexist fields
		//if the fields are not mentioned in set, they will not be effected
		coll.updateOne(Filters.eq("_id", 7), Updates.combine(Updates.set("x", 17),
														Updates.set("z", false)));
		
		//in above case, if the query does not find the records, no change will be made
		//if we want to do is if record not exist we want to add this one
		//we can do add Upsert() function and set the value to true
		coll.updateOne(Filters.eq("_id", 9), Updates.combine(Updates.set("x", 100),
											Updates.set("y", false)), 
						new UpdateOptions().upsert(true));
		
		//we can update many records at the same time
		//create a filter with a range to locate all the records
		Bson filter = Filters.and(Filters.gte("_id", 2), Filters.lte("_id", 4));
		coll.updateMany(filter, Updates.inc("x", -10));
		
		List<Document> doc = coll.find().into(new ArrayList<Document>());
		for(Document each: doc)
			Helpers.printJson(each);
		
		c.close();
	}
	
	@Test
	public void testSort(){
		MongoClient c = new MongoClient();
		MongoDatabase db = c.getDatabase("course");
		MongoCollection<Document> coll = db.getCollection("SortTest");
		
		coll.drop();
		
		for(int i = 0; i < 10; i++){
			for(int j = 0; j < 10; j++){
				coll.insertOne(new Document("i",i).append("j", j));
			}
		}
		
		Bson proj = Projections.excludeId();
		//sort
		/*
		 * same as before, we can you document object to perform the same functions
		 * field with 1 is ascending 
		 * field with -1 is descending
		 */
		Bson sort = Sorts.orderBy(Sorts.ascending("i"), Sorts.descending("j"));
		//Bson sort = Sorts.descending("i", "j");
		
		/*
		 * other functions associated with find() including:
		 * skip(num): which will skip the first num of records before start to save records in list
		 * limit(num: which limit the num number of records stored in list 
		 */
		
		List<Document> all = coll.find().projection(proj)
								.sort(sort)
								.skip(10)
								.limit(50)
								.into(new ArrayList<Document>());
		
		for(Document each: all)
			Helpers.printJson(each);
		
		c.close();
	}
	
	@Test
	public void testFilter(){
		MongoClient c = new MongoClient();
		MongoDatabase db = c.getDatabase("course");
		MongoCollection<Document> coll = db.getCollection("filterTest");
		
		db.drop();
		
		
		Random r = new Random();
		for(int i = 0; i < 10; i++){
			coll.insertOne(new Document().append("x", r.nextInt(2))
										.append("y", r.nextInt(100))
										.append("i", i));
			
		}
	
		MongoCursor<Document> cur = coll.find().iterator();
		while(cur.hasNext())
			Helpers.printJson(cur.next());
		
		System.out.println("===================================================");
		
		//create a Bson filter which restrict then results we want to find
		//give the filter to find() method as args as well as for count() method
		//extra filters can be appended to the first document 
		//extra conditions can be appended to the sub-level document
		//this is not the commonly we use
//		Bson filter = new Document("x", 0).append("y", new Document("$gt", 20)
//																.append("$lt", 80));
		
		//better way to create filter is as follow:
		//the function is exactly as above
		Bson filter = Filters.and(Filters.eq("x", 0), Filters.gt("y", 20), 
				                 Filters.lt("y", 80));
		
		/*
		 * since using filter some the data we already know and do not
		 * need to print them again, and fields such as _id is not needed to be projected
		 * we can use projection to hide these fields when we out put the data
		 */
		
		//we can use document object to do projection "field":1 or 0(1 is included 0 is excluded)
		//Bson projection = new Document("x", 0).append("y", 1);
		
		//a better way is to use Projections class
		Bson projection = Projections.fields(Projections.include("y", "i"), 
										Projections.excludeId());
		
		List<Document> list = coll.find(filter).projection(projection)
								.into(new ArrayList<Document>());
		
		for(Document each: list)
			Helpers.printJson(each);
		
		long countAll = coll.count();
		long countFiltered = coll.count(filter);
		System.out.println("all:" + countAll + " x=0:" + countFiltered);
		
		c.close();
	}
}
