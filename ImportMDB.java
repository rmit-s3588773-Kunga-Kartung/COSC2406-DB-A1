import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.opencsv.CSVReader;

public class ImportMDB {

   // Initialize MongoDB;
   // Create new mongoDB database
   // Create a new collection called business where all documents
   // will be stored.
   public static MongoCollection InitializeMongo() {
      MongoClient mongoClient = new MongoClient("localhost", 27017);
      MongoDatabase DB = mongoClient.getDatabase("MongoDB");
      MongoCollection collection = DB.getCollection("Business");
      return collection;
   }

   // CSVReader reads the .csv file and tokenizes into list array.
   // InsertOneModel docs array used to store batch of document inserts.
   // Each list element corresponds to a field of the document.
   // Once size of docs (count used to represent this) reaches batchSize 
   // bulk write performed.
   // docs then cleared for next batch.
   public static void InsertDocument(MongoCollection collection) throws IOException {
      List<InsertOneModel<Document>> docs = new ArrayList<>();
      Document document;
      @SuppressWarnings("deprecation")
      CSVReader reader = new CSVReader(new FileReader("BUSINESS_NAMES_201803.csv"), '\t');
      String[] list;
      list = reader.readNext();
      int count = 0;
      int batchSize = 1000;

      while ((list = reader.readNext()) != null) {
         docs.add(new InsertOneModel<>(document = new Document("Name", list[1]).append("Status", list[2])
               .append("RegDate", list[3]).append("CancDate", list[4]).append("RenewDate", list[5])
               .append("FormerSNum", list[6]).append("PrevSReg", list[7]).append("ABN", list[8])));
         if (++count % batchSize == 0) {
            collection.bulkWrite(docs, new BulkWriteOptions());
            docs.clear();
         }
      }
      collection.bulkWrite(docs, new BulkWriteOptions());
   }
   
   //Main method
   //Calculates the time taken for the insertion into mongoDB in minutes.
   public static void main(String[] args) throws IOException {
      long startTime = System.currentTimeMillis();

      try {
         MongoCollection collection = InitializeMongo();
         InsertDocument(collection);
      } catch (Exception e) {
         e.printStackTrace();
      }

      long elapsedTime = System.currentTimeMillis() - startTime;
      float totInMin = elapsedTime / (60 * 1000F);
      System.out.println("Finished. Time taken: " + totInMin);
   }

}
