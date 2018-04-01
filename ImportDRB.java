import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.*;
import com.opencsv.CSVReader;

public class ImportDRB {

   private static final String FILE = "BUSINESS_NAMES_201803.csv";
   private static final String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
   private static final String DBURL = "jdbc:derby:myDB;create=true";

   //This method called to Create table is it already doesn't exists.
   //If table does exist it will drop the table then create a new table,
   //Otherwise it will proceed to create a new table.
   //Primary key set as ID which increments by 1
   public static void CreateTable(Connection conn) throws SQLException {
      String dropTable = "DROP TABLE BUSINESS";
      String createTable = "CREATE TABLE BUSINESS (ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), BUSINESS_NAME VARCHAR(200), "
            + "STATUS VARCHAR(12), DATE_OF_REG DATE, DATE_OF_CAN DATE, RENEWAL_DATE DATE, FORMER_STATE_NUM VARCHAR(10), PREV_STATE_OF_REG VARCHAR(3), ABN VARCHAR(20))";
      try {
         PreparedStatement drop = conn.prepareStatement(dropTable);
         drop.execute();
      }
      catch (Exception e) {
         System.out.println("Table does not exist. Continuing creating table.");
      }
      PreparedStatement create = conn.prepareStatement(createTable);
      create.execute();
   }
   
   //The main function that creates PreparedStatements to execute.
   //CSVReader reads the .csv file and tokenises the read line to list array.
   //From the array we replace one single quote with two single quotes so it takes
   //the single quote as a character as some business names contain a single quote.
   //It then reformats the date to yyyy-MM-dd Date array as this is what format derby accepts. 
   //list[] values set to corresponding field in BUSINESS table in the PreparedStatments.
   //PreparedStatements stored in batch until batch size is batchSize.
   //Once batch size is batchSize then batch statments excecuted.
   public static void InsertRow(Connection conn) throws SQLException, IOException {
      String insertRow = "INSERT INTO BUSINESS (BUSINESS_NAME, STATUS, DATE_OF_REG, DATE_OF_CAN, RENEWAL_DATE, FORMER_STATE_NUM, "
            + "PREV_STATE_OF_REG, ABN) VALUES (?,?,?,?,?,?,?,?)";
      final int batchSize = 10000;
      int count = 0;
      conn.setAutoCommit(false);
      @SuppressWarnings({ "deprecation", "resource" })
      CSVReader reader = new CSVReader(new FileReader(FILE), '\t');
      String[] list;
      Date[] dates = new Date[6];
     
      PreparedStatement insert = conn.prepareStatement(insertRow);
      list = reader.readNext();
      
      while ((list = reader.readNext()) != null) {
         //Replace all ' with two ' i.e '' 
         list[1] = list[1].replace("\'", "\'\'");

         //Format date variable to yyyy-MM-dd
         for (int d = 3; d <= 5; d++) {
            if (!list[d].isEmpty()) {
               Pattern pattern = Pattern.compile("(\\d{4})/(\\d{2})/(\\d{2}).+");
               Matcher matcher = pattern.matcher(list[d]);
               if (matcher.matches()) {
                  SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd");
                  Date tmp = new Date();
                  try {
                     tmp = df.parse(list[d]);
                  } catch (ParseException e) {
                     e.printStackTrace();
                  }
                  list[d] = df.format(tmp).replace("/", "-");
                  dates[d] = java.sql.Date.valueOf(list[d]);
               } else {
                  Date initDate = new Date();
                  try {
                     initDate = new SimpleDateFormat("dd/MM/yyyy").parse(list[d]);
                  } catch (Exception e) {
                     e.printStackTrace();
                  }
                  SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
                  list[d] = formatter.format(initDate).replace("/", "-");
                  dates[d] = java.sql.Date.valueOf(list[d]);
               }
            } else
               continue;
         }
         
         //Insert into preparedSatement.
         insert.setString(1, list[1]);
         insert.setString(2, list[2]);
         if (dates[3] == null) {
            insert.setNull(3, Types.DATE);
         } else {
            insert.setDate(3, new java.sql.Date(dates[3].getTime()));
         }
         if (dates[4] == null) {
            insert.setNull(4, Types.DATE);
         } else {
            insert.setDate(4, new java.sql.Date(dates[4].getTime()));
         }
         if (dates[5] == null) {
            insert.setNull(5, Types.DATE);
         } else {
            insert.setDate(5, new java.sql.Date(dates[5].getTime()));
         }
         insert.setString(6, list[6]);
         insert.setString(7, list[7]);
         insert.setString(8, list[8]);
         //Stored preparedStatement in batch
         insert.addBatch();
         
         //Once batch size reaches batchSize then execute the batch.
         if (++count % batchSize == 0) {
            insert.executeBatch();
         }
      }
      //Execute the remaining batches that do not make up to size batchSize.
      insert.executeBatch();

      insert.close();
      conn.commit();
      conn.close();
   }

   //Main method.
   //Calculates the time taken for the insertion into derbyDB in minutes.
   public static void main(String[] args) {
      long startTime = System.currentTimeMillis();

      try {
         Class.forName(DRIVER);
         Connection conn = DriverManager.getConnection(DBURL);
         CreateTable(conn);
         InsertRow(conn);
      }
      catch (Exception e){
         e.printStackTrace();
      }
      
      long elapsedTime = System.currentTimeMillis() - startTime;
      float totInMin = elapsedTime / (60 * 1000F);
      System.out.println("Finished. Time taken: " + totInMin);
   }
}
