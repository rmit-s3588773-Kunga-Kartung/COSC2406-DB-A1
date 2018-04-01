package app;

import java.sql.*;

public class queryDB {

   public static void main(String[] args) {
     
      
      String query1 = "SELECT COUNT (BUSINESS_NAME), STATUS"
            + "FROM BUSINESS"
            + "GROUP BY STATUS";

      String query2 = "COUNT(*) FROM BUSINESS";
      
      String query3 = "SELECT * FROM BUSINESS WHERE BUSINESS_NAME LIKE '%CHIPS'";
      
      try {
         Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
         Connection conn = DriverManager.getConnection("jdbc:derby:myDB");
         PreparedStatement query1I = conn.prepareStatement(query1);
         PreparedStatement query2I = conn.prepareStatement(query2);
         PreparedStatement query3I = conn.prepareStatement(query3);
         
         long startTime = System.currentTimeMillis();
         query1I.execute();
         long elapsedTime = System.currentTimeMillis() - startTime;
         System.out.println("Finished. Time taken: " + elapsedTime);
         
         long startTime2 = System.currentTimeMillis();
         query2I.execute();
         long elapsedTime2 = System.currentTimeMillis() - startTime2;
         System.out.println("Finished. Time taken: " + elapsedTime2);
         
         long startTime3 = System.currentTimeMillis();
         query3I.execute();
         long elapsedTime3 = System.currentTimeMillis() - startTime3;
         System.out.println("Finished. Time taken: " + elapsedTime3);
         
      } catch (Exception e) {
         e.printStackTrace();
      }
     
      
   }

}
