import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class dbquery {

   //This method reads the heap file created and compares it to the
   //text input given by the user. The bytes read and stored in their own
   //byte array as the size of each field is fixed. 
   //From there it is converted to a String and compared to the text inputed.
   //If it is match it prints the Page and Record number and also all of its
   //corresponding values.
   //It continues to loop for the entire heap incase other matching string are 
   //found.
   //Page and Record are able to calculated as the fields are set from dbload.java
   public static void searchFile(String text, int pSize) throws IOException {
      RandomAccessFile raf = new RandomAccessFile("heap." + pSize, "r");

      Path path = Paths.get("heap." + pSize);
      byte[] bytes = Files.readAllBytes(path);
      int count = bytes.length;

      int pageNum = 0;
      int recCount = 0;

      for (int i = 0; i < count; i += 270) {
         raf.seek(i);
         byte[] recordByte = new byte[270];
         raf.read(recordByte);
         byte[] name = Arrays.copyOfRange(recordByte, 0, 200);
         byte[] status = Arrays.copyOfRange(recordByte, 200, 212);
         byte[] dor = Arrays.copyOfRange(recordByte, 212, 222);
         byte[] doc = Arrays.copyOfRange(recordByte, 222, 232);
         byte[] rd = Arrays.copyOfRange(recordByte, 232, 242);
         byte[] fsn = Arrays.copyOfRange(recordByte, 242, 252);
         byte[] psor = Arrays.copyOfRange(recordByte, 252, 255);
         byte[] abn = Arrays.copyOfRange(recordByte, 255, 270);

         String bn_name = new String(name);
         
         if(bn_name.toLowerCase().contains(text.toLowerCase())) {
            System.out.println("record: " + recCount);
            System.out.println("page: " + pageNum);
            System.out.println("Businss Name: " + bn_name);
            System.out.println("Status: " + new String(status));
            System.out.println("Date Of Registration: " + new String(dor));
            System.out.println("Date Of Cancellation: " + new String(doc));
            System.out.println("Renewal Date: " + new String(rd));
            System.out.println("Former State Number: " + new String(fsn));
            System.out.println("Previous State of Registration: " + new String(psor));
            System.out.println("Australian Business Number: " + new String(abn));
            System.out.println();
         }
         else {
            recCount++;
            if(recCount == 15) {
               pageNum++;
               recCount=0;
            }
         }
      }
      raf.close();
   }
   
   //Validates user input so that pagesize is an number otherwise exits application
   public static int CheckValidPageSize(String[] args) throws Exception {
      
      int pos = args.length - 1;
      try {
         int x = Integer.parseInt(args[pos]);
         return x;
      } catch (Exception e) {
         System.out.println("pagesize argument must be a number.");
         System.exit(0);
      }

      return 0;
   }

   //Validates user input so that the text is not null. 
   //It reads the args.length-1 as there will be multiple number of
   //elements in args to make up for the business names. -1 because 
   //the last element will be page size.
   public static String CheckValidString(String[] args) throws Exception {

      if (!args[0].isEmpty()) {
         String[] splitname = new String[args.length-1];
         for (int i = 0; i < args.length - 1; i++) {
            splitname[i] = args[i];
         }
         String fullString = String.join(" ", splitname);
         return fullString;
      } else {
         System.out.println("Please enter something to search.");
         System.exit(0);
      }
      return null;
   }

   //Main method
   //Calculates the time taken to search heap file in ms.
   public static void main(String[] args) {
      long startTime = System.nanoTime();
      try {
         int pageSize = CheckValidPageSize(args);
         String text = CheckValidString(args);
         searchFile(text, pageSize);
      } catch (Exception e) {
         e.printStackTrace();
      }
      long endTime = System.nanoTime();
      long totalTime = (endTime - startTime) / 1000000;
      System.out.println("Completed search in: " + totalTime + "ms");
   }

}
