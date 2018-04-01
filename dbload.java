import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

import com.opencsv.CSVReader;

//Created to store an arraylist of arraylist of records.
//Each page holds an x amount of records.
//This is to reinact how pages work.
class Page {
   ArrayList<ArrayList<Record>> page = new ArrayList<ArrayList<Record>>();
}

//Record object created for each line read on the .csv file.
class Record {
   String b_name;
   String status;
   String dateOfReg;
   String dateOfCancel;
   String renDate;
   String formStateNum;
   String prevStateOfReg;
   String ABN;
}

public class dbload {

   //InitaliseOutputStream. File created is heap.(pagesize).
   public static DataOutputStream InitialiseOutputStream(int pageSize) throws FileNotFoundException {
      DataOutputStream os = new DataOutputStream(new FileOutputStream("heap." + pageSize));
      return os;
   }

   //This method writes to the heap file.
   //CSVReader reads the .csv file and splits into values array.
   //New page and record objects initialized.
   //Strings converted to bytes and written to file through DataOutputStream
   //Length of each field set.
   //Values are used to create new record.
   //Total size of all the fields is set as the record size.
   //Records are stored in a page until another record cannot fit into the page.
   //Then a new page is created and process continued until no records left to add.
   public static void WriteHeapFile(DataOutputStream os, File file, int pSize) throws IOException {
      CSVReader reader = new CSVReader(new FileReader(file), '\t');
      Page pg = new Page();
      Record rec = null;
      String values[];
      
      int pageSize = pSize;
      int sizeCount = 0;
      int recCount = 0;
      int pageNum = 0;
      values = reader.readNext();

      while ((values = reader.readNext()) != null) {
         int nB = 0;
         int sB = 0;
         int dorB = 0;
         int docB = 0;
         int rdB = 0;
         int fsnB = 0;
         int psorB = 0;
         int abnB = 0;

         //The following convert all the field into bytes before printing to heap
         if (byteSize(values[1]) <= 200) {
            byte[] src = values[1].getBytes("UTF-8");
            byte[] tempname = Arrays.copyOf(src, 200);
            os.write(tempname);
            nB = tempname.length;
         } else {
            byte[] src = values[1].getBytes("UTF-8");
            os.write(src);
            nB = src.length;
         }

         if (byteSize(values[2]) <= 12) {
            byte[] src = values[2].getBytes("UTF-8");
            byte[] tempstatus = Arrays.copyOf(src, 12);
            os.write(tempstatus);
            sB = tempstatus.length;
         } else {
            byte[] src = values[2].getBytes("UTF-8");
            os.write(src);
            sB = src.length;
         }

         if (byteSize(values[3]) <= 10) {
            byte[] src = values[3].getBytes("UTF-8");
            byte[] tempDOR = Arrays.copyOf(src, 10);
            os.write(tempDOR);
            dorB = tempDOR.length;
         } else {
            byte[] src = values[3].getBytes("UTF-8");
            os.write(src);
            dorB = src.length;
         }

         if (byteSize(values[4]) <= 10) {
            byte[] src = values[4].getBytes("UTF-8");
            byte[] tempDOC = Arrays.copyOf(src, 10);
            os.write(tempDOC);
            docB = tempDOC.length;
         } else {
            byte[] src = values[4].getBytes("UTF-8");
            os.write(src);
            docB = src.length;
         }

         if (byteSize(values[5]) <= 10) {
            byte[] src = values[5].getBytes("UTF-8");
            byte[] tempRD = Arrays.copyOf(src, 10);
            os.write(tempRD);
            rdB = tempRD.length;
         } else {
            byte[] src = values[5].getBytes("UTF-8");
            os.write(src);
            rdB = src.length;
         }

         if (byteSize(values[6]) <= 10) {
            byte[] src = values[6].getBytes("UTF-8");
            byte[] tempFSN = Arrays.copyOf(src, 10);
            os.write(tempFSN);
            fsnB = tempFSN.length;
         } else {
            byte[] src = values[6].getBytes("UTF-8");
            os.write(src);
            fsnB = src.length;
         }

         if (byteSize(values[7]) <= 3) {
            byte[] src = values[7].getBytes("UTF-8");
            byte[] tempPSOR = Arrays.copyOf(src, 3);
            os.write(tempPSOR);
            fsnB = tempPSOR.length;
         } else {
            byte[] src = values[7].getBytes("UTF-8");
            os.write(src);
            psorB = src.length;
         }

         if (byteSize(values[8]) <= 15) {
            byte[] src = values[8].getBytes("UTF-8");
            byte[] tempABN = Arrays.copyOf(src, 15);
            os.write(tempABN);
            abnB = tempABN.length;
         } else {
            byte[] src = values[8].getBytes("UTF-8");
            os.write(src);
            abnB = src.length;
         }

         //The following creates a record object and adds the corresponding values.
         rec = new Record();
         rec.b_name = values[1];
         rec.status = values[2];
         rec.dateOfReg = values[3];
         rec.dateOfCancel = values[4];
         rec.renDate = values[5];
         rec.formStateNum = values[6];
         rec.prevStateOfReg = values[7];
         rec.ABN = values[8];

         //Total size of record.
         int recSize = nB + sB + dorB + docB + rdB + fsnB + psorB + abnB;

         //This checks that record fits into the page and if not
         //create a new page.
         if ((recSize + sizeCount) < pageSize) {
            sizeCount = sizeCount + recSize;
            recCount++;
            pg.page.add(new ArrayList<Record>());
            pg.page.get(pageNum).add(rec);
         } else {
            pageNum++;
            pg.page.add(new ArrayList<Record>());
            pg.page.get(pageNum).add(rec);
            sizeCount = recSize;
         }
      }
      
      //Print out the total number of pages and the total number of records.
      System.out.println("Total number of pages used: " + pg.page.size());
      int count=0;
      for(int i=0; i<pg.page.size(); i++) {
         for(int j=0; j<pg.page.get(i).size(); j++) {
            count++;
         }
      }
      System.out.println("Total number of records loaded: " + count);
   }

   //Called to convert the strings to bytes
   public static int byteSize(String string) throws UnsupportedEncodingException {
      final byte[] x = string.getBytes("UTF-8");
      int y = x.length;
      return y;
   }

   //This checks to make sure that user inputs a valid number for page size.
   public static int CheckValidPageSize(String[] args) throws Exception {

      int pos = args.length - 2;
      try {
         int x = Integer.parseInt(args[pos]);
         return x;
      } catch (Exception e) {
         System.out.println("pagesize argument must be a number.");
         System.exit(0);
      }
      return 0;
   }

   //This checks to make sure that the file name user inputs is a valid file.
   public static File CheckValidFile(String[] args) throws Exception {

      File f = new File(args[2]);
      if (!args[0].equals("-p") || args[1].isEmpty() || !f.exists()) {
         System.out.print("Invalid arguments or file may not exist. Please check and try again.");
         System.exit(0);
      }
      return f;
   }

   //Main method
   //Calculates the time taken to create heap file in ms.
   public static void main(String[] args) {
      long startTime = System.nanoTime();
      try {
         int pageSize = CheckValidPageSize(args);
         File file = CheckValidFile(args);
         DataOutputStream os = InitialiseOutputStream(pageSize);
         WriteHeapFile(os, file, pageSize);
      } catch (Exception e) {
         e.printStackTrace();
      }
      long endTime = System.nanoTime();
      long totalTime = (endTime - startTime) / 1000000;
      System.out.println("Completed creation of heap file in: " + totalTime + "ms");
   }
}
