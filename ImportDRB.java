package app;

import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.*;

import com.opencsv.CSVReader;

public class ImportDRB {

	private static final String AWSFILE = "/home/ec2-user/derby3.0/BUSINESS_NAMES_201803.csv";
	//private static final String FILE = "/Users/kingkung/University/Semester12018/DB/Assignment1/BUSINESS_NAMES_201803.csv";
	private static final String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String DBURL = "jdbc:derby:myDB;create=true";

	public static void main(String[] args) throws IOException, ClassNotFoundException, ParseException, SQLException{

		long startTime = System.currentTimeMillis();
		Class.forName(DRIVER);
		Connection conn = DriverManager.getConnection(DBURL);
		conn.setAutoCommit(false);
		Statement stmt = conn.createStatement();
		PreparedStatement pstmt = null;
		String createTable = "CREATE TABLE BUSINESS (BUSINESS_NAME VARCHAR(200), STATUS VARCHAR(12), DATE_OF_REG DATE, DATE_OF_CAN DATE, RENEWAL_DATE DATE,"
				+ "FORMER_STATE_NUM VARCHAR(10), PREV_STATE_OF_REG VARCHAR(3), ABN VARCHAR(20))";
		String insertRow = "INSERT INTO BUSINESS (BUSINESS_NAME, STATUS, DATE_OF_REG, DATE_OF_CAN, RENEWAL_DATE, FORMER_STATE_NUM, "
				+ "PREV_STATE_OF_REG, ABN) VALUES (?,?,?,?,?,?,?,?)";

		@SuppressWarnings({ "deprecation", "resource" })
		CSVReader reader = new CSVReader(new FileReader(FILE), '\t');
		String[] list;
		Date[] dates = new Date[6];

		//Create Table
		stmt.executeUpdate(createTable);
		//Start next line
		list = reader.readNext();
		//Read
		int line=0;
		final int batchSize = 1000;
		int count = 0;

		while ((list = reader.readNext()) != null) {
			line++;
			System.out.println(line);
				//Format Date
				list[1] = list[1].replace("\'", "\'\'");

				//date filter
				for(int d=3; d<=5; d++) {
					//list[d] = list[d].replace("/", "-");
					if (!list[d].isEmpty()) {
						//System.out.println(list[d]);
						Pattern  pattern = Pattern.compile("(\\d{4})/(\\d{2})/(\\d{2}).+");
						Matcher matcher = pattern.matcher(list[d]);
						if(matcher.matches()) {
							SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd");
							Date tmp = new Date();
							try {
								tmp = df.parse(list[d]);
							} catch (ParseException e) {
								e.printStackTrace();
							}
							list[d] = df.format(tmp).replace("/", "-");
							dates[d] = java.sql.Date.valueOf(list[d]);
							//System.out.println(list[d]);
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
							//System.out.println(list[d]);
						}
					} else continue;
				}

			try {
				pstmt = conn.prepareStatement(insertRow);
				pstmt.setString(1, list[1]);
				pstmt.setString(2, list[2]);
				if(dates[3] == null) {
					pstmt.setNull(3, Types.DATE);
				} else {
					pstmt.setDate(3, new java.sql.Date(dates[3].getTime()));
				}
				if(dates[4] == null) {
					pstmt.setNull(4, Types.DATE);
				} else {
					pstmt.setDate(4, new java.sql.Date(dates[4].getTime()));
				}
				if(dates[5] == null) {
					pstmt.setNull(5, Types.DATE);
				} else {
					pstmt.setDate(5, new java.sql.Date(dates[5].getTime()));
				}
				pstmt.setString(6, list[6]);
				pstmt.setString(7, list[7]);
				pstmt.setString(8, list[8]);
				pstmt.executeUpdate();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}


		long elapsedTime = System.currentTimeMillis() - startTime;
		float totInMin = elapsedTime/(60*1000F);
		System.out.println("Finished");
		System.out.println(totInMin);
	}

}
