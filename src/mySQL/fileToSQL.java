package mySQL;

import myCalendar.CalendarQuickstart;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;

import com.google.api.client.util.DateTime;
import com.google.api.services.calendar.model.Event;
import com.google.api.services.calendar.model.EventDateTime;

public class fileToSQL {
	private static Connection connect = null;
	private Statement statement = null;
	private static PreparedStatement preparedStatement = null; 
	private static ResultSet resultSet = null; 
	
	public static void main(String[] args) throws Exception, IOException {
		PreparedStatement preparedstatement = null; 
		int time;
		int value; 
		try{
			// This will load the MySQL driver, each DB has its own driver
		    Class.forName("com.mysql.jdbc.Driver");
		    
		      // Setup the connection with the DB
		    connect = DriverManager
		          .getConnection("jdbc:mysql://localhost/band?"
		              + "user=mj&password=tlsalswp");
			String read = null;
			
			BufferedReader in = new BufferedReader(new FileReader("/Users/MJ/Downloads/heartRate.txt"));
			/*
			while((read = in.readLine()) != null){
				String[] splited = read.split("  ");
				System.out.println(splited[0].substring(3));
				System.out.println(splited[1]);

				time = Integer.parseInt(splited[0].substring(4));
				value = Integer.parseInt(splited[1]);
				addData(connect, preparedstatement, time,value);
			}
			*/
			
			
			/** read from the CSV file */
			
			String csvFile = "/Users/MJ/Downloads/heartRate.csv";
			BufferedReader br = null;
			String line = "";
			String csvSplitBy = ",";
			
			ArrayList<Double> unixTime = new ArrayList<Double>();
			ArrayList<Integer> mdate = new ArrayList<Integer>();
			ArrayList<Integer> mtime = new ArrayList<Integer>();
			
			ArrayList<Integer> mhour = new ArrayList<Integer>();
			ArrayList<Integer> mmin = new ArrayList<Integer>();
			ArrayList<Integer> mvalue = new ArrayList<Integer>();
			String sYear = "";
			String sMonth = "";
			String sDay = "";
			String sDate = "";
			String sHour = "";
			String sMin = "";
			String sTime = "";
		
			//add data from csv file onto the database 
			/*
			
			br = new BufferedReader(new FileReader(csvFile));
			br.readLine();
			while((line = br.readLine()) != null) {
				String[] data = line.split(csvSplitBy);
				sDate = data[1].substring(0, 4) + data[1].substring(5, 7) + data[1].substring(8, 10);
				sHour= data[1].substring(11,13);
				sMin = data[1].substring(14, 16);
				unixTime.add(Double.parseDouble(data[0]));
				mdate.add(Integer.parseInt(sDate));
				mtime.add(Integer.parseInt(sHour + sMin));
				mhour.add(Integer.parseInt(sHour));
				mmin.add(Integer.parseInt(sMin));
				mvalue.add(Integer.parseInt(data[2]));
				addData(connect, preparedstatement, Double.parseDouble(data[0]), Integer.parseInt(sDate), 
					Integer.parseInt(sHour+sMin), Integer.parseInt(data[2]));			
				
			}
					
			*/
			
			br = new BufferedReader(new FileReader(csvFile));
			br.readLine();
			while((line = br.readLine()) != null){
				String[] data = line.split(csvSplitBy);
				Date date = new Date(Long.parseLong(data[0]));
				
				sYear = new java.text.SimpleDateFormat("yyyy").format(date);
				sMonth = new java.text.SimpleDateFormat("MM").format(date);
				sDay = new java.text.SimpleDateFormat("dd").format(date);
				sHour = new java.text.SimpleDateFormat("HH").format(date);
				sMin = new java.text.SimpleDateFormat("mm").format(date);
				//System.out.println(sYear + "/" + sMonth + "/" + sDay + " ::: " + sHour+ "/" + sMin);	
				//addData(connect, preparedstatement, Double.parseDouble(data[0]), 
				//	Integer.parseInt(sYear+sMonth+sDay), Integer.parseInt(sHour), 
				//	Integer.parseInt(sMin),Integer.parseInt(data[2]));

			}
			
			int count = 0;
			preparedstatement = connect.prepareStatement("select count(*) from band.heartRate;");
			resultSet = preparedstatement.executeQuery();
			if(resultSet.next()){
				count = resultSet.getInt(1);
				//System.out.println(resultSet.getInt(1));
			}
				
			
			
			/** group the data in the database by hours */

			preparedstatement = connect.prepareStatement("select * from heartRate;");
			resultSet = preparedstatement.executeQuery();
			int hour = 0;
			int date = 0;
			Boolean first = true;
			Boolean inLoop = false;
			while(resultSet.next()){
				inLoop = true;
				if(hour == resultSet.getInt(4) && date == resultSet.getInt(3)){
					mdate.add(resultSet.getInt(3));
					mhour.add(resultSet.getInt(4));
					mvalue.add(resultSet.getInt(6));

				}
				else{
					if(!first){
						//addNewData(connect, preparedStatement, date, hour, mvalue);
					}
					mdate = new ArrayList<Integer>();
					mhour = new ArrayList<Integer>();
					mvalue = new ArrayList<Integer>();
					hour = resultSet.getInt(4);
					date = resultSet.getInt(3);
					first = !first;
				}
				
			}
			if(inLoop){
				//addNewData(connect,preparedStatement, date, hour, mvalue);
			}
			
			
			//createAnalysisTable(connect, preparedstatement);
			
			
			/*
			int i = 10;
			while(i < count){
				preparedstatement = connect.prepareStatement("select * from heartRate where ID > " + Integer.toString(i-10) + " and ID <= " + Integer.toString(i) + ";");
				resultSet = preparedstatement.executeQuery();
				int[] tempTime = new int[10];
				int[] tempValue = new int[10];
				int j = 0;
				while(resultSet.next()){
					tempTime[j] = resultSet.getInt(4);
					tempValue[j] = resultSet.getInt(5);
					j++;
				}
				//addNewData(connect, preparedstatement, tempTime, tempValue);
				i += 10;
				
			}
			
			*/
			
			/**Adding data to the calendar */
			
	        //making an event
	        com.google.api.services.calendar.Calendar service =
	                CalendarQuickstart.getCalendarService();
	        
			preparedstatement = connect.prepareStatement("select * from heart_rate_processed;");
			resultSet = preparedstatement.executeQuery();
			while(resultSet.next()){
				date = resultSet.getInt(2);
				String stDate = Integer.toString(date);
				hour = resultSet.getInt(3);
				String stHour = Integer.toString(hour);
				String ntHour = Integer.toString(hour+1);
				double heartRate = resultSet.getDouble(5);
				
				Event event = new Event()
		        .setSummary(Double.toString(heartRate))
		        .setDescription("heartRate");
	
			    DateTime startDateTime = new DateTime(stDate.substring(0, 4) + "-" +
			    stDate.substring(4, 6) + "-" + stDate.substring(6) +"T"+ stHour + ":00:00");
			    EventDateTime start = new EventDateTime()
			        .setDateTime(startDateTime)
			        .setTimeZone("UTC");
			    event.setStart(start);
			
			    DateTime endDateTime = new DateTime(stDate.substring(0, 4) + "-" +
					    stDate.substring(4, 6) + "-" + stDate.substring(6) +"T"+ ntHour + ":00:00");
			    EventDateTime end = new EventDateTime()
			        .setDateTime(endDateTime)
			        .setTimeZone("UTC");
			        
			    event.setEnd(end);
	
			    //service.events().insert("primary", event).execute();  
				
			}
			System.out.println("done");
			
		}catch (IOException e) {System.out.println("There was a problem: " + e);}
        	if (connect != null)
        		try{connect.close();} catch(SQLException ignore){} 
    	}
	
	





	private static void addNewData(Connection connect, PreparedStatement preparedstatement, int date, int hour,ArrayList<Integer> value) throws SQLException{
		preparedstatement = connect.prepareStatement("insert into band.heart_rate_processed(timeslotDate, timeSlotHour,min,max,mean,median,standard_deviation, biggest_change) values(?,?,?,?,?,?,?,?)");
		preparedstatement.setInt(1, date);
		preparedstatement.setInt(2, hour);
		preparedstatement.setInt(3, getMin(value));
		preparedstatement.setInt(4, getMax(value));
		preparedstatement.setDouble(5, getMean(value));
		preparedstatement.setInt(6, getMedian(value));
		preparedstatement.setDouble(7, getSD(value));
		preparedstatement.setInt(8, getMaxChange(value));

		preparedstatement.executeUpdate();
	}
	
	public static void createAnalysisTable(Connection connection, PreparedStatement preparedstatement) throws SQLException{
		preparedstatement = connection.prepareStatement("create table heart_rate_processed("
				+ "ID int not null Auto_increment primary key,"
				+ "timeslotDate int,"
				+ "timeslotHour int,"
				+ "min int,"
				+ "max int,"
				+ "mean double, "
				+ "median int,"
				+ "standard_deviation double,"
				+ "biggest_change int);");
		preparedstatement.executeUpdate();
		
	}
	public static int getMin(ArrayList<Integer> num){
		int min = Integer.MAX_VALUE;
	
		for(int var : num){
			if(var < min){
				min = var;
			}
		}
		return min;
	}
	public static int getMaxChange(ArrayList<Integer> num){
		int maxChange = 0; 
		int temp = 0;
		
		for(int i = 0; i<num.size(); i++){
			if(temp == 0){
				temp = num.get(i);
			}
			System.out.println(num.get(i));
			if(Math.abs(num.get(i) - temp) > Math.abs(maxChange)){
				System.out.println(num.get(i)-temp);
				maxChange = num.get(i) - temp;
			}
			temp = num.get(i);
		}
		return maxChange;
	}
	public static int getMax(ArrayList<Integer> num){
		int max = Integer.MIN_VALUE;
		for(int var: num){
			if(var > max){
				max = var;
			}
		}
		return max;
	}
	public static double getMean(ArrayList<Integer> num){
		double sum = 0;
		for(double var: num){
			sum += var;
		}
		return sum/(num.size());
	}
	public static int getMedian(ArrayList<Integer> num){
		Collections.sort(num);
		
		return num.get(num.size()/2);

	}
	public static double getSD(ArrayList<Integer> num){
		double deviation = 0.0;
		double mean = getMean(num);
		for(double var: num){
			double delta =var - mean;
			deviation += delta*delta;
		}
		deviation = Math.sqrt(deviation/num.size());
		return deviation;
	}
	public static void addData(Connection connection, PreparedStatement preparedstatement,double unix, int date, int hour, int min, int value)throws SQLException{
		preparedstatement = connection.prepareStatement("insert into band.heartRate(unix_time, date, hour, min,value) values(?,?,?,?,?)");
		preparedstatement.setDouble(1, unix);
		preparedstatement.setInt(2, date);
		preparedstatement.setInt(3, hour);
		preparedstatement.setInt(4, min);
		preparedstatement.setInt(5,value);
		preparedstatement.executeUpdate();
	}
	  public void readDataBase() throws Exception {
		    try {
		      // This will load the MySQL driver, each DB has its own driver
		      Class.forName("com.mysql.jdbc.Driver");
		      // Setup the connection with the DB
		      connect = DriverManager
		          .getConnection("jdbc:mysql://localhost/band?"
		              + "user=mj&password=tlsalswp");

		      // Statements allow to issue SQL queries to the database
		   
		      resultSet = statement
		      .executeQuery("select * from band.heartRate");
		      writeMetaData(resultSet);
		      
		    } catch (Exception e) {
		      throw e;
		    } finally {
		      close();
		    }

		  }
	  private void writeMetaData(ResultSet resultSet) throws SQLException {
		    //   Now get some metadata from the database
		    // Result set get the result of the SQL query
		    
		    System.out.println("The columns in the table are: ");
		    
		    System.out.println("Table: " + resultSet.getMetaData().getTableName(1));
		    for  (int i = 1; i<= resultSet.getMetaData().getColumnCount(); i++){
		      System.out.println("Column " +i  + " "+ resultSet.getMetaData().getColumnName(i));
		    }
		  }
	  
	  // You need to close the resultSet
	  private void close() {
	    try {
	      if (resultSet != null) {
	        resultSet.close();
	      }

	      if (statement != null) {
	        statement.close();
	      }

	      if (connect != null) {
	        connect.close();
	      }
	    } catch (Exception e) {

	    }
	  }


}
