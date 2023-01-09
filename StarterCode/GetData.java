import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;



//json.simple 1.1
// import org.json.simple.JSONObject;
// import org.json.simple.JSONArray;

// Alternate implementation of JSON modules.
import org.json.JSONObject;
import org.json.JSONArray;

public class GetData{
	
    static String prefix = "jiaqni.";
	
    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;
	
    // You must refer to the following variables for the corresponding 
    // tables in your database

    String cityTableName = null;
    String userTableName = null;
    String friendsTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;
    String programTableName = null;
    String educationTableName = null;
    String eventTableName = null;
    String participantTableName = null;
    String albumTableName = null;
    String photoTableName = null;
    String coverPhotoTableName = null;
    String tagTableName = null;

    // This is the data structure to store all users' information
    // DO NOT change the name
    JSONArray users_info = new JSONArray();		// declare a new JSONArray

	
    // DO NOT modify this constructor
    public GetData(String u, Connection c) {
	super();
	String dataType = u;
	oracleConnection = c;
	// You will use the following tables in your Java code
	cityTableName = prefix+dataType+"_CITIES";
	userTableName = prefix+dataType+"_USERS";
	friendsTableName = prefix+dataType+"_FRIENDS";
	currentCityTableName = prefix+dataType+"_USER_CURRENT_CITY";
	hometownCityTableName = prefix+dataType+"_USER_HOMETOWN_CITY";
	programTableName = prefix+dataType+"_PROGRAMS";
	educationTableName = prefix+dataType+"_EDUCATION";
	eventTableName = prefix+dataType+"_USER_EVENTS";
	albumTableName = prefix+dataType+"_ALBUMS";
	photoTableName = prefix+dataType+"_PHOTOS";
	tagTableName = prefix+dataType+"_TAGS";
    }
	
	
	
	
    //implement this function

    @SuppressWarnings("unchecked")
    public JSONArray toJSON() throws SQLException{ 

    	JSONArray users_info = new JSONArray();

	// Your implementation goes here....		
    	try (Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {

    		// GET ALL INFORMATION FROM USER TABLE
    		// ORDER THE ID so that user_id would be the index of the users_info array
    		String get_all_user = "SELECT USER_ID, FIRST_NAME, LAST_NAME, GENDER, YEAR_OF_BIRTH, MONTH_OF_BIRTH, DAY_OF_BIRTH " +
								  "FROM " + userTableName + " " +
								  "ORDER BY USER_ID ASC";
			ResultSet rst = stmt.executeQuery(get_all_user);

			while (rst.next()) {
				JSONObject item = new JSONObject();
				item.put("user_id", rst.getLong(1));
				item.put("first_name", rst.getString(2));
				item.put("last_name", rst.getString(3));
				item.put("gender", rst.getString(4));
				item.put("YOB", rst.getInt(5));
				item.put("MOB", rst.getInt(6));
				item.put("DOB", rst.getInt(7));
				users_info.put(item);
			}

			rst.close();


			// GET ALL HOMETOWN CITY
			// user_id is already the index I need
			String get_all_hometown = "SELECT UHC.USER_ID, C.CITY_NAME, C.STATE_NAME, C.COUNTRY_NAME " +
									  "FROM " + hometownCityTableName + " UHC, " + cityTableName + " C " +
									  "WHERE UHC.HOMETOWN_CITY_ID = C.CITY_ID " +
									  "ORDER BY UHC.USER_ID ASC";
			ResultSet rst1 = stmt.executeQuery(get_all_hometown);
			int count = 0;
			while(rst1.next()) {
				JSONObject item = new JSONObject();
				item.put("state", rst1.getString(3));
				item.put("city", rst1.getString(2));
				item.put("country", rst1.getString(4));

				while (count != rst1.getInt(1)) {
					JSONObject empty = new JSONObject();
					users_info.getJSONObject(count).put("hometown", empty);
					count = count + 1;
				}
				users_info.getJSONObject(rst1.getInt(1)).put("hometown", item);
				count = count + 1;
			}
			rst1.close();



			// GET ALL CURRENT CITY
			// user_id is already the index I need
			String get_all_current  = "SELECT UCC.USER_ID, C.CITY_NAME, C.STATE_NAME, C.COUNTRY_NAME " +
									  "FROM " + currentCityTableName + " UCC, " + cityTableName + " C " +
									  "WHERE UCC.CURRENT_CITY_ID = C.CITY_ID " +
									  "ORDER BY UCC.USER_ID ASC";
			ResultSet rst2 = stmt.executeQuery(get_all_current);
			count = 0;
			while(rst2.next()) {
				JSONObject item = new JSONObject();
				item.put("state", rst2.getString(3));
				item.put("city", rst2.getString(2));
				item.put("country", rst2.getString(4));

				while (count != rst2.getInt(1)) {
					JSONObject empty = new JSONObject();
					users_info.getJSONObject(count).put("hometown", empty);
					count = count + 1;
				}
				users_info.getJSONObject(rst2.getInt(1)).put("current", item);
				count = count + 1;
			}
			rst2.close();



			// GET ALL FRIENDS
			// user_id is already the index I need
			String get_all_friends  = "SELECT F.USER1_ID, F.USER2_ID, SUM.C " +
									  "FROM " + friendsTableName + " F, " + 
									  "(SELECT USER1_ID, COUNT(*) AS C FROM " + friendsTableName + " GROUP BY USER1_ID) SUM " +
									  "WHERE SUM.USER1_ID = F.USER1_ID " +
									  "ORDER BY F.USER1_ID ASC, F.USER2_ID ASC";
			ResultSet rst3 = stmt.executeQuery(get_all_friends);
			while(rst3.next()) {
				JSONArray friends = new JSONArray();
				friends.put(rst3.getLong(2));

				for (int i = 0; i < rst3.getInt(3) - 1; i ++) {
					rst3.next();
					friends.put(rst3.getLong(2));
				}
				users_info.getJSONObject(rst3.getInt(1)).put("friends", friends);
			}
			rst3.close();

			stmt.close();




    	}

    	catch (SQLException e) {
        	System.err.println(e.getMessage());
            return users_info;
        }
		
		return users_info;
    }

    // This outputs to a file "output.json"
    public void writeJSON(JSONArray users_info) {
	// DO NOT MODIFY this function
	try {
	    FileWriter file = new FileWriter(System.getProperty("user.dir")+"/output.json");
	    file.write(users_info.toString());
	    file.flush();
	    file.close();

	} catch (IOException e) {
	    e.printStackTrace();
	}
		
    }
}
