package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }
    
    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                "SELECT COUNT(*) AS Birthed, Month_of_Birth " +         // select birth months and number of uses with that birth month
                "FROM " + UsersTable + " " +                            // from all users
                "WHERE Month_of_Birth IS NOT NULL " +                   // for which a birth month is available
                "GROUP BY Month_of_Birth " +                            // group into buckets by birth month
                "ORDER BY Birthed DESC, Month_of_Birth ASC");           // sort by users born in that month, descending; break ties by birth month
            
            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) {                       // step through result rows/records one by one
                if (rst.isFirst()) {                   // if first record
                    mostMonth = rst.getInt(2);         //   it is the month with the most
                }
                if (rst.isLast()) {                    // if last record
                    leastMonth = rst.getInt(2);        //   it is the month with the least
                }
                total += rst.getInt(1);                // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);
            
            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + mostMonth + " " +             // born in the most popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + leastMonth + " " +            // born in the least popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically

            return info;

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }
    
    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FirstNameInfo info = new FirstNameInfo();
                info.addLongName("Aristophanes");
                info.addLongName("Michelangelo");
                info.addLongName("Peisistratos");
                info.addShortName("Bob");
                info.addShortName("Sue");
                info.addCommonName("Harold");
                info.addCommonName("Jessica");
                info.setCommonNameCount(42);
                return info;
            */
            
            // Finds users with longest first names
            ResultSet rst = stmt.executeQuery(
                                              "SELECT DISTINCT FIRST_NAME " +
                                              "FROM " + UsersTable + " " +
                                              "WHERE FIRST_NAME IS NOT NULL " +
                                              "GROUP BY FIRST_NAME " +
                                              "ORDER BY length(FIRST_NAME) DESC");

            FirstNameInfo info = new FirstNameInfo();
            
            // iterate through rst table to the end where the shortest first name is
            String baseString = null;
            while(rst.next()){
                String first_name = rst.getString(1);
                if (rst.isFirst()){                     // if first record
                    baseString = rst.getString(1);      // it is the name with most letters
                }
                if (first_name.length() == baseString.length()){    // another first name same length as longest
                    info.addLongName(first_name);
                }
            }
            
            // iterate back from end until you find a first name longer than the shortest
            while(rst.previous()){
                String first_name = rst.getString(1);
                if (rst.isLast()){
                    baseString = first_name;
                }
                if (first_name.length() == baseString.length()){
                    info.addShortName(first_name);
                }
                else break;
            }
            
            rst = stmt.executeQuery(
                                    "SELECT FIRST_NAME, COUNT(FIRST_NAME) " +
                                    "FROM " + UsersTable + " " +
                                    "WHERE FIRST_NAME IS NOT NULL " +
                                    "GROUP BY FIRST_NAME ORDER BY 2 DESC");
            
            // find first name held by most users and count
            int baseCount = 0;
            while(rst.next()) {
                String first_name = rst.getString(1);
                int count = rst.getInt(2);
                if (rst.isFirst()) {
                    baseCount = count;
                    info.setCommonNameCount(baseCount);
                }
                if(count != baseCount) break;
                
                info.addCommonName(first_name);
            }
            
            rst.close();
            stmt.close();
            
            return info;                // placeholder for compilation
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }
    
    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
                UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
                results.add(u1);
                results.add(u2);
            */
            ResultSet rst = stmt.executeQuery("SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " +
                                              "FROM "+ UsersTable + " U " +
                                              "WHERE U.USER_ID NOT IN (SELECT DISTINCT F.USER1_ID " +
                                              "FROM " + FriendsTable + " F " +
                                              "UNION " +
                                              "SELECT DISTINCT F.USER2_ID " +
                                              "FROM " + FriendsTable + " F)");
            
            // go through each individual user
            while (rst.next()){
                long user_id = rst.getLong(1);
                String first_name = rst.getString(2);
                String last_name = rst.getString(3);
                UserInfo u = new UserInfo(user_id, first_name, last_name);
                results.add(u);
            }
            
            rst.close();
            stmt.close();
        }
        
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
                UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
                results.add(u1);
                results.add(u2);
            */
            ResultSet rst = stmt.executeQuery("SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " +
                                              "FROM " + UsersTable + " U, " + CurrentCitiesTable + " C, " + HometownCitiesTable + " H " +
                                              "WHERE U.USER_ID = C.USER_ID AND U.USER_ID = H.USER_ID " +
                                              "AND C.CURRENT_CITY_ID IS NOT NULL AND H.HOMETOWN_CITY_ID IS NOT NULL " +
                                              "AND C.CURRENT_CITY_ID != H.HOMETOWN_CITY_ID " +
                                              "ORDER BY 1 ASC");
            
            // go through each individual user
            while (rst.next()){
                long id = rst.getLong(1);
                String first_name = rst.getString(2);
                String last_name = rst.getString(3);
                UserInfo u = new UserInfo(id, first_name, last_name);
                results.add(u);
            }
            
            rst.close();
            stmt.close();
        }
        
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
                UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
                UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
                UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                tp.addTaggedUser(u1);
                tp.addTaggedUser(u2);
                tp.addTaggedUser(u3);
                results.add(tp);
            */
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
                UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
                MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
                PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
                mp.addSharedPhoto(p);
                results.add(mp);
            */
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(16, "The", "Hacker");
                UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
                UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
                UsersPair up = new UsersPair(u1, u2);
                up.addSharedFriend(u3);
                results.add(up);
            */
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                EventStateInfo info = new EventStateInfo(50);
                info.addState("Kentucky");
                info.addState("Hawaii");
                info.addState("New Hampshire");
                return info;
            */
            ResultSet rst = stmt.executeQuery("SELECT DISTINCT STATE_NAME, COUNT(*) " +
                                              "FROM " + EventsTable + " E " +
                                              "LEFT JOIN " + CitiesTable + " C ON E.EVENT_CITY_ID = C.CITY_ID " +
                                              "WHERE STATE_NAME IS NOT NULL " +
                                              "GROUP BY STATE_NAME " +
                                              "HAVING COUNT(*) = (SELECT MAX(COUNT(*)) " +
                                                                "from " + EventsTable + " E " +
                                                                "LEFT JOIN " + CitiesTable + " C ON E.EVENT_CITY_ID = C.CITY_ID " +
                                                                "WHERE STATE_NAME IS NOT NULL " +
                                                                "GROUP BY STATE_NAME)");
            
            long count = 0;
            while (rst.next()) {
                count = rst.getLong(2);
            }
            
            EventStateInfo info = new EventStateInfo(count);
            
            while (rst.previous()){
                
            }
            
            while (rst.next()){
                String state_name = rst.getString(1);
                info.addState(state_name);
            }
            rst.close();
            stmt.close();
            
            return info;
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }
    
    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */
            ResultSet rst = stmt.executeQuery("SELECT U.user_id, U.FIRST_NAME, U.LAST_NAME " +
                                              "FROM " + FriendsTable + " F, " + UsersTable + " U " +
                                              "WHERE (F.USER1_ID = " + userID + " AND U.USER_ID = F.USER2_ID) OR (F.USER2_ID = " + userID + " AND U.USER_ID = F.USER1_ID) " +
                                              "ORDER BY U.YEAR_OF_BIRTH ASC, U.MONTH_OF_BIRTH ASC, U.DAY_OF_BIRTH ASC, U.USER_ID DESC"
                                              );
            
            // finding oldest firend
            long user_id = 0;
            String first_name = null;
            String last_name = null;
            if (rst.next()){
                user_id = rst.getLong(1);
                first_name = rst.getString(2);
                last_name = rst.getString(3);
            }
            UserInfo old = new UserInfo(user_id, first_name, last_name);
            
            // finding youngest friend
            long user_id2 = 0;
            String first_name2 = null;
            String last_name2 = null;
            if (rst.last()){
                user_id2 = rst.getLong(1);
                first_name2 = rst.getString(2);
                last_name2 = rst.getString(3);
            }
            
            UserInfo young = new UserInfo(user_id2, first_name2, last_name2);
            
            rst.close();
            stmt.close();
            return new AgeInfo(old, young);
            
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }
    
    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
                UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            */
            ResultSet rst = stmt.executeQuery(
                                              "SELECT U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME " +
                                              "FROM " + UsersTable + " U1, " + UsersTable + " U2, " +
                                              HometownCitiesTable + " H1, " + HometownCitiesTable + " H2, " + FriendsTable +" F " +
                                              "WHERE U1.USER_ID < U2.USER_ID " +
                                              "AND U1.LAST_NAME = U2.LAST_NAME " +        // same last name
                                              "AND U1.USER_ID = H1.USER_ID " +
                                              "AND U2.USER_ID = H2.USER_ID " +
                                              "AND H1.HOMETOWN_CITY_ID = H2.HOMETOWN_CITY_ID " +    // same hometown
                                              "AND U1.USER_ID = F.USER1_ID " +
                                              "AND U2.USER_ID = F.USER2_ID " +
                                              "AND ABS(U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) < 10" + // less than 10 years
                                              "ORDER BY U1.USER_ID ASC, U2.USER_ID ASC");
            while (rst.next()){
                Long user1_id = rst.getLong(1);
                String first_name_1 = rst.getString(2);
                String last_name_1 = rst.getString(3);
                
                UserInfo u1 = new UserInfo(user1_id, first_name_1, last_name_1);
                
                Long user2_id = rst.getLong(4);
                String first_name_2 = rst.getString(5);
                String last_name_2 = rst.getString(6);
                
                UserInfo u2 = new UserInfo(user2_id, first_name_2, last_name_2);
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            }
            rst.close();
            stmt.close();
            
            return results;
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
