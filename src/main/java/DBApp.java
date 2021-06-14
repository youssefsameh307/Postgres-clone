// init merging  here
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

// init merging 
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
public class DBApp implements DBAppInterface {

	Hashtable<String, Table> tables = new Hashtable<String, Table>();
	// here
	public static Comparable checkContent(Table t, Hashtable<String,Object> DBRecord, Hashtable<String,Object> otherRecord) {
		Set s1 = DBRecord.entrySet();
		Set s2 = otherRecord.entrySet();
		
		if(s1.containsAll(s2)) {
			return (Comparable) DBRecord.get(t.clusteringKey);
		}
		return null;
	}
	
	public void loadTableToDB(String tableName) {
		try {
			ArrayList<String> oldTables = Table.getTableNamesFromMeta();
			 for(String x: oldTables) {
				 if(x.equals(tableName))
					 tables.put(x, Table.DeserializeTable("./src/main/resources/data/tables/"+x+".ser"));
			 }
			 System.err.println(tableName + " table loaded successfully \n");
		 } catch (IndexOutOfBoundsException e) {
				System.err.println("no tables to be loaded \n");
		} catch (Exception e) {
				System.err.println("error in loading tables \n");
		}
	}
	
	public void deLoadTableFromDB(String tableName) {
		try {
			 for(String x: this.tables.keySet()) {
				 if(x.equals(tableName)) {
					 this.tables.get(x).SerializeTable();
					 this.tables.remove(x);
					 System.err.println(tableName + " table de-loaded successfully \n");
					 return;
				 }
			 }
		 } catch (IndexOutOfBoundsException e) {
				System.err.println("no tables to be de-loaded \n");
		} catch (Exception e) {
				System.err.println("error in de-loading tables \n");
		}
	}
	
	@Override
	public void init() {
		System.out.println("init");
		File folder = new File(System.getProperty("user.dir")+"/src/main/resources");
		File[] listOfFiles = folder.listFiles();
		String Path = System.getProperty("user.dir")+"/src/main/resources";
		try {
			System.out.println(get_conf());
		} catch (FileNotFoundException e) {
			if(e.getMessage() != null)
        		System.err.println(e.getMessage());
			//e.printStackTrace();
		}
		
		File data = new File("./src/main/resources/data");
		if(!data.exists()) {
			data.mkdir();
			System.err.println("data directory created sucessfully");
		}
		
		File meta = new File("./src/main/resources/metadata.csv");
		if(!meta.exists()) {
			try {
				meta.createNewFile();
			} catch (IOException e) {
				System.err.println("error in creating metadata file");
			}
			System.err.println("metadata file created sucessfully");
		}
				
	}

	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException{
                try {
                    Table2 t = new Table2(tableName, clusteringKey, colNameType, colNameMin, colNameMax, true);
                    tables.put(tableName, t);
                } catch (IOException e) {
                   throw new DBAppException("IO excetion while createing table");
                }
            }
	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
		loadTableToDB(tableName);
        Table t = tables.get(tableName);
        try {
            t.createIndex(columnNames);
        } catch (IOException e) {
            System.out.println("error occured while creating index please check column names");
            e.printStackTrace();
        }
        deLoadTableFromDB(tableName);
	}

	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        // TODO: JOE make sure all to validate the record going in that is 1) has no extr rows from table 2) has the clustering key with a value not null
		loadTableToDB(tableName);
	    
		try {
			Hashtable<String, Object> newrecord = new Hashtable<String, Object>();
			for(String x: colNameValue.keySet()) {
				newrecord.put(x, colNameValue.get(x)); // # there is always a purpose 
			}

	           Table2 t = (Table2)tables.get(tableName);
               t.validate_input(newrecord);
	           if(t == null)	
	        	   throw new DBAppException("table not found");
	           else {
	        	   if((newrecord.get(t.clusteringKey) == null) || (newrecord.get(t.clusteringKey).toString().length() == 0)) {
	        			deLoadTableFromDB(tableName);
	        		   throw new DBAppException("primary key cannot be null");
	        	   }
	        	   if(t.keys.get(newrecord.get(t.clusteringKey).toString()) == null) {
	        		   t.insert(newrecord);
	        		   t.keys.put(newrecord.get(t.clusteringKey).toString(), newrecord.get(t.clusteringKey).toString());
	        		   //t.SerializeTable();
	        			deLoadTableFromDB(tableName);
	        	   }
	        	   else {
	        			deLoadTableFromDB(tableName);
		        	   throw new DBAppException("record already exists found");
	        	   }
	           }
			}catch (IOException e){
    			deLoadTableFromDB(tableName);
				if(e.getMessage() != null)
	        		System.err.println(e.getMessage());
	    	   //e.printStackTrace();
	       }
		deLoadTableFromDB(tableName);
		System.out.println("insert successful");
	}

	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {
		// TODO JOE: validate what is coming in from the type to that all the columns exist in this table
		loadTableToDB(tableName);
		
		try {
			
			if( (clusteringKeyValue == null) || (clusteringKeyValue.length() == 0) ) {
    			deLoadTableFromDB(tableName);
				throw new DBAppException("primary key cannot be empty");
			}
			Hashtable<String, Object> newrecord = new Hashtable<String, Object>();
			for(String x: columnNameValue.keySet()) {
					newrecord.put(x, columnNameValue.get(x));
			}

	           Table2 t = (Table2) tables.get(tableName);
               columnNameValue.put(t.clusteringKey, clusteringKeyValue);
               t.validate_input(columnNameValue);
	           if(t == null)	
	        	   throw new DBAppException("table not found");
	           else {
	        	   Object value = Table.get_parsed_value(clusteringKeyValue, Table.getTypeCast(t.tableName, t.clusteringKey));
	        	   if(t.keys.get(value.toString()) != null) {
                       if(value instanceof Date){
                           value= new SimpleDateFormat("yyyy-MM-dd").parse(value.toString()); 
                       }
                       newrecord.put(t.clusteringKey, value);
                       
	        		   t.update((Comparable) value, newrecord);
	        			deLoadTableFromDB(tableName);
	        	   }
	        	   else {
	        			deLoadTableFromDB(tableName);
	        		   throw new DBAppException("record does not exist");
	        	   }
	           }
			}catch (IOException | ParseException e){
    			deLoadTableFromDB(tableName);
				if(e.getMessage() != null)
	        		System.err.println(e.getMessage());
				//e.printStackTrace();
	       }
		deLoadTableFromDB(tableName);
		System.out.println("updated successfully");
		
	}

	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
 
		loadTableToDB(tableName);
		
		try {
//			Vector<Comparable> deleted = new Vector<>();
			Hashtable<String, Object> newrecord = new Hashtable<String, Object>();
			for(String x: columnNameValue.keySet()) {
					newrecord.put(x, columnNameValue.get(x));
			}
			
			Table2 t = (Table2) tables.get(tableName);
			if(t == null)	
				throw new DBAppException("table not found");
			else {
				Object o = newrecord.get(t.clusteringKey);
				if(o == null) {
					if(t.assertRangeC(newrecord) == -1) {
	        			deLoadTableFromDB(tableName);
			        	throw new DBAppException("incompatible data type");
					}
			    	if(t.assertRangeC(newrecord) == 0) {
	        			deLoadTableFromDB(tableName);
			        	throw new DBAppException("data out of range");
			    	}
					for(Vector<Hashtable<String, Object>> pi :t.pages) {
						for(Hashtable<String, Object> pii: pi) {
							Page p = Page.DeserializePage(pii.get("path").toString(), t);
							for(Hashtable h :p.contents) {
								Comparable key = checkContent(t, h, columnNameValue);
								if(key == null)
									continue;
								else {
									t.delete(key);
									t.keys.remove(key.toString());
									t.SerializeTable();
//									deleted.add(key);
									}
								}
							}
						}
//					for(Comparable x: deleted)
//						t.delete(x);
					}
				else {
					if(t.assertRangeC(newrecord) == -1) {
	        			deLoadTableFromDB(tableName);
			        	throw new DBAppException("incompatible data type");
					}
			    	if(t.assertRangeC(newrecord) == 0) {
	        			deLoadTableFromDB(tableName);
			        	throw new DBAppException("data out of range");
			    	}
					//t.assertRangeC(newrecord);
					t.delete((Comparable) o);
					t.keys.remove(o.toString());
					//t.SerializeTable();
        			deLoadTableFromDB(tableName);
					}
				}
			} catch (Exception e) {
    			deLoadTableFromDB(tableName);
    			if(e.getMessage() != null)
            		System.err.println(e.getMessage());
    			//e.printStackTrace();
			}
		deLoadTableFromDB(tableName);
		System.out.println("delete successful");
	}

	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		
        
        loadTableToDB(sqlTerms[0]._strTableName);
        Table t = tables.get(sqlTerms[0]._strTableName);
        Vector<Hashtable<String, Object>> results=new Vector<>();
        try {
            results = t.selectFromTable(sqlTerms, arrayOperators);
        } catch (IOException e) {
            System.out.println("error occured while selecting data from table please check input");
            e.printStackTrace();
        }
        deLoadTableFromDB(sqlTerms[0]._strTableName);
        return results.iterator();
    }
	
    public void testUpdateStudentsExtra2() {

        String table = "students";
        Hashtable<String, Object> row = new Hashtable();
        row.put("first_name", "foo");
        row.put("middle_name", "hamada");
        row.put("last_name", "bar");

        Date dob = new Date(1992 - 1900, 9 - 1, 8);
        row.put("dob", dob);
        row.put("gpa", 1.1);

        try {
			updateTable(table, "82-8772", row);
		} catch (DBAppException e) {
			e.printStackTrace();
		}

    }
	
	public static void main(String args[]) throws Exception
	{  		
		DBApp dbApp = new DBApp();
		dbApp.init();
		
		dbApp.createPCsTable(dbApp);
		
		Hashtable<String, Object> row = new Hashtable<String, Object>();
		
		for(int i=1; i<=8; i++) {
			row.put("pc_id", i);
			row.put("student_id", "45-6900");
			dbApp.insertIntoTable("pcs", row);
			row.clear();
		}
		row.put("pc_id", 20);
		row.put("student_id", "45-6900");
		dbApp.insertIntoTable("pcs", row);
		row.clear();
		for(int i=9; i<=19; i++) {
			row.put("pc_id", i);
			row.put("student_id", "45-6900");
			dbApp.insertIntoTable("pcs", row);
			row.clear();
		}
		for(int i=21; i<=25; i++) {
			row.put("pc_id", i);
			row.put("student_id", "45-6900");
			dbApp.insertIntoTable("pcs", row);
			row.clear();
		}
		row.put("pc_id", 30);
		row.put("student_id", "45-6900");
		dbApp.insertIntoTable("pcs", row);
		row.clear();
		for(int i=26; i<=29; i++) {
			row.put("pc_id", i);
			row.put("student_id", "45-6900");
			dbApp.insertIntoTable("pcs", row);
			row.clear();
		}
		System.out.println("test");
		row.put("pc_id", 21);
		row.put("student_id", "45-6900");
		dbApp.deleteFromTable("pcs", row);
		row.clear();
		row.put("pc_id", 22);
		row.put("student_id", "45-6900");
		dbApp.deleteFromTable("pcs", row);
		row.clear();
		row.put("pc_id", 23);
		row.put("student_id", "45-6900");
		dbApp.deleteFromTable("pcs", row);
		row.clear();
		
		
		
		
		
		
		
		
		

	}  
	
	
	private void createStudentTable(DBApp dbApp) throws Exception {
        String tableName = "students";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("id", "java.lang.String");
        htblColNameType.put("first_name", "java.lang.String");
        htblColNameType.put("last_name", "java.lang.String");
        htblColNameType.put("dob", "java.util.Date");
        htblColNameType.put("gpa", "java.lang.Double");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("id", "43-0000");
        minValues.put("first_name", "AAAAAA");
        minValues.put("last_name", "AAAAAA");
        minValues.put("dob", "1990-01-01");
        minValues.put("gpa", "0.7");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("id", "99-9999");
        maxValues.put("first_name", "zzzzzz");
        maxValues.put("last_name", "zzzzzz");
        maxValues.put("dob", "2000-12-31");
        maxValues.put("gpa", "5.0");

        dbApp.createTable(tableName, "id", htblColNameType, minValues, maxValues);
    }

    private void createCoursesTable(DBApp dbApp) throws Exception {
        String tableName = "courses";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("date_added", "java.util.Date");
        htblColNameType.put("course_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("hours", "java.lang.Integer");


        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("date_added", "1990-01-01");
        minValues.put("course_id", "100");
        minValues.put("course_name", "AAAAAA");
        minValues.put("hours", "1");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("date_added", "2000-12-31");
        maxValues.put("course_id", "2000");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("hours", "24");

        dbApp.createTable(tableName, "date_added", htblColNameType, minValues, maxValues);

    }

    private void createTranscriptsTable(DBApp dbApp) throws Exception {
        String tableName = "transcripts";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("gpa", "java.lang.Double");
        htblColNameType.put("student_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("date_passed", "java.util.Date");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("gpa", "0.7");
        minValues.put("student_id", "43-0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("date_passed", "1990-01-01");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("gpa", "5.0");
        maxValues.put("student_id", "99-9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("date_passed", "2020-12-31");

        dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
    }

    private void createPCsTable(DBApp dbApp) throws Exception {
        String tableName = "pcs";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("pc_id", "java.lang.Integer");
        htblColNameType.put("student_id", "java.lang.String");


        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("pc_id", "0");
        minValues.put("student_id", "43-0000");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("pc_id", "20000");
        maxValues.put("student_id", "99-9999");

        dbApp.createTable(tableName, "pc_id", htblColNameType, minValues, maxValues);
    }
	
    public void testClearMetaDataFile() throws Exception{

        String metaFilePath = "src/main/resources/metadata.csv";
        File metaFile = new File(metaFilePath);

        if (!metaFile.exists()) {
            throw new Exception("`metadata.csv` in Resources folder does not exist");
        }

        PrintWriter writer = new PrintWriter(metaFile);
        writer.write("");
        writer.close();
    }
    
    public void testDataDirectory() throws Exception {
        String dataDirPath = "src/main/resources/data";
        File dataDir = new File(dataDirPath);

        if (!dataDir.isDirectory() || !dataDir.exists()) {
            throw new Exception("`data` Directory in Resources folder does not exist");
        }

        ArrayList<String> files = new ArrayList<>();
        try {
            files = Files.walk(Paths.get(dataDirPath))
                    .map(f -> f.toAbsolutePath().toString())
                    .filter(p -> !Files.isDirectory(Paths.get(p)))
                    .collect(Collectors.toCollection(ArrayList::new));
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (String file : files) {
            Files.delete(Paths.get(file));
        }
    }
    
    private void insertStudentRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader studentsTable = new BufferedReader(new FileReader("src/main/resources/students_table.csv"));
        String record;
        int c = limit;
        if (limit == -1) {
            c = 1;
        }

        Hashtable<String, Object> row = new Hashtable<>();
        while ((record = studentsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("id", fields[0]);
            row.put("first_name", fields[1]);
            row.put("last_name", fields[2]);

            int year = Integer.parseInt(fields[3].trim().substring(0, 4));
            int month = Integer.parseInt(fields[3].trim().substring(5, 7));
            int day = Integer.parseInt(fields[3].trim().substring(8));

            Date dob = new Date(year - 1900, month - 1, day);
            row.put("dob", dob);

            double gpa = Double.parseDouble(fields[4].trim());

            row.put("gpa", gpa);

            dbApp.insertIntoTable("students", row);
            row.clear();
            if (limit != -1) {
                c--;
            }
        }
        studentsTable.close();
    }

    private void insertCoursesRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader coursesTable = new BufferedReader(new FileReader("src/main/resources/courses_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = coursesTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");



            int year = Integer.parseInt(fields[0].trim().substring(0, 4));
            int month = Integer.parseInt(fields[0].trim().substring(5, 7));
            int day = Integer.parseInt(fields[0].trim().substring(8));

            Date dateAdded = new Date(year - 1900, month - 1, day);

            row.put("date_added", dateAdded);

            row.put("course_id", fields[1]);
            row.put("course_name", fields[2]);
            row.put("hours", Integer.parseInt(fields[3]));


            dbApp.insertIntoTable("courses", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        coursesTable.close();
    }

    private void insertTranscriptsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader transcriptsTable = new BufferedReader(new FileReader("src/main/resources/transcripts_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = transcriptsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("gpa", Double.parseDouble(fields[0].trim()));
            row.put("student_id", fields[1].trim());
            row.put("course_name", fields[2].trim());

            String date = fields[3].trim();
            int year = Integer.parseInt(date.substring(0, 4));
            int month = Integer.parseInt(date.substring(5, 7));
            int day = Integer.parseInt(date.substring(8));

            Date dateUsed = new Date(year - 1900, month - 1, day);
            row.put("date_passed", dateUsed);

            dbApp.insertIntoTable("transcripts", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        transcriptsTable.close();
    }

    private void insertPCsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader pcsTable = new BufferedReader(new FileReader("src/main/resources/pcs_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = pcsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("pc_id", Integer.parseInt(fields[0].trim()));
            row.put("student_id", fields[1].trim());

            dbApp.insertIntoTable("pcs", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        pcsTable.close();
    }
    
    public void testExtraStudentsInsertion() {
        String table = "students";
        Hashtable<String, Object> row = new Hashtable();
        row.put("id", "31-1235");
        row.put("first_name", "foo");
        row.put("middle_name", "bateekh");
        row.put("last_name", "bar");


        Date dob = new Date(1995 - 1900, 4 - 1, 1);
        row.put("dob", dob);

        row.put("gpa", 1.1);
        
        try {
			insertIntoTable(table, row);
		} catch (DBAppException e) {
			e.printStackTrace();
		}
    }

    public void testExtraCoursesInsertion() {
        String table = "courses";
        Hashtable<String, Object> row = new Hashtable();

        Date date_added = new Date(2011 - 1900, 4 - 1, 1);
        row.put("date_added", date_added);

        row.put("course_id", "foo");
        row.put("course_name", "bar");
        row.put("hours", 13);
        row.put("semester", 5);
        
        try {
			insertIntoTable(table, row);
		} catch (DBAppException e) {
			e.printStackTrace();
		}
    }

    public void testExtraTranscriptsInsertion() {
        String table = "transcripts";
        Hashtable<String, Object> row = new Hashtable();
        row.put("gpa", 1.5);
        row.put("student_id", "34-9874");
        row.put("course_name", "bar");
        row.put("elective", true);


        Date date_passed = new Date(2011 - 1900, 4 - 1, 1);
        row.put("date_passed", date_passed);
        
        try {
			insertIntoTable(table, row);
		} catch (DBAppException e) {
			e.printStackTrace();
		}
    }

    public void testExtraPCsInsertion() {
        String table = "pcs";
        Hashtable<String, Object> row = new Hashtable();
        row.put("pc_id", 50);
        row.put("student_id", "31-12121");
        row.put("room", "C7.02");
        
        try {
			insertIntoTable(table, row);
		} catch (DBAppException e) {
			e.printStackTrace();
		}
    }
    
    public void testUpdateStudents() throws Exception {
    	
        String table = "students";
        Hashtable<String, Object> row = new Hashtable();
        row.put("first_name", "foo");
        row.put("last_name", "bar");

        Date dob = new Date(1992 - 1900, 9 - 1, 8);
        row.put("dob", dob);
        row.put("gpa", 1.1);

        this.updateTable(table, "82-8772", row);
    }

    public void testUpdateCourses() throws Exception {
       
        String table = "courses";
        Hashtable<String, Object> row = new Hashtable();

        row.put("course_id", "foo");
        row.put("course_name", "bar");
        row.put("hours", 13);
                
        this.updateTable(table, "2000-04-03", row);
    }

    public void testUpdateTranscripts() throws Exception {

        String table = "transcripts";
        Hashtable<String, Object> row = new Hashtable();

        row.put("student_id", "34-9874");
        row.put("course_name", "bar");

        Date date_passed = new Date(2011 - 1900, 4 - 1, 1);
        row.put("date_passed", date_passed);

        this.updateTable(table, "1.57", row);
    }

    public void testUpdatePCs() throws Exception {

        String table = "pcs";
        Hashtable<String, Object> row = new Hashtable();
        row.put("student_id", "32-12121");

        this.updateTable(table, "50", row);
    }
    
    public void testUpdateStudentsExtra() {

        String table = "students";
        Hashtable<String, Object> row = new Hashtable();
        row.put("first_name", "foo");
        row.put("middle_name", "hamada");
        row.put("last_name", "bar");

        Date dob = new Date(1992 - 1900, 9 - 1, 8);
        row.put("dob", dob);
        row.put("gpa", 1.1);
        
        try {
			this.updateTable(table, "82-8772", row);
		} catch (DBAppException e) {
			e.printStackTrace();
		}
        
    }
    
    public void testUpdateCoursesExtra() {
        
        String table = "courses";
        Hashtable<String, Object> row = new Hashtable();
        row.put("elective", true);
        row.put("course_id", "foo");
        row.put("course_name", "bar");
        row.put("hours", 13);
        row.put("semester", 5);
        
        try {
			this.updateTable(table, "2000-04-01", row);
		} catch (DBAppException e) {
			e.printStackTrace();
		}
    }

    public void testUpdateTranscriptsExtra() {
        
        String table = "transcripts";
        Hashtable<String, Object> row = new Hashtable();

        row.put("student_id", "34-9874");
        row.put("course_name", "bar");

        Date date_passed = new Date(2011 - 1900, 4 - 1, 1);
        row.put("date_passed", date_passed);

        row.put("elective", true);

        try {
			this.updateTable(table, "1.57", row);
		} catch (DBAppException e) {
			e.printStackTrace();
		}

    }

    public void testUpdatePCsExtra() {
        
        String table = "pcs";
        Hashtable<String, Object> row = new Hashtable();
        row.put("student_id", "32-12121");

        row.put("os", "linux");

        try {
			this.updateTable(table, "50", row);
		} catch (DBAppException e) {
			e.printStackTrace();
		}

    }

    public void testStudentsDeletionComplex() throws Exception {
        
        String table = "students";
        Hashtable<String, Object> row = new Hashtable();

        Date dob = new Date(1993 - 1900, 11 - 1, 21);
        row.put("dob", dob);

        row.put("gpa", 1.23);

        deleteFromTable(table, row);

    }

    public void testCoursesDeleteComplex() throws Exception {

        String table = "courses";
        Hashtable<String, Object> row = new Hashtable();
        Date dateAdded = new Date(2000 - 1900, 11 - 1, 21);
        row.put("date_added", dateAdded);
        row.put("course_name", "pAYqDr");

        deleteFromTable(table, row);
    }

    public void testTranscriptsDeleteComplex() throws Exception {
        
        String table = "transcripts";
        Hashtable<String, Object> row = new Hashtable();

        row.put("course_name", "TwKnJm");
        row.put("gpa", 0.92);

        deleteFromTable(table, row);
    }

    public void testPCsDeleteComplex() throws Exception {
        
        String table = "pcs";
        Hashtable<String, Object> row = new Hashtable();
        row.put("pc_id", 18763);
        row.put("student_id", "57-4782");

        deleteFromTable(table, row);
    }
    
    
    
    
    
    
    
    
    
    
    
    
    
	private static File get_conf_file() throws FileNotFoundException {
		String Path = System.getProperty("user.dir")+"/src/main/resources";
		return new File(Path+"/DBApp.config");
	}
	private static HashMap<String, String> get_conf() throws FileNotFoundException {
		HashMap<String, String> map = new HashMap<String, String>();
		 Scanner sc = new Scanner(get_conf_file());
		 while (sc.hasNextLine()) {
		     String line = sc.nextLine();
		     String[] key_value_pair = line.split("=");
		    map.put(key_value_pair[0], key_value_pair[1]);
		 }
		 sc.close();
		 return map;
	}
}

		
		
//		for(int i=1; i<=3; i++) {
//			row.put("pc_id", i);
//			row.put("student_id", "45-6900");
//			dbApp.insertIntoTable("pcs", row);
//			row.clear();
//		}
//		for(int i=5; i<=7; i++) {
//			row.put("pc_id", i);
//			row.put("student_id", "45-6900");
//			dbApp.insertIntoTable("pcs", row);
//			row.clear();
//		}
//		for(int i=9; i<=11; i++) {
//			row.put("pc_id", i);
//			row.put("student_id", "45-6900");
//			dbApp.insertIntoTable("pcs", row);
//			row.clear();
//		}
//		row.put("pc_id", 4);
//		row.put("student_id", "55-5555");
//		dbApp.insertIntoTable("pcs", row);
//		row.clear();
//		row.put("pc_id", 8);
//		row.put("student_id", "55-5555");
//		dbApp.insertIntoTable("pcs", row);
//		row.clear();
//		row.put("pc_id", 8);
//		row.put("student_id", "55-5555");
//		dbApp.deleteFromTable("pcs", row);
//		row.clear();
		
//		row.put("pc_id", 1);
//		row.put("student_id", "55-5555");
//		dbApp.insertIntoTable("pcs", row);
//		row.clear();
//		row.put("pc_id", 2);
//		row.put("student_id", "55-5555");
//		dbApp.insertIntoTable("pcs", row);
//		row.put("pc_id", 3);
//		row.put("student_id", "55-5555");
//		dbApp.insertIntoTable("pcs", row);
//		row.clear();
//		
//		//test overflow insert
//		for(int i=4; i<=5; i++) {
//			row.put("pc_id", i);
//			row.put("student_id", "45-6900");
//			dbApp.insertIntoTable("pcs", row);
//			row.clear();
//		}
//		row.put("pc_id", 100);
//		row.put("student_id", "45-6900");
//		dbApp.insertIntoTable("pcs", row);
//		row.clear();
//		for(int i=11; i<=15; i++) {
//			row.put("pc_id", i);
//			row.put("student_id", "45-6900");
//			dbApp.insertIntoTable("pcs", row);
//			row.clear();
//		}
//		
//		//test overflow update
//		for(int i=11; i<=15; i++) {
//			row.put("student_id", "99-9999");
//			dbApp.updateTable("pcs", i+"", row);
//			row.clear();
//		}
//		for(int i=10; i<=15; i++) {
//			row.put("student_id", "99-9999");
//			dbApp.updateTable("pcs", i+"", row);
//			row.clear();
//		}

//		System.out.println("begin");
//		//test overflow delete
//		row.put("pc_id", 1);
//		dbApp.deleteFromTable("pcs", row);
//		row.clear();
//		row.put("pc_id", 2);
//		dbApp.deleteFromTable("pcs", row);
//		row.clear();
//		row.put("pc_id", 3);
//		dbApp.deleteFromTable("pcs", row);
//		row.clear();
//		for(int i=11; i<=15; i++) {
//			row.put("pc_id", i);
//			dbApp.deleteFromTable("pcs", row);
//			row.clear();
//		}
//		row.put("pc_id", 10);
//		dbApp.deleteFromTable("pcs", row);
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
//		Hashtable<String, Object> colNameValue = new Hashtable<String, Object>();
//		
//		colNameValue.put("pc_id", 5);
//		//colNameValue.put("student_id", "55-5555");
//		dbApp.insertIntoTable("pcs", colNameValue);
//		colNameValue.clear();
//		
//		colNameValue.put("pc_id", 6);
//		//colNameValue.put("student_id", "55-5555");
//		dbApp.insertIntoTable("pcs", colNameValue);
//		colNameValue.clear();
//		
//		colNameValue.put("student_id", "55-5555");
////		colNameValue.put("test", "test");
//		dbApp.updateTable("pcs", "5", colNameValue);
//		
//		colNameValue.put("pc_id", 5);
////		colNameValue.put("test", "test");
//		dbApp.deleteFromTable("pcs", colNameValue);
//		colNameValue.clear();
//		
//		
//		
		//dbApp.testClearMetaDataFile();
		//dbApp.testDataDirectory();
		
//		dbApp.createPCsTable(dbApp);
		
//		Hashtable<String, Object> row = new Hashtable<String, Object>();
//		
//		//test ranges
//		row.put("pc_id", -1);
//		row.put("student_id", "42-6900");
//		dbApp.insertIntoTable("pcs", row);
//		row.clear();
//		
//		row.put("pc_id", -1);
//		row.put("student_id", "69-0420");
//		dbApp.insertIntoTable("pcs", row);
//		row.clear();
//		
//		row.put("pc_id", 5);
//		row.put("student_id", "42-6900");
//		dbApp.insertIntoTable("pcs", row);
//		row.clear();
//		
//		row.put("pc_id", 5);
//		row.put("student_id", "69-0420");
//		dbApp.insertIntoTable("pcs", row);
//		row.clear();
//		
//		row.put("pc_id", 0);
//		row.put("student_id", "43-0000");
//		dbApp.insertIntoTable("pcs", row);
//		row.clear();
//		
//		row.put("pc_id", 20000);
//		row.put("student_id", "99-9999");
//		dbApp.insertIntoTable("pcs", row);
//		row.clear();
//		
//		
//		//page 0
//		for(int i=1; i<=5; i++) {
//			row.put("pc_id", i);
//			row.put("student_id", "45-6900");
//			dbApp.insertIntoTable("pcs", row);
//			row.clear();
//			}
//		
//		//page 1
//		for(int i=6; i<=10; i++) {
//			row.put("pc_id", i);
//			row.put("student_id", "46-4994");
//			dbApp.insertIntoTable("pcs", row);
//			row.clear();
//		}
//		
//		//page 2
//		row.put("pc_id", 18);
//		row.put("student_id", "45-6900");
//		dbApp.insertIntoTable("pcs", row);
//		row.clear();
//				
//		//delete all of page 1
//		row.put("student_id", "46-4994");
//		dbApp.deleteFromTable("pcs", row);
//		row.clear();
//		
//		//test page number
//		for(int i=11; i<=15; i++) {
//			row.put("pc_id", i);
//			row.put("student_id", "69-4200");
//			dbApp.insertIntoTable("pcs", row);
//			row.clear();
//		}
//		row.put("pc_id", 16);
//		row.put("student_id", "69-4200");
//		dbApp.insertIntoTable("pcs", row);
//		row.clear();
//		
//		row.put("pc_id", 7);
//		row.put("student_id", "69-4200");
//		dbApp.insertIntoTable("pcs", row);
//		row.clear();
//		
//		row.put("pc_id", 24);
//		row.put("student_id", "69-4200");
//		dbApp.insertIntoTable("pcs", row);
//		row.clear();
//		row.put("pc_id", 48);
//		row.put("student_id", "69-4200");
//		dbApp.insertIntoTable("pcs", row);
//		row.clear();
//		row.put("pc_id", 93);
//		row.put("student_id", "69-4200");
//		dbApp.insertIntoTable("pcs", row);
//		row.clear();
//		row.put("pc_id", 23);
//		row.put("student_id", "69-4200");
//		dbApp.insertIntoTable("pcs", row);
//		row.clear();
//		
//		for(int i=0; i<50;) {
//			Random r = new Random();
//			int x = r.nextInt(100);
//			if(dbApp.tables.get("pcs").keys.containsKey(x+""))
//				continue;
//			row.put("pc_id", x);
//			row.put("student_id", "random");
//			dbApp.insertIntoTable("pcs", row);
//			row.clear();
//			i++;
//		}
//System.out.println("done");
		
//		// T E S T S
//        DBApp dbApp = new DBApp();
//        dbApp.init();
//        //create tables from old tests
//        dbApp.testClearMetaDataFile();
//       dbApp.testDataDirectory();
//       dbApp.createStudentTable(dbApp);
//       dbApp. createCoursesTable(dbApp);
//       dbApp.createTranscriptsTable(dbApp);
//       dbApp.createPCsTable(dbApp);
//       
//        //insertions
//        int limit = 500;
//        dbApp.insertStudentRecords(dbApp, limit);
//        dbApp.insertCoursesRecords(dbApp, limit);
//        dbApp.insertTranscriptsRecords(dbApp, limit);
//        dbApp.insertPCsRecords(dbApp, limit);
//        
//        //extra insertions
//        dbApp.testExtraStudentsInsertion();
//        dbApp.testExtraCoursesInsertion();
//        dbApp.testExtraTranscriptsInsertion();
//        dbApp.testExtraPCsInsertion();
//        
//        dbApp.testExtraTranscriptsInsertion();
//        dbApp.testUpdateStudentsExtra2();
//        
//        
//        
//        //update students
//        String table = "students";
//        Hashtable<String, Object> row = new Hashtable();
//        row.put("id", "82-8772");
//        row.put("first_name", "before");
//        row.put("middle_name", "before");
//        row.put("last_name", "before");
//        Date dob = new Date(2000, 5, 6);
//        row.put("dob", dob);
//        row.put("gpa", 2.0);
//        
//        dbApp.insertIntoTable(table, row);
//        dbApp.testUpdateStudents();
//        
//        
//        //update courses
//        table = "courses";
//        row = new Hashtable();
//        Date date_added = new Date(2000 - 1900, 4 - 1, 3);
//        row.put("date_added", date_added);
//        row.put("course_id", "before");
//        row.put("course_name", "before");
//        row.put("hours", 100);
//        row.put("semester", 50);
//        
//        dbApp.insertIntoTable(table, row);
//        dbApp.testUpdateCourses();
//
//        
//        //update transcripts
//        table = "transcripts";
//        row = new Hashtable();
//        row.put("gpa", 1.57);
//        row.put("student_id", "before");
//        row.put("course_name", "before");
//        row.put("elective", false);
//        Date date_passed = new Date(2011, 4, 1);
//        row.put("date_passed", date_passed);
//        
//        dbApp.insertIntoTable(table, row);
//        dbApp.testUpdateTranscripts();
//
//        
//        //update pcs
//        table = "pcs";
//        row = new Hashtable();
//        row.put("pc_id", 50);
//        row.put("student_id", "before");
//        row.put("room", "before");
//        
//		dbApp.insertIntoTable(table, row);
//        dbApp.testUpdatePCs();
//
//        
//        //update extra
//        dbApp.testUpdateStudentsExtra();
//        dbApp.testUpdateCoursesExtra();
//        dbApp.testUpdateTranscriptsExtra();
//        dbApp.testUpdatePCsExtra();
//        
//        
//        //deletes
//        dbApp.testStudentsDeletionComplex();
//        dbApp.testCoursesDeleteComplex();
//        dbApp.testTranscriptsDeleteComplex();
//        dbApp.testPCsDeleteComplex();
