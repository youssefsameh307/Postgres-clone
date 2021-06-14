import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;



public class Table implements Serializable{

    /**
	 * 
	 */
	private static final long serialVersionUID = -2628645096996933045L;
	String tableName;
    String clusteringKey;
    Hashtable<String, String> colNameType;
    Hashtable<String, String> colNameMin;
    Hashtable<String, String> colNameMax;
    
    
    Vector<Vector<Hashtable<String, Object>>> pages = new Vector<Vector<Hashtable<String, Object>>>(0,1);
    
    
	Hashtable<String, Object> col_name_min_value;
	Hashtable<String, Object> col_name_max_value;
	
	Hashtable<String, String> keys = new Hashtable<String, String>();	
    Vector<String> col_names = new Vector();
	int IdGenerator = 0;
    Vector<GridIndex> indices = new Vector<>();
    public String path;
  
    public Table(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax, boolean writable) throws DBAppException, IOException {
        this.tableName = tableName;
        this.clusteringKey = clusteringKey;
        this.colNameType = colNameType;
        this.colNameMax = colNameMax;
        this.colNameMin = colNameMin;
        Enumeration enu = colNameType.keys();
        this.path = "./src/main/resources/data/tables/"+tableName+".ser";
        while (enu.hasMoreElements()) {
            this.col_names.add((String)enu.nextElement());
        }
       
        
      if(writable)
        this.writeToMeta(true);
        
        try {
            this.set_table_properties_from_metadata_file();
//            for(String x: this.col_name_min_value.keySet()) {
//            	String type = getTypeCast(tableName, x);
//            	switch(type.toLowerCase()) {
//            	case "java.lang.double": this.defaultValues.put(x, 0.0);break;
//            	case "java.lang.string" : this.defaultValues.put(x, "null");break;
//                case "java.util.date": this.defaultValues.put(x, new Date(0-1900,1-1,1));break;
//                case "java.lang.integer" : this.defaultValues.put(x, 0);break;
//                default: throw new DBAppException("error in creating default values");
//            	}
//            }
        }catch(Exception e) {
        	if(e.getMessage() != null)
        		System.err.println(e.getMessage());
            //e.printStackTrace();
        }
        finally {

            if(this instanceof Table2){
                Table2 this_obj = (Table2)this;
                this_obj.contents = new Vector<>();
            }
            if(writable)
                 this.SerializeTable();
            
		}
    }

    
    public  int assertRangeC(Hashtable<String,Object> record) throws IOException, DBAppException{
        for (String key:record.keySet()){
        	if( !((record.get(key)).getClass().getCanonicalName()).equalsIgnoreCase(getTypeCast(this.tableName, key)) )
            	return -1;
        	Comparable keyc = (record.get(key) instanceof String)? (Comparable) record.get(key).toString().toLowerCase() : (Comparable) record.get(key);
        	Comparable maxc = (record.get(key) instanceof String)? (Comparable) col_name_max_value.get(key).toString().toLowerCase() : (Comparable) col_name_max_value.get(key);
        	Comparable minc = (record.get(key) instanceof String)? (Comparable) col_name_min_value.get(key).toString().toLowerCase() : (Comparable) col_name_min_value.get(key);
        	if(keyc.compareTo(minc)<0)
                return 0;
            if(keyc.compareTo(maxc)>0)
                return 0;
            
            	
        }
        return 1;
    }
    
//    private ArrayList<Hashtable<String, Object>> LinearSearch(Hashtable<String, Object> search_criteria) {
//  		ArrayList<Hashtable<String, Object>> result_set = new ArrayList<Hashtable<String,Object>>();
//  		ArrayList<String> criteria = new ArrayList<String>();
//  		Enumeration enu = search_criteria.keys();
//  		while (enu.hasMoreElements()) {criteria.add((String) enu.nextElement());}
//  		for(Hashtable<String, Object> element : this.pages){
//  			String path_to_page = (String) element.get("path");
//  			Page p = this.loadPage(path_to_page);
//  			
//  			for( Hashtable<String, Object> row :p.content){
//  				boolean matches_search_criteria = true;
//  				for (String key : criteria) {
//  					boolean accepted = row.get(key).equals(search_criteria.get(key));
//  					if(!accepted){
//  						matches_search_criteria = false;
//  					}
//  				}
//  				
//  				if(matches_search_criteria){
//  					result_set.add(row);
//  				}
//  			}
//  		}
//  		return result_set;
//  	}
//
//    private ArrayList<Page> LinearSearchPages(Hashtable<String, Object> search_criteria) {
//    	ArrayList<Page> result_set = new ArrayList<Page>();
//  		ArrayList<String> criteria = new ArrayList<String>();
//  		Enumeration enu = search_criteria.keys();
//  		while (enu.hasMoreElements()) {criteria.add((String) enu.nextElement());}
//  		for(Hashtable<String, Object> element : this.pages){
//  			String path_to_page = (String) element.get("path");
//  			Page p = this.loadPage(path_to_page);
//  			
//  			for( Hashtable<String, Object> row :p.content){
//  				boolean matches_search_criteria = true;
//  				for (String key : criteria) {
//  					boolean accepted = row.get(key).equals(search_criteria.get(key));
//  					if(!accepted){
//  						matches_search_criteria = false;
//  					}
//  				}
//  				
//  				if(matches_search_criteria){
//  					result_set.add(p);
//  					break;
//  				}
//  			}
//  		}
//  		return result_set;
//  	}
 
    public int search(Comparable id) throws IOException {
        int size = pages.size();
        int lowIndex = 0;
        int highIndex=size-1;
        int elementpos=0;

        while (lowIndex <= highIndex) {

            int midIndex = (lowIndex + highIndex) / 2;
            Hashtable<String, Object> current = pages.get(midIndex).firstElement();
            Comparable currentmin = (Comparable) current.get("min");
            Comparable currentmax = (Comparable) current.get("max");
            Comparable nextpagemin=0;
            Comparable prevpagemax=0;
            if(midIndex!=size-1)
                nextpagemin =(Comparable) pages.get(midIndex+1).firstElement().get("min");
            if(midIndex!=0)
                prevpagemax = (Comparable) pages.get(midIndex-1).firstElement().get("max");


            if (currentmin.compareTo(id)<=0 && currentmax.compareTo(id)>=0) {
                elementpos = midIndex;
                return elementpos;
            }
            if(midIndex!=0&&currentmin.compareTo(id)>=0&&prevpagemax.compareTo(id)<=0){
            	
            	if(!Page.DeserializePage(pages.get(midIndex-1).firstElement().get("path").toString(), this).isFull())
                    elementpos=midIndex-1;
                else
                    elementpos = midIndex;
                return elementpos;
            }
            if(midIndex!=size-1&&currentmax.compareTo(id)<=0&&nextpagemin.compareTo(id)>=0){
            	if(!Page.DeserializePage(pages.get(midIndex).firstElement().get("path").toString(), this).isFull())
                    elementpos = midIndex;
                else
                    elementpos = midIndex+1;
                return elementpos;
            }
            else if (currentmin.compareTo(id)>0) { 
                highIndex = midIndex-1;
            } else if (currentmax.compareTo(id)<0) { 
                lowIndex = midIndex+1;
            }
        }
        if(highIndex<0)
            return -1;
        else
            return -2;
    }

//     public boolean insert(Hashtable<String,Object> record) throws IOException, DBAppException {
//     	if(assertRangeC(record) == -1)
//         	throw new DBAppException("incompatible data type");
//     	if(assertRangeC(record) == 0)
//         	throw new DBAppException("data out of range");
// //        Hashtable<String,Object> merged = Page.merge(this.defaultValues, record);
//     	try {
//             Comparable key = (Comparable) (record.get(this.clusteringKey));
//             if(pages.isEmpty()){
//                 pages.add(new Vector<Hashtable<String, Object>>(0,1));
//                 Page newpage = new Page(record,this);
//                 // update indecies
//                 updateIndecies(record, newpage, tableOperations.INSERT);
//                 SerializeTable();
//                 return true;
//             }
//             int pagenumber = search(key);
//             if (pagenumber == -1) {
//                 Page page_to_ne_inserted_to = Page.DeserializePage(pages.firstElement().firstElement().get("path").toString(), this);
//                 page_to_ne_inserted_to.insert(record);
//                 // update indecies
//                 updateIndecies(record, page_to_ne_inserted_to, tableOperations.INSERT);
//                 SerializeTable();
//                 return true;
//             }
//             if (pagenumber == -2) {
//                 Page lastpage = Page.DeserializePage(pages.lastElement().firstElement().get("path").toString(), this);
//                 if (lastpage.isFull()) {
//                     pages.add(new Vector<Hashtable<String, Object>>(0,1));
//                     Page next_page = new Page(record,this);
//                     lastpage.nextpage = next_page;
//                     //update indecies
//                     updateIndecies(record, next_page, tableOperations.INSERT);
//                     SerializeTable();
//                     return true;
//                 } else
//                     lastpage.insert(record);
//                     //update indecies
//                     updateIndecies(record, lastpage, tableOperations.INSERT);
//                 SerializeTable();
//                 return true;
//             }
//             Page p = Page.DeserializePage(pages.get(pagenumber).firstElement().get("path").toString(), this);
//             p.insert(record);
//             // update indecies here only
//             updateIndecies(record, p, tableOperations.INSERT);
//             try {
// 				pages.get(pagenumber).get(1);
// 				updateMinMax(p, pagenumber, null);
// 			} catch (ArrayIndexOutOfBoundsException e) {
				
// 			} finally {
// 				SerializeTable();
// 	            return true;
// 			}
            
//         }
//         catch (Exception e){
//             throw e;
//             // System.err.println("error has occurred while inserting");
//             // return false;
            
//         }
//     }
    
    public void updateIndecies(Hashtable<String, Object> record, Page p, tableOperations operation) throws DBAppException, IOException {
        switch(operation){
            case INSERT: {
                for (GridIndex g : this.indices) {
                    g.insert(record, p); // insert in all indecies
                }
                break;
            }
            case DELETE : {
                for (GridIndex g : this.indices) {
                    g.delete(record, p); // insert in all indecies
                }
                break;
            }
            case UPDATE: {
                for (GridIndex g : this.indices) {
                    g.update(record, p); // insert in all indecies
                }
                break;
            }
        }
    }


    // public void show(Comparable key) throws IOException, DBAppException {
    //     int pagenum = search(key);
    //     if (pagenum < 0){
    //         System.err.println("not found");
    //     }
    //     else{
    //         Page.DeserializePage(pages.get(pagenum).firstElement().get("path").toString(), this).show(key);
    //     }
    // }

    // public boolean update(Comparable key, Hashtable<String,Object> record) throws IOException, DBAppException {
    // 	if(assertRangeC(record) == -1)
    //     	throw new DBAppException("incompatible data type");
    // 	if(assertRangeC(record) == 0)
    //     	throw new DBAppException("data out of range");
    // 	int pagenum = search(key);
    //     if (pagenum < 0){
    //         System.err.println("not found");
    //         return false;
    //     }
    //     else{
    //     	Page.DeserializePage(pages.get(pagenum).firstElement().get("path").toString(), this).update(key, record);
    //         SerializeTable();
    //         return true;
    //     }
    // }
    
    // public void updateMinMax(Page p, int position, Comparable deleted) throws IOException, DBAppException {
    // 	Vector<Hashtable<String, Object>> x = this.pages.get(position);
    // 	Comparable temp = p.getmin();
    // 	p.setmin(p.getmax());
    // 	p.setmax(temp);
    // 	//p.setmax((Comparable) this.col_name_min_value.get(this.clusteringKey));
    // 	//p.setmin((Comparable) this.col_name_max_value.get(this.clusteringKey));
    // 	boolean skip = true;
    // 	for(Hashtable<String, Object> h: x) {
    // 		Comparable currentMin = (Comparable) h.get("min");
    // 		Comparable currentMax = (Comparable) h.get("max");
    // 		if(!currentMin.equals(deleted) && p.getmin().compareTo(currentMin)>0)
    // 			p.setmin(currentMin);
    //     	if(!currentMax.equals(deleted) && p.getmax().compareTo(currentMax)<0)
    //     		p.setmax(currentMax);
    // 	}
    // 	p.updatePage();
    // }
    
    // public void updateOverflow(Page original, int pagenum, int emptyP) throws IOException, DBAppException {
    // 	int lastposition = emptyP;
	// 	try {
	// 		while(true) {
	// 			Page p = Page.DeserializePage(pages.get(pagenum).get(emptyP+1).get("path").toString(), this);
	// 			lastposition = p.overflow--;
	// 			p.updatePage();
	// 			emptyP++;
	// 		}
	// 	} catch(ArrayIndexOutOfBoundsException e) {
    //     	this.pages.get(pagenum).remove(lastposition);
    		 
	// 		File del2 = new File("./src/main/resources/data/"+this.tableName+"_"+original.ID+"_O"+lastposition+".ser");
    //     	Files.delete(del2.toPath());
	// 	}
    // }
    // 
    
    public void updateOriginal(int pagenum) throws IOException {
        new File(this.pages.get(pagenum).get(0).get("path").toString()).delete();
        this.pages.remove(pagenum);
       // this.pages.insertElementAt(null, pagenum);
    }
    
    
    // public boolean delete(Comparable key) throws IOException, DBAppException { // TODO: JOE ---->>> where do u delete so that i can dlelete from buckets ya basha 
    	
    //     int pagenum = search(key);
    //     if (pagenum < 0){
    //         System.err.println("not found");
    //         return false;
    //     }
    //     else{
    //     	boolean updated = false;
    //     	boolean isoriginal = true;
    //     	boolean empty = false;
    //     	int emptyP = -1;
    // 		Page original = Page.DeserializePage(this.pages.get(pagenum).firstElement().get("path").toString(), this);
    //         Hashtable<String,Object> record =original.get(original.search(key));
    //         updateIndecies(record, original, tableOperations.DELETE);
    //     	for(Hashtable<String, Object> x: this.pages.get(pagenum)) {
    //     		emptyP++;
    //     		Page p;
    //     		if(isoriginal) {
    //     			p = original;
    //     			isoriginal = false;
    //     		}
    //     		else {
    //     			p = Page.DeserializePage(x.get("path").toString(), this);
    //     		}
    //     		Boolean del = p.delete(key); // delete page
                
    //     		if(del && !original.isEmpty()) {
    //     			p.updatePage();
    // 				updateMinMax(original, pagenum, key);
    //     		}
    //     		if(del && original.isEmpty() && !updated) {
    // 				updateMinMax(p, pagenum, key);
    // 				updated = true;
    //     		}
    //     		if(del) {
    //     			if(p.isEmpty() && p != original)
    //     				empty = true;
    //     			break;
    //     		}
        			
    //     		//Page.DeserializePage(pages.get(pagenum).firstElement().get("path").toString(), this).delete(key);
    //     	}
    //     	if(empty) {
    // 			updateOverflow(original, pagenum, emptyP);
    //     	}
        	
        	
    //     	Page x = Page.DeserializePage(pages.get(pagenum).firstElement().get("path").toString(), this);
    //         if(pages.size()>0 && x.isEmpty()) {
    //         	try {
    //         		Page p = Page.DeserializePage(pages.get(pagenum).get(1).get("path").toString(), this);
    //         		updateMinMax(p, pagenum, key);
    //         		updateOverflow(x, pagenum, 0);
    //         	} catch(ArrayIndexOutOfBoundsException e) {
    //             	updateOriginal(pagenum);
    //         	}
    //         }
           
    //        // primary_key.put(this.clusteringKey, key);
            
    //         SerializeTable();
    //         return true;
    //     }


    // }
   
    
    public static ArrayList<String> readFromMeta() {
        try {
            File F = new File("./src/main/resources/metadata.csv");
            FileReader FR = new FileReader(F);
            BufferedReader BR = new BufferedReader(FR);

            ArrayList<String> previousDetails = new ArrayList<>();
            while(BR.ready())
                previousDetails.add(BR.readLine());
            BR.close();
            return previousDetails;
        } catch (Exception e) {
            System.err.println("Error in reading from metadata.csv");
            return null;
        }
    }

    public void writeToMeta(boolean keepOld) {
        ArrayList<String> oldData = readFromMeta();

        try {
            File F = new File("./src/main/resources/metadata.csv");
            FileWriter FW = new FileWriter(F);
            BufferedWriter BW = new BufferedWriter(FW);

            String[][] columnTypes = new String[this.colNameType.size()][2];
            String[][] columnMinimum = new String[this.colNameType.size()][2];
            String[][] columnMaximum = new String[this.colNameType.size()][2];

            int j=0;
            for(Entry<String, String> x: this.colNameType.entrySet()) {
                columnTypes[j][0] = x.toString().split("=")[0];
                columnTypes[j][1] = x.toString().split("=")[1];
                j++;
            }

            j=0;
            for(Entry<String, String> x: this.colNameMin.entrySet()) {
                columnMinimum[j][0] = x.toString().split("=")[0];
                columnMinimum[j][1] = x.toString().split("=")[1];
                j++;
            }

            j=0;
            for(Entry<String, String> x: this.colNameMax.entrySet()) {
                columnMaximum[j][0] = x.toString().split("=")[0];
                columnMaximum[j][1] = x.toString().split("=")[1];
                j++;
            }

            if(keepOld) {
                for(String x: oldData) {
                    BW.write(x);
                    BW.write("\r\n");
                }
            }

            for(int i=0; i<this.colNameType.size(); i++) {
                String tableDetails = this.tableName + ", " + columnTypes[i][0] + ", " +
                        columnTypes[i][1] + ", " + this.clusteringKey.equals(columnTypes[i][0]) + ", " +
                        "false, " + columnMinimum[i][1] + ", " + columnMaximum[i][1];
                BW.write(tableDetails);
                BW.write((i == this.colNameType.size() - 1)? "":"\r\n");
            }
            BW.write("\r\n");
            BW.close();
        } catch (Exception e) {
            System.err.println("Error in writing to metadata.csv");
        }
    }
    
    public static ArrayList<String> getTableNamesFromMeta() throws DBAppException, IOException{
    	ArrayList<String> names = new ArrayList<String>();
    	ArrayList<Table> tables = loadTablesFromMeta();
    	for(Table t: tables)
    		names.add(t.tableName);
    	return names;
    }

    public static ArrayList<Table> loadTablesFromMeta() throws DBAppException, IOException {
        ArrayList<Table> tables = new ArrayList<Table>();
        ArrayList<String[]> tablesAsString = new ArrayList<String[]>();

        for(String x:readFromMeta())
            tablesAsString.add(x.split(", "));

        Hashtable htblColNameType = new Hashtable ( );
        Hashtable htblColNameMin = new Hashtable ( );
        Hashtable htblColNameMax = new Hashtable ( );
        String clusterKey = "";

        for(int i=0; i<tablesAsString.size()-1; i++) {
            if(tablesAsString.get(i)[0].equals(tablesAsString.get(i+1)[0])) {
                htblColNameType.put(tablesAsString.get(i)[1], tablesAsString.get(i)[2]);
                htblColNameMin.put(tablesAsString.get(i)[1], tablesAsString.get(i)[4]);
                htblColNameMax.put(tablesAsString.get(i)[1], tablesAsString.get(i)[5]);

                if(Boolean.valueOf(tablesAsString.get(i)[3]))
                    clusterKey = tablesAsString.get(i)[1];
            }
            else {
                htblColNameType.put(tablesAsString.get(i)[1], tablesAsString.get(i)[2]);
                htblColNameMin.put(tablesAsString.get(i)[1], tablesAsString.get(i)[4]);
                htblColNameMax.put(tablesAsString.get(i)[1], tablesAsString.get(i)[5]);

                if(Boolean.valueOf(tablesAsString.get(i)[3]))
                    clusterKey = tablesAsString.get(i)[1];

                tables.add(new Table(tablesAsString.get(i)[0], clusterKey,
                        (Hashtable<String, String>) htblColNameType.clone(),
                        (Hashtable<String, String>) htblColNameMin.clone(),
                        (Hashtable<String, String>) htblColNameMax.clone(), false));

                htblColNameType.clear();
                htblColNameMin.clear();
                htblColNameMax.clear();
                clusterKey = "";
            }
        }

        htblColNameType.put(tablesAsString.get(tablesAsString.size()-1)[1], tablesAsString.get(tablesAsString.size()-1)[2]);
        htblColNameMin.put(tablesAsString.get(tablesAsString.size()-1)[1], tablesAsString.get(tablesAsString.size()-1)[4]);
        htblColNameMax.put(tablesAsString.get(tablesAsString.size()-1)[1], tablesAsString.get(tablesAsString.size()-1)[5]);

        if(Boolean.valueOf(tablesAsString.get(tablesAsString.size()-1)[3]))
            clusterKey = tablesAsString.get(tablesAsString.size()-1)[1];

        tables.add(new Table(tablesAsString.get(tablesAsString.size()-1)[0], clusterKey,
                (Hashtable<String, String>) htblColNameType.clone(),
                (Hashtable<String, String>) htblColNameMin.clone(),
                (Hashtable<String, String>) htblColNameMax.clone(), false));

        htblColNameType.clear();
        htblColNameMin.clear();
        htblColNameMax.clear();
        clusterKey = "";

        return tables;
    }


    public void validateMetaDataFile() throws MetaDataException, IOException {
        File F = new File("./src/main/resources/metadata.csv");
        FileReader FR = new FileReader(F);
        BufferedReader BR = new BufferedReader(FR);
        ArrayList<String> data = (ArrayList<String>) Files.readAllLines(Paths.get("./src/main/resources/metadata.csv"));
        BR.close();

        Hashtable<String, String> htbl_colname_type = new Hashtable<>();
        Hashtable<String, String> htbl_colname_min = new Hashtable<>();
        Hashtable<String, String> htbl_colname_max = new Hashtable<>();
        for (String x : data) {
            String[] properties = x.split(",");
            
            if (!properties[0].trim().equals(this.tableName))
                continue;
            String col_name = properties[1].trim();
            String col_type = properties[2].trim();
            String col_min = properties[5].trim();
            String col_max = properties[6].trim();
            htbl_colname_type.put(col_name, col_type);
            htbl_colname_min.put(col_name, col_min);
            htbl_colname_max.put(col_name, col_max);

            if (col_min.compareTo(col_max) > 0) {
                throw new MetaDataException("corrupt MetaData File : min is bigger than maximum");
            }
            try {
                Double d = new Double(18);
                Class t = d.getClass();
                System.out.println(t.getName());
                System.out.println(col_type);
                System.out.println(t.getName().equals(col_type));
                Class.forName(t.getName());
                Class.forName(col_type);

            } catch (ClassNotFoundException e) {
                //e.printStackTrace();
            	if(e.getMessage() != null)
            		System.err.println(e.getMessage());
                throw new MetaDataException("corrupt MetaData File : "+ col_name+ " " + col_type + " class is not found" );


            }

        }
    }

    public void set_table_properties_from_metadata_file() throws IOException, ClassNotFoundException, DBAppException, ParseException {
        File F = new File("./src/main/resources/metadata.csv");
        FileReader FR = new FileReader(F);
        BufferedReader BR = new BufferedReader(FR);
        ArrayList<String> data = (ArrayList<String>) Files.readAllLines(Paths.get("./src/main/resources/metadata.csv"));
        BR.close();

        Hashtable<String, String> htbl_colname_type = new Hashtable<>();
        Hashtable<String, Object> htbl_colname_min = new Hashtable<>();
        Hashtable<String, Object> htbl_colname_max = new Hashtable<>();
        for (String x : data) {
            String[] properties = x.split(",");
            
            if (!properties[0].equals(this.tableName)) {
                continue;
            }

            String col_name = properties[1].trim();
            String col_type = properties[2].trim();
            Object col_min = get_parsed_value(properties[5].trim(), col_type);
            Object col_max = get_parsed_value(properties[6].trim(), col_type);
            htbl_colname_type.put(col_name, col_type);
            htbl_colname_min.put(col_name, col_min);
            htbl_colname_max.put(col_name, col_max);
        }
        this.colNameType = htbl_colname_type;
        this.col_name_min_value = htbl_colname_min;
        this.col_name_max_value = htbl_colname_max;
        
    }
    public static Object get_parsed_value(String string, String col_type) throws DBAppException, ParseException {
    	switch(col_type.trim().toLowerCase()) {
            case "java.lang.double": return Double.parseDouble(string);
            case "java.lang.string" : return string;
            case "java.util.date":return new SimpleDateFormat("yyyy-MM-dd").parse(string);  
            case "java.lang.integer" :return Integer.parseInt(string);
            default : throw new DBAppException("unrecognized class " + col_type);
        }

    }

    private boolean verify_class(Object value, String string) {
        System.out.println(string);
        System.out.println(value.getClass());
        Object temp ;
        switch(string.trim().toLowerCase()) {
            case "java.lang.double": temp = (double) value;break;
            case "java.lang.integer":temp= (int) value;break;
            case "java.lang.date": temp = (Date) value;break;
            case "java.lang.String": temp =  (String) value;break;
        }
        return true;
    }

    public void validate_input(Hashtable<String, Object> htblColNameValue) throws DBAppException {
      
        // 1) cols exist in table 
        for (String key_name : htblColNameValue.keySet()) {
            if(this.colNameType.get(key_name)==null){
                throw new DBAppException("sorry column " + key_name + " does not exist");
            }

        }
        // 2) there exists a clustering key 
        Object temp = htblColNameValue.get(this.clusteringKey);
        if(temp==null){
            throw new DBAppException("can not insert a record withour primary key ");
        }
    }
    
    public static String getTypeCast(String tableName, String Key) throws IOException, DBAppException  {
        File F = new File("./src/main/resources/metadata.csv");
        FileReader FR = new FileReader(F);
        BufferedReader BR = new BufferedReader(FR);
        ArrayList<String> data = (ArrayList<String>) Files.readAllLines(Paths.get("./src/main/resources/metadata.csv"));
        BR.close();
        
        
        for(String x: data) {
            if(x.split(",")[0].equals(tableName) && x.split(",")[1].substring(1, x.split(",")[1].length()).equals(Key))
                return x.split(",")[2].substring(1, x.split(",")[2].length());
        }
        throw new DBAppException("something wrong in the data type or column does not exist");
    }
    
    public void SerializeTable() throws DBAppException, IOException {
        File directory_createor = new File(path);
        if (directory_createor.getParentFile().mkdirs() || directory_createor.getParentFile().exists()) {
            directory_createor.createNewFile();
        } else {
            throw new DBAppException("Failed to create directory for the tables " + directory_createor.getParent());
        }
            FileOutputStream file = new FileOutputStream(path);
            ObjectOutputStream out = new ObjectOutputStream(file);
            out.writeObject(this);
            out.close();
            file.close();

        
    }
    
    public static Table DeserializeTable(String path) throws IOException {

        Table table = null;


            FileInputStream file = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(file);

            try {
                table = (Table)in.readObject();
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            in.close();
            file.close();

            System.out.println("Table deserialized!");

        

        return table;
    }

    
    
    public static void main(String[] args) throws Exception {
        test_result_sets();
    }
    private static void test_result_sets() throws DBAppException, IOException {
                // rest everything a a test method i created
                String strTableName = "Student";

                Hashtable<String,String> htblColNameType = new Hashtable<String,String> ( );
                htblColNameType.put("id", "java.lang.Integer");
                htblColNameType.put("name", "java.lang.String");
                htblColNameType.put("gpa", "java.lang.Double");
                htblColNameType.put("grade", "java.lang.String");
          
                Hashtable<String,String> htblColNameMin = new Hashtable<String,String> ( );
                htblColNameMin.put("id", "0");
                htblColNameMin.put("name", "A");
                htblColNameMin.put("gpa", "0.0");
                htblColNameMin.put("grade", "A");

                Hashtable<String,String> htblColNameMax = new Hashtable<String,String> ( );
                htblColNameMax.put("id", "100");
                htblColNameMax.put("name", "ZZZZZZZZZZZ");
                htblColNameMax.put("gpa", "5.0");
                htblColNameMax.put("grade", "F");

          
                  Table2 t = new Table2( strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax, true);
                  String[] cols_to_create_index_on = new String[]{"id", "gpa"};
               
                  double number_of_stundets = 40;
                  
                  for (int i = 0; i < 10; i++) {
                      Hashtable htblColNameValue = new Hashtable();
                      htblColNameValue.put("id", new Integer(i));
                      htblColNameValue.put("name", new String("Ahmed Noor " + i));
                      Double gpa = (Double)((i/number_of_stundets) * 5.0);
                      htblColNameValue.put("gpa", new Double(gpa) );
                      Random rand = new Random();
                      List asList = Arrays.asList("A", "B", "C", "E'", "F");
                      asList.get(0);
                      htblColNameValue.put("grade", asList.get(rand.nextInt(5)));
                      t.insert(htblColNameValue);
                  }
                  for (int i = 20; i < 40; i++) {
                    Hashtable htblColNameValue = new Hashtable();
                    htblColNameValue.put("id", new Integer(i));
                    htblColNameValue.put("name", new String("Ahmed Noor " + i));
                    Double gpa = (Double)((i/number_of_stundets) * 5.0);
                    htblColNameValue.put("gpa", new Double(gpa) );
                    Random rand = new Random();
                    List asList = Arrays.asList("A", "B", "C", "E'", "F");
                    asList.get(0);
                    htblColNameValue.put("grade", asList.get(rand.nextInt(5)));
                    t.insert(htblColNameValue);
                }
                for(int i = 0;i<=9;i++){
                    t.delete(i);
                }
                for (int i = 10; i < 20; i++) {
                    Hashtable htblColNameValue = new Hashtable();
                    htblColNameValue.put("id", new Integer(i));
                    htblColNameValue.put("name", new String("Ahmed Noor " + i));
                    Double gpa = (Double)(((i-10)/number_of_stundets) * 5.0);
                    htblColNameValue.put("gpa", new Double(gpa) );
                    Random rand = new Random();
                    List asList = Arrays.asList("A", "B", "C", "E'", "F");
                    asList.get(0);
                    htblColNameValue.put("grade", asList.get(rand.nextInt(5)));
                    t.insert(htblColNameValue);
                }
              

                  t.createIndex(cols_to_create_index_on);
                  cols_to_create_index_on = new String[]{"id", "name"};
                  t.createIndex(cols_to_create_index_on);
                  cols_to_create_index_on = new String[]{"name"};
                  t.createIndex(cols_to_create_index_on);
                  cols_to_create_index_on = new String[]{"grade", "name"};
                  t.createIndex(cols_to_create_index_on);
                  cols_to_create_index_on = new String[]{"grade", "name", "id"};
                  t.createIndex(cols_to_create_index_on);
                 GridIndex g= t.indices.get(0); // get the only created indecies

                


                  
        
          
           
            //table creation
                System.out.println(t);
                //   update 15, 16, 17
                for (int i = 15; i < 16; i++) {
                    Hashtable htblColNameValue = new Hashtable();
                    htblColNameValue.put("id", new Integer(i));
                    htblColNameValue.put("name", new String("i am updated type of thing " + i));
                    Double gpa = (Double)(((i-10)/number_of_stundets) * 5.0);
                    htblColNameValue.put("gpa", new Double(gpa) );
                    Random rand = new Random();
                    List asList = Arrays.asList("A", "B", "C", "E'", "F");
                    asList.get(0);
                    htblColNameValue.put("grade", asList.get(rand.nextInt(5)));
                    t.insert(htblColNameValue);
                    t.update(i, htblColNameValue);
                }
                System.out.println(t);
                System.out.println("==============================================");
                SQLTerm[] arrSQLTerms = new SQLTerm[3];
                arrSQLTerms[0] = new SQLTerm();
                arrSQLTerms[1] = new SQLTerm();
                arrSQLTerms[2] = new SQLTerm();

                arrSQLTerms[0]._strTableName  = "Student";
                arrSQLTerms[0]._strColumnName= "name";
                arrSQLTerms[0]._objValue = "Ahmed Noor 97";
                arrSQLTerms[0]._strOperator ="!=";
                        
                arrSQLTerms[1]._strTableName = "Student";
                arrSQLTerms[1]._strColumnName="gpa";
                arrSQLTerms[1]._objValue = new Double( 1 );;
                arrSQLTerms[1]._strOperator ="<";// X

                arrSQLTerms[2]._strTableName = "Student";
                arrSQLTerms[2]._strColumnName="gpa";
                arrSQLTerms[2]._objValue = new Double( 1 );;
                arrSQLTerms[2]._strOperator ="<";// X

                String[] strarrOperators = new String[1];
                strarrOperators[0] = "AND";
                Vector<Hashtable<String, Object>> records = t.selectFromTable(arrSQLTerms, strarrOperators);
                List<Integer> recordList = new ArrayList<>();
                for (Hashtable<String, Object> record : records) {
                    recordList.add((int)record.get("id"));
                    System.out.println(record);
                }
                Collections.sort(recordList);

                for (int record : recordList) {
                    System.out.println(record);
                }

            
            

    }
            


    private Vector<String> get_result_set(Vector<SQLTerm> v) throws DBAppException, IOException {
        GridIndex optimal_index_to_be_used = get_optimal_index_to_be_used(v);
        Vector<String> path_to_pages_that_should_be_considered = optimal_index_to_be_used.get_result_set(v);
        return path_to_pages_that_should_be_considered;
    }

    //Karim
    private GridIndex get_optimal_index_to_be_used(Vector<SQLTerm> v) {
        Vector<GridIndex> tableIndecies = this.indices;
        String[] termColNames = v.stream().map(term -> term._strColumnName).toArray(String[]::new);
        for(GridIndex index:tableIndecies){
            Boolean flag = true;
            for(String IndexColName:index.column_names){
                if(!Arrays.stream(termColNames).anyMatch(termcolname->termcolname.equals(IndexColName))){
                    continue;
                }
            }
            return index;
        }
        return null;
    }


   


    private GridIndex get_optimal_index_to_be_used(String colum_name, String operator, String value) {
        return this.indices.get(0); // TODO:JOE
    }


    private static void test_buckets() throws IOException, DBAppException {
        // rest everything a a test method i created
        String strTableName = "Student";

      Hashtable<String,String> htblColNameType = new Hashtable<String,String> ( );
      htblColNameType.put("id", "java.lang.Integer");
      htblColNameType.put("name", "java.lang.String");
      htblColNameType.put("gpa", "java.lang.Double");

      Hashtable<String,String> htblColNameMin = new Hashtable<String,String> ( );
      htblColNameMin.put("id", "0");
      htblColNameMin.put("name", "A");
      htblColNameMin.put("gpa", "0.0");

      Hashtable<String,String> htblColNameMax = new Hashtable<String,String> ( );
      htblColNameMax.put("id", "100");
      htblColNameMax.put("name", "ZZZZZZZZZZZ");
      htblColNameMax.put("gpa", "5.0");

        Table2 t = new Table2( strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax, true);
        String[] cols_to_create_index_on = new String[]{"id", "gpa"};
        t.createIndex(cols_to_create_index_on);
        GridIndex g= t.indices.get(0); // get the only created indecies
        double number_of_stundets = 100.0;
        
        for (int i = 0; i < number_of_stundets; i++) {
			Hashtable htblColNameValue = new Hashtable();
			htblColNameValue.put("id", new Integer(i));
			htblColNameValue.put("name", new String("Ahmed Noor " + i));
            Double gpa = (Double)((i/number_of_stundets) * 5.0);
			htblColNameValue.put("gpa", new Double(gpa) );

			t.insert(htblColNameValue);
		}
        System.out.println(t.toString());
        System.out.println("YOU SHOULD GET A UNIFORM DISTRIBUTION CUZ ALL STUDENTS ARE AT THE DIAGONAL");
        System.out.println(t.indices.get(0));


       
    }
    public Vector<Hashtable<String, Object>> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException, IOException{
        // for every pages scann it and store records that succed all the assSQLTerms
        Vector<Hashtable<String, Object>> results = new Vector<>();
        switch(strarrOperators[0]){
            case "AND": {
                Vector<String> pages_to_consider = get_and_of_result_set(arrSQLTerms);// TODO:JOe
                if(pages_to_consider!=null){
                for (String path : pages_to_consider) {
                    try{
                    Page page = Page.DeserializePage(path, this);
                    Vector<Hashtable<String, Object>> records_in_page  = page.get_all_records(); // TODO:JOE
                    results.addAll(filter(records_in_page, arrSQLTerms, strarrOperators));
                    }catch(IOException e){
                        System.out.println("tried to load " + path);
                        continue;
                    }

                }
            }else{
                results=filter(getAllRecordsOfTable(),arrSQLTerms,strarrOperators);
            }
                break;
            }
            case "OR": {
                Vector<String> pages_to_consider = get_or_of_result_set(arrSQLTerms);
                for (String path : pages_to_consider) {
                    try{
                    Page page = Page.DeserializePage(path, this);
                    Vector<Hashtable<String, Object>> records_in_page  = page.get_all_records();
                    for (Hashtable<String,Object> record : records_in_page) {
                        for (SQLTerm sqlTerm : arrSQLTerms) {
                            if(record_satifies_sqlTerm(sqlTerm, record)){
                                // if it satsfies 1 of them just add it to result set
                                results.add(record);
                                break;
                            }
                        }
                    }
                }catch(IOException e){
                    System.out.println("tried to load " + path);
                    continue;
                }
                }
            break;
            }
            case "XOR" : {
                Vector<String> pages_to_consider = get_xor_of_result_set(arrSQLTerms); // TODO: JOE
                if(pages_to_consider!=null){
                for (String path : pages_to_consider) {
                    try{
                    Page page = Page.DeserializePage(path, this);
                    Vector<Hashtable<String, Object>> records_in_page  = page.get_all_records();//TODO:JOE
                    for (Hashtable<String,Object> record : records_in_page) {
                        for (SQLTerm sqlTerm : arrSQLTerms) {
                            if(record_satifies_sqlTerm(sqlTerm, record)){
                                // if it satsfies 1 of them just add it to result set
                                results.add(record);
                                break;
                            }
                        }
                    }
                
                }catch(IOException e){
                    System.out.println("tried to load " + path);
                    continue;
                }
                }
            } 
            else{
                results=filter(getAllRecordsOfTable(),arrSQLTerms,strarrOperators);
            }
            
                // here fillter all results that satisfy more than 1 SQLTerm 
                // TODO: XOR all the results 
            }
        }
        return results;
    }

    Vector<Hashtable<String, Object>> filter(Vector<Hashtable<String, Object>> records_in_page,
            SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
            Vector<Hashtable<String, Object>> res = new Vector<>();
            switch(strarrOperators[0]){
                case "AND":{
                    for (Hashtable<String,Object> record : records_in_page) {
                        boolean flag_failed_to_satisfy = false;
                        for (SQLTerm sqlTerm : arrSQLTerms) {
                            if(!record_satifies_sqlTerm(sqlTerm, record)){
                                flag_failed_to_satisfy = true;
                                break;
                            }
                        }
                        if(flag_failed_to_satisfy){
                            continue;
                        }else{
                            res.add(record);
                        }
                    }
                    break;
                }
                case "OR": {
                    for (Hashtable<String,Object> record : records_in_page) {
                        for (SQLTerm sqlTerm : arrSQLTerms) {
                            if(record_satifies_sqlTerm(sqlTerm, record)){
                                res.add(record);
                                break;
                            }
                        }
                    }
                    break;
                }
                case "XOR":{
                    for (Hashtable<String,Object> record : records_in_page) {
                        boolean alternating_flag = false;
                       // int satisfied_sqlTerm  = 0;
                        for (SQLTerm sqlTerm : arrSQLTerms) {
                            if(record_satifies_sqlTerm(sqlTerm, record)){
                                alternating_flag = !alternating_flag;
                            }
                        }
                        if(alternating_flag)
                            res.add(record);
                    }
                    break;
                }
                default: throw new DBAppException("unknow operator on SQL Terms : " + strarrOperators[0]);
            }
            return res;
    }


    public static Vector<Vector<SQLTerm>> getSubsets(SQLTerm[] set)
    {
        int n = set.length;
        Vector<Vector<SQLTerm>> permutation=new Vector<>();
        for (int i = 0; i < (1<<n); i++)
        {
            Vector<SQLTerm> temp = new Vector<>();
            for (int j = 0; j < n; j++){   
                if ((i & (1 << j)) > 0)
                    temp.add(set[j]);
            }
            permutation.add(temp);
        }
        return permutation;
    }

    
    public static Vector<Vector<SQLTerm>> getSubsetsv(Vector<SQLTerm> set)  //Karim this method gets all subsets form an array 
    {
        int n = set.size();
        Vector<Vector<SQLTerm>> permutation=new Vector<>();
        for (int i = 0; i < (1<<n); i++)
        {
            Vector<SQLTerm> temp = new Vector<>();
            for (int j = 0; j < n; j++){   
                if ((i & (1 << j)) > 0)
                    temp.add(set.get(j));
            }
            permutation.add(temp);
        }
        return permutation;
    }

    public Set<String> XORsets(Set<String> a ,List<String> b){  //Karim XORS
        Set<String> c = new HashSet<>();
        c.addAll(a); 
        c.addAll(b);
        a.retainAll(b); 
        c.removeAll(a); 
        return c;
    }


    private Vector<String> get_xor_of_result_set(SQLTerm[] arrSQLTerms) throws DBAppException, IOException { //karim
        //transforms to vector
        Vector<SQLTerm> newArrSQLTerms =new Vector<>();
        for(SQLTerm term:arrSQLTerms){
            newArrSQLTerms.add(term);
        }
        //final list of sets of paths
        List<List<String>> setsOfPaths = new ArrayList<>();
        //for each  single SQLterm check if it has a valid index else use linear scan and put the set in setsofpaths not implemented yet 
            for(SQLTerm single:arrSQLTerms){
                    Vector<SQLTerm> vectorOfOne = new Vector<>();
                    vectorOfOne.add(single);
                    GridIndex optimalIndex = get_optimal_index_to_be_used(vectorOfOne);
                    if(optimalIndex!=null){
                        Vector<String> setOfPagePaths= optimalIndex.get_result_set(vectorOfOne);
                        List<String> setOfPagePathsL = Collections.list(setOfPagePaths.elements());
                        setsOfPaths.add(setOfPagePathsL);
                    }else{
                        // do linear scan and add to sets of paths
                    }
            }
            if(setsOfPaths.isEmpty()){
                return null;
            }

            //XORing part
            Set<String> retValue=new HashSet<>();
            for(List<String> singleSet :setsOfPaths){
                retValue= XORsets(retValue,singleSet);
            }
            Vector<String> retvalue2 = new Vector<>(retValue);
            return retvalue2;
    }

    private Vector<String> get_or_of_result_set(SQLTerm[] arrSQLTerms) throws DBAppException, IOException {
    //Karim same as XOR difference is in the XORIng part its now oring part
        Vector<SQLTerm> newArrSQLTerms =new Vector<>();
        for(SQLTerm term:arrSQLTerms){
            newArrSQLTerms.add(term);
        }
        List<List<String>> setsOfPaths = new ArrayList<>();

            for(SQLTerm single:arrSQLTerms){
                    Vector<SQLTerm> vectorOfOne = new Vector<>();
                    vectorOfOne.add(single);
                    GridIndex optimalIndex = get_optimal_index_to_be_used(vectorOfOne);
                    if(optimalIndex!=null){
                        Vector<String> setOfPagePaths= optimalIndex.get_result_set(vectorOfOne);
                        List<String> setOfPagePathsL = Collections.list(setOfPagePaths.elements());
                        setsOfPaths.add(setOfPagePathsL);
                    }else{
                        // do linear scan 
                    }
            }

            //ORIng the records  this is a union between paths as set doesnt support duplicates
            Set<String> retValue=new HashSet<>();
            for(List<String> singleSet :setsOfPaths){
                retValue.addAll(singleSet);
            }
            Vector<String> retvalue2 = new Vector<>(retValue);
            return retvalue2;

    }
    
    private Vector<String> get_and_of_result_set(SQLTerm[] arrSQLTerms) throws DBAppException, IOException {
        // [Name<john, GPA>5, last!= 5aled, id=14]
        // which index to use
        // restults 
        //remove duplicates
        // split them somway so that we use the available indecies 
        

        //karim the anding one is alittle bit different it gets permutaions of the SQLTERM array and tries to find 
        //a valid index and then if found they are removed from the Starting SQLterm array and looped till the array
        //of SQLterms is empty 
        //if its stuck (stepbro... i am stuck) there is a flag to check that and then it does a linear search for the rest
        // of SQL terms the linear scan is not implemented 
        Hashtable<String,SQLTerm> reference = new Hashtable<>();
        Vector<SQLTerm> newArrSQLTerms =new Vector<>();
        for(SQLTerm term:arrSQLTerms){
            reference.put(term._strColumnName, term);
            newArrSQLTerms.add(term);
        }
        ArrayList<Vector<Hashtable<String,Object>>> setsOfRecords=new ArrayList<>();
        List<List<String>> setsOfPaths = new ArrayList<>();
        int prevsize;
        Boolean stuck = false; //stuck flag 
            while(newArrSQLTerms.size()!=0 && !stuck){
                prevsize=newArrSQLTerms.size();
                Vector<Vector<SQLTerm>> perm = getSubsetsv(newArrSQLTerms);
                for(int i=perm.size()-1;i>=0;i--){ //access the permutation from greatest in length to smallest 
                    // actually if this is reversed it might help in the stuck part and may actually remove the flag and cond
                    //but leave it as it is 
                    Vector<SQLTerm> colNamePerm =  perm.get(i);
                    GridIndex optimalIndex = get_optimal_index_to_be_used(colNamePerm);
                    if(optimalIndex!=null){ //index is found  add paths to pages and remove used SQLTerms from the loop list newArrSqlTerms
                        Vector<String> setOfPagePaths= optimalIndex.get_result_set(colNamePerm);
                        List<String> setOfPagePathsL = Collections.list(setOfPagePaths.elements());
                        setsOfPaths.add(setOfPagePathsL);
                        for(SQLTerm term :colNamePerm){
                            newArrSQLTerms.remove(term);
                        }// break to re enter the while loop after the removal of SQLTerms
                        break;
                    }
                }
                //checls for stuck
                if(prevsize==newArrSQLTerms.size()){
                    stuck=true;
                    break;
                }
            }
            //same part as the above ones 
            if(stuck){
                //linear search with what is left in newArrSQLTerms and add the paths to sets of paths 
                // add everything to do a liner searcch record by record 
               // setsOfPaths.addAll(this.get_all_pages()); // JOE: add all pages to be considered
               
            }

            //anding the records  this is an intersection between paths using retain all
            if(setsOfPaths.isEmpty()){
                return null;
            }
            Set<String> retValue=new HashSet<>(setsOfPaths.get(0));
            for(List<String> singleSet :setsOfPaths){
                retValue.retainAll(singleSet);
            }
            Vector<String> retvalue2 = new Vector<>(retValue);
            return retvalue2;
        
    }


    public  Vector<Hashtable<String,Object>> getAllRecordsOfTable() throws IOException{
        Vector<Page> actualPages = new Vector<>();
        Vector<Hashtable<String,Object>> allRecords= new Vector<Hashtable<String,Object>>();
        for(Vector<Hashtable<String,Object>> page:this.pages){
            Page singlePage = Page.DeserializePage((String)page.get(0).get("path"));
            actualPages.add(singlePage);
        }

        for(Page singlePage:actualPages){
            Vector<Hashtable<String,Object>> recordsOfOnePage = singlePage.get_all_records();
            allRecords.addAll(recordsOfOnePage);
        }
        return allRecords;

    }


    private boolean record_satifies_sqlTerm(SQLTerm sqlTerm, Hashtable<String, Object> record) throws DBAppException {
        switch(sqlTerm._strOperator){
            case "=":{
                Comparable value = (Comparable) sqlTerm._objValue;
                Object value_record = record.get(sqlTerm._strColumnName);
                if(value.compareTo(value_record) == 0)
                    return true;
                else 
                    return false;
            } 
            case ">=": {
                Comparable value = (Comparable) sqlTerm._objValue;
                Object value_record = record.get(sqlTerm._strColumnName);
                if(value.compareTo(value_record) <= 0)
                    return true;
                else 
                    return false;
                // otherwise go to less than part
            }
            case ">" : {
                Comparable value = (Comparable) sqlTerm._objValue;
                Object value_record = record.get(sqlTerm._strColumnName);
                if(value.compareTo(value_record) < 0)
                    return true;
                else 
                    return false;
            }

            case "<=" :{
                Comparable value = (Comparable) sqlTerm._objValue;
                Object value_record = record.get(sqlTerm._strColumnName);
                if(value.compareTo(value_record) >= 0)
                    return true;
                else 
                    return false;
            }
            case "<" :{
                Comparable value = (Comparable) sqlTerm._objValue;
                Object value_record = record.get(sqlTerm._strColumnName);
                if(value.compareTo(value_record) > 0)
                    return true;
                else 
                    return false;
            }
            case "!=" :{
                Comparable value = (Comparable) sqlTerm._objValue;
                Object value_record = record.get(sqlTerm._strColumnName);
                if(value.compareTo(value_record) != 0)
                    return true;
                else 
                    return false;
            }
            default : throw new DBAppException("unknow SQL_operator in SQLTerm " + sqlTerm._strOperator);
        }
    }




   


    private Vector<String> _xor(Vector<String> pages_to_consider1, Vector<String> pages_to_consider2) {
        Vector<String> ans = new Vector<>();
        for (String string : pages_to_consider1) {
            if(!pages_to_consider2.contains(string)){
                ans.add(string);
            }
        }
        for (String string : pages_to_consider2) {
            if(!pages_to_consider1.contains(string)){
                ans.add(string);
            }
        }
        return ans;
    }


    protected void createIndex(String[] cols_to_create_index_on) throws IOException {
        // TODO check that all cloumn names are valid 
        // TODO: JOE -> if there are alrady inserts don not forget to put them in the new index
        //Karim done !!
        try {
            GridIndex g = new GridIndex(cols_to_create_index_on, this.colNameMin, this.colNameMax, this.colNameType,this);
            this.indices.add(g);
            if(pages.size()!=0){
                boolean flag = false;
                for(Vector<Hashtable<String,Object>> col:pages){
                    if(col!=null){
                        flag = true;
                        break;
                    }
                }
                if(flag){
                    for(Vector<Hashtable<String,Object>> col2:pages){
                        for(Hashtable<String,Object> record:col2){
                            Page tempPage = Page.DeserializePage((String)record.get("path"), this);
                            Vector<Hashtable<String,Object>> content = tempPage.contents;
                            for(Hashtable<String,Object> pagerecord:content){
                                g.insert(pagerecord, tempPage);
                            }
                        }
                    }
                }
            }
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (DBAppException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }


    // private static void test_something() throws IOException, DBAppException{
    //     System.out.println(getTypeCast("students", "gpa"));
    	
    //     String strTableName = "Student";
 
    //     Hashtable<String,String> htblColNameType = new Hashtable<String,String> ( );
    //     htblColNameType.put("id", "java.lang.Integer");
    //     htblColNameType.put("name", "java.lang.String");
    //     htblColNameType.put("gpa", "java.lang.Double");
 
    //     Hashtable<String,String> htblColNameMin = new Hashtable<String,String> ( );
    //     htblColNameMin.put("id", "0");
    //     htblColNameMin.put("name", "A");
    //     htblColNameMin.put("gpa", "0.0");
 
    //     Hashtable<String,String> htblColNameMax = new Hashtable<String,String> ( );
    //     htblColNameMax.put("id", "1000000");
    //     htblColNameMax.put("name", "ZZZZZZZZZZZ");
    //     htblColNameMax.put("gpa", "5.0");
 
    //     Table T = new Table( strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax, false);
 
    //     Hashtable<String,Object> h1 = new Hashtable<String,Object> ( );
    //     h1.put("id", 2343432 );
    //     h1.put("name", new String ("Ahmed Noor" ) );
    //     h1.put("gpa", 0.95  );
    //     T.insert(h1);
 
    //     Hashtable<String,Object> h2 =new Hashtable<String,Object> ( );
 
    //     h2.put("id", 453455 );
    //     h2.put("name", new String ("Ahmed Noor" ) );
    //     h2.put("gpa", 0.95 );
    //     T.insert(h2);
 
 
 
    //     Hashtable<String,Object> h3 = new Hashtable<String,Object> ( );
 
    //     h3.put("id",5674567 );
    //     h3.put("name", new String ("Dalia Noor" ) );
    //     h3.put("gpa", 1.25 );
    //     T.insert(h3);
 
    //     Hashtable<String,Object> h4 = new Hashtable<String,Object> ( );
 
    //     h4.put("id", 23498 );
    //     h4.put("name", new String ("John Noor" ) );
    //     h4.put("gpa", 1.5 );
    //     T.insert(h4);
 
    //     Hashtable<String,Object> h5 = new Hashtable<String,Object> ( );
 
    //     h5.put("id", 78452 );
    //     h5.put("name", new String ("Zaky Noor" ) );
    //     h5.put("gpa", 0.88 );
    //     T.insert(h5);
 
 
    //     //System.out.println(T.pages.firstElement().getmin());
    //    // System.out.println(T.pages.firstElement().getmax());
 
    //     Hashtable<String,Object> h7 = new Hashtable<String,Object> ( );
 
    //     h7.put("id", 234 );
    //     h7.put("name", new String ("John Noor" ) );
    //     h7.put("gpa", 1.5 );
    //     T.insert(h7);
 
 
    //     Hashtable<String,Object> h8 = new Hashtable<String,Object> ( );
 
    //     h8.put("id", 123498 );
    //     h8.put("name", new String ("John Noor" ) );
    //     h8.put("gpa", 1.5 );
    //     T.insert(h8);
 
 
    //     Hashtable<String,Object> h6 = new Hashtable<String,Object> ( );
    //     h6.put("name", new String ("test" ) );
 
    //     T.update(23498,h6);
    //     T.delete(78452);
    //     System.out.println(1);
    //     int[] records = {23498, 78452, 234, 123498, 5674567, 453455, 2343432, 11};
 
    //     Hashtable<String,Object> h9 = new Hashtable<String,Object> ( );
    //     h9.put("id", 30000 );
    //     h9.put("name", new String ("s7s Noor" ) );
    //     h9.put("gpa", 1.5 );
    //     T.insert(h9);
    //     Arrays.sort(records);
    //    // T.pages.firstElement().print();

    // }
    
//    private static void test_search() throws IOException, DBAppException {
//      	String tableName = "test table";
//  		String clusteringKey = "id";
//  		int max_page_size = 128;
//  		Hashtable<String, String> colNameMin ;
//  		Hashtable<String, String> colNameMax ;
//        Hashtable<String,String> htblColNameType = new Hashtable<String,String> ( );
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("gpa", "java.lang.Double");
//        Hashtable<String,String> htblColNameMin = new Hashtable<String,String> ( );
//        htblColNameMin.put("id", "0");
//        htblColNameMin.put("name", "A");
//        htblColNameMin.put("gpa", "0.0");
//        Hashtable<String,String> htblColNameMax = new Hashtable<String,String> ( );
//        htblColNameMax.put("id", "1000000");
//        htblColNameMax.put("name", "ZZZZZZZZZZZZZ");
//        htblColNameMax.put("gpa", "10.0");
//  		htblColNameType = new Hashtable();
//  		htblColNameType.put("id", "java.lang.Integer");
//  		htblColNameType.put("name", "java.lang.String");
//  		htblColNameType.put("gpa", "java.lang.double");	
//  		Table t = new Table(tableName, clusteringKey, htblColNameType, htblColNameMin, htblColNameMax,true);
//  		Hashtable htblColNameValue = new Hashtable();
//  		for (int i = 0; i < 6; i++) {
//  			htblColNameValue = new Hashtable();
//  			htblColNameValue.put("id", new Integer(i));
//  			htblColNameValue.put("name", new String("Ahmed Noor " + i));
//  			htblColNameValue.put("gpa", new Double(0.95 + i));
//  			t.insert(htblColNameValue);
//  		}
 		
 		
 		
 		
//  		System.out.println("starting table");
//  		System.out.println(t);
//  		System.out.println("searching from the most general search to the most specific");
//  		Hashtable<String, Object> search_keys = new Hashtable();
 		
//  		System.out.println("expected results are all rows in table as the search criteria was empty so like i was looking for rows that had any value in any field");
//  		for(Hashtable<String, Object> resulting_row: t.LinearSearch(search_keys)){
//  			System.out.println(resulting_row);
//  		}
 		
//  		htblColNameValue = new Hashtable();
//  		htblColNameValue.put("id", new Integer(8));
//  		htblColNameValue.put("name", new String("test row"));
//  		htblColNameValue.put("gpa", new Double(0.95));
//  		t.insert(htblColNameValue);
 		
//  		search_keys.put("gpa", new Double(0.95));
//  		System.out.println("expected results are all rows with gpa 0.95 which should be 2 rows one of which is test");
//  		for(Hashtable<String, Object> resulting_row: t.LinearSearch(search_keys)){
//  			System.out.println(resulting_row);
//  		}
//  		search_keys.put("id", new Integer(0));

//  		System.out.println("expected results are all rows with gpa 0.95 and id = 0");
//  		for(Hashtable<String, Object> resulting_row: t.LinearSearch(search_keys)){
//  			System.out.println(resulting_row);
//  		}
//  	}
    
    // static void test_table_class() throws IOException, DBAppException {
	// 	String tableName = "test table";
	// 	String clusteringKey = "id";
	// 	int max_page_size = 128;
	// 	Hashtable<String, String> colNameMin ;
	// 	Hashtable<String, String> colNameMax ;
    //   Hashtable<String,String> htblColNameType = new Hashtable<String,String> ( );
    //   htblColNameType.put("id", "java.lang.Integer");
    //   htblColNameType.put("name", "java.lang.String");
    //   htblColNameType.put("gpa", "java.lang.Double");

    //   Hashtable<String,String> htblColNameMin = new Hashtable<String,String> ( );
    //   htblColNameMin.put("id", "0");
    //   htblColNameMin.put("name", "A");
    //   htblColNameMin.put("gpa", "0.0");

    //   Hashtable<String,String> htblColNameMax = new Hashtable<String,String> ( );
    //   htblColNameMax.put("id", "1000000");
    //   htblColNameMax.put("name", "ZZZZZZZZZZZ");
    //   htblColNameMax.put("gpa", "5.0");

	// 	htblColNameType = new Hashtable();
	// 	htblColNameType.put("id", "java.lang.Integer");
	// 	htblColNameType.put("name", "java.lang.String");
	// 	htblColNameType.put("gpa", "java.lang.double");

	// 	Table t = new Table(tableName, clusteringKey, htblColNameType, htblColNameMax, htblColNameMax,true);
	// 	System.out.println("<<Empty Table>>");
	// 	System.out.println(t);
	// 	Hashtable htblColNameValue = new Hashtable();
	// 	htblColNameValue.put("id", new Integer(2343432));
	// 	htblColNameValue.put("name", new String("Ahmed Noor"));
	// 	htblColNameValue.put("gpa", new Double(0.95));

	// 	t.insert(htblColNameValue);
	// 	System.out.println("<<Table with entrie>>");
	// 	System.out.println(t);

	// 	for (int i = 0; i < max_page_size; i++) {
	// 		htblColNameValue = new Hashtable();
	// 		htblColNameValue.put("id", new Integer(i));
	// 		htblColNameValue.put("name", new String("Ahmed Noor " + i));
	// 		htblColNameValue.put("gpa", new Double(0.95 + i));

	// 		t.insert(htblColNameValue);
	// 	}
	// 	System.out.println("<<Table with 1 page maxed out >>");
	// 	System.out.println(t);

	// 	htblColNameValue = new Hashtable();
	// 	htblColNameValue.put("id", new Integer(max_page_size));
	// 	htblColNameValue.put("name", new String("karim in page 2 "));
	// 	htblColNameValue.put("gpa", new Double(1.00));

	// 	t.insert(htblColNameValue);

	// 	System.out.println("<<Table with 2 pages  cuz of overflow>>");
	// 	System.out.println(t);

	// 	htblColNameValue = new Hashtable();
	// 	htblColNameValue.put("id", new Integer(max_page_size+1));
	// 	htblColNameValue.put("name", new String("karim in page 2 "));
	// 	htblColNameValue.put("gpa", new Double(1.00));

	// 	t.insert(htblColNameValue);

	// 	System.out.println("<<Table with 2 pages  cuz of overflow>>");
	// 	System.out.println(t);
		
	// 	for (int i = 0; i < 200; i++) {
	// 		htblColNameValue = new Hashtable();
	// 		htblColNameValue.put("id", new Integer(i));
	// 		htblColNameValue.put("name", new String("Ahmed Noor " + i));
	// 		htblColNameValue.put("gpa", new Double(0.95 + i));

	// 		t.insert(htblColNameValue);
	// 	}
	// 	System.out.println("<<Table with few page maxed out pages and also a very agressive input algorithm>>");
	// 	System.out.println(t);
		
	// 	htblColNameValue = new Hashtable();
	// 	htblColNameValue.put("id", new Integer(11));
	// 	htblColNameValue.put("name", new String("assme"));
	// 	htblColNameValue.put("gpa", new Double(0.9));

	// 	t.insert(htblColNameValue);
		
	// 	System.out.println("<<Table with few page maxed out pages and also a very agressive input algorithm>>");
	// 	System.out.println(t);
		
    // }
    

    
    static void test_serialization() throws IOException, DBAppException {
      String strTableName = "Student";

      Hashtable<String,String> htblColNameType = new Hashtable<String,String> ( );
      htblColNameType.put("id", "java.lang.Integer");
      htblColNameType.put("name", "java.lang.String");
      htblColNameType.put("gpa", "java.lang.Double");

      Hashtable<String,String> htblColNameMin = new Hashtable<String,String> ( );
      htblColNameMin.put("id", "0");
      htblColNameMin.put("name", "A");
      htblColNameMin.put("gpa", "0.0");

      Hashtable<String,String> htblColNameMax = new Hashtable<String,String> ( );
      htblColNameMax.put("id", "1000000");
      htblColNameMax.put("name", "ZZZZZZZZZZZ");
      htblColNameMax.put("gpa", "5.0");

        Table t = new Table( strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax, false);
        Hashtable record1 = new Hashtable<String, Object>();
		Hashtable record2 = new Hashtable<String, Object>();
		record1.put("id", 15);
		record2.put("id", 10);
		Page p1 = new Page(record1, t);
		String path_to_p1 = t.savePage(p1);
		Page p2 = new Page(record2, t);
		String path_to_p2 = t.savePage(p2);
		p2.insert(record1);
		t.savePage(p2);
		
		System.out.println("now print p1");
		Page p1_new = t.loadPage(path_to_p1);
		System.out.println(p1_new);

		Page p2_new = t.loadPage(path_to_p2);
		System.out.println("now print p2 with updated value to make sure it has been overwriten after save");
		System.out.println(p2_new);
    }
    
    
//	///////////Serilization part
	public String savePage(Page p) throws IOException, DBAppException { 
		// String path_to_be_saved_to = "DB/" + this.tableName;
		// String path = "DB/"+this.tableName+"/"+p.ID+".ser";
		// 	File page_file = new File(path);
		// 	ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(page_file));
	    //     os.writeObject(p);
	    //     os.close(); 
	         
	    //     File dicrectory = new File("DB/" + this.tableName);
	    //     boolean bool = dicrectory.mkdirs();
		//       if(bool){
		//          System.out.println("Directory created successfully");
		//       }else{
		//          System.out.println("Sorry couldn’t create specified directory " + dicrectory.getAbsolutePath());
		//       }
		//         File f = new File(dicrectory.getAbsolutePath(), p.ID+".ser");
		//         ObjectOutputStream os;
		// 			os = new ObjectOutputStream(new FileOutputStream(f));
		// 			 os.writeObject(p);
		// 		     os.close(); 
				
		p.SerializePage();
        String path = p.path;
	
	
		return path;
	}
	
	public Page loadPage(String path) throws IOException {
		Page p = null;

	         FileInputStream fileInStr = new FileInputStream(path);
	         ObjectInputStream objInStr = new ObjectInputStream(fileInStr);
	         try {
                p  = (Page) objInStr.readObject();
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
	         objInStr.close();
	         fileInStr.close();
	    
		return p;
		
	}
	/////////////////// UTIL
	@Override
	public String toString() {
		String str = "";
		str += "Table [tableName=" + tableName + ", clusteringKey=" + clusteringKey 
				+ "]\n";
		str += "columns in Table \n{\n";
		for (String key : colNameType.keySet()) {
			str += key + "  " + colNameType.get(key) + "\n";
		}
		str += "}\n << ==================================== >>\n";
		int dummy_index = 0;
		for (Vector<Hashtable<String, Object>> t1 : this.pages) {
			for(Hashtable<String, Object> t2: t1) {
			str += "Page " + dummy_index + " min = " + t2.get("min") + " max = " + t2.get("max");
			Page p = null;
            try {
                p = this.loadPage((String) t2.get("path"));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                
            }
			str += "\n" + p;
			str += "------------------------------ \n";
			dummy_index++;
			}
		}

		str += "||||||||||||||||||||     |||||||||||||||||||| \n \n";
		return str;
	}

}