import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;

public class GridIndex implements Serializable {
    
    /**
	 * 
	 */
	private static final long serialVersionUID = 6457024016163227718L;
	private int dim;
    public ndIndex content;
    ndIndex size_of_each_bucket; // tells us how many elements in each bucket like in grid index {name:'aaa' -> 'bbb', id: 13 -> 30} has 5 elements
    private Hashtable<String, List> ranges;
    public Vector<String> column_names;
    Table table;

    public GridIndex(String[] cols_to_create_index_on, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax, Hashtable<String, String> colNameType ,Table table) throws IOException, ParseException, DBAppException {
        /*takes a string array of the dimension names to create Grid index on
        EXample 
        new GridIndex(["name", "id", GPA],
                         {name:"aaa", id:1, Gpa : 0.0},
                         {name:"zzz", id:1000, Gpa : 5.0},
                         {name:"java.lang.String", id:"java.lang.Integer", Gpa : "java.lang.Double"})
        */
        this.table=table;
        this.dim = cols_to_create_index_on.length;
        this.column_names = get_vector_from_array(cols_to_create_index_on);
        this.content = new ndIndex(this.dim);
        this.size_of_each_bucket = new ndIndex(this.dim);
        this.ranges = new Hashtable<String, List>();
        for (String dimension_name : cols_to_create_index_on) {
            Object parsed_min;
            Object parsed_max;
           
            String type = colNameType.get(dimension_name);
            switch(type.toLowerCase()) {
                           	case "java.lang.double": {
                                   parsed_min = Double.parseDouble(colNameMin.get(dimension_name));
                                   parsed_max = Double.parseDouble(colNameMax.get(dimension_name));
                                   break;
                               }
                           	case "java.lang.string" :{
                                parsed_min = colNameMin.get(dimension_name);
                                parsed_max = colNameMax.get(dimension_name);
                                break;
                            }
                               case "java.util.date": {
                                {
                                	parsed_min = new SimpleDateFormat("yyyy-MM-dd").parse(colNameMin.get(dimension_name));
                                	parsed_max = new SimpleDateFormat("yyyy-MM-dd").parse(colNameMax.get(dimension_name));
//                                    parsed_min = new SimpleDateFormat().parse(colNameMin.get(dimension_name));
//                                    parsed_max = new SimpleDateFormat().parse(colNameMax.get(dimension_name));
                                    break;
                                }
                               }
                               case "java.lang.integer" : {
                                parsed_min = Integer.parseInt(colNameMin.get(dimension_name));
                                parsed_max = Integer.parseInt(colNameMax.get(dimension_name));
                                break;
                            }
                               default: throw new DBAppException("unknown data type not integer, date ,String, Double");
            	}
           // System.out.println(Ranges.getRange(parsed_min, parsed_max));
            List range = Ranges.getRange(parsed_min, parsed_max);
            this.ranges.put(dimension_name, range);
        }

        if(table!=null){
            Vector<Vector<Hashtable<String,Object>>> currentpages = table.pages;
        }
    }

    private Vector<String> get_vector_from_array(String[] cols_to_create_index_on) {
        Vector<String> temp = new Vector<>();
        for (String i : cols_to_create_index_on) {
            temp.add(i);
        }
        return temp;
    }
    public void delete(Hashtable<String, Object> colNameValue, Page p) throws DBAppException, IOException {
        ArrayList<Integer> multi_dim_index_of_this_record = new ArrayList<>();
        for (String col_name : column_names) {
            Object value = colNameValue.get(col_name);
            if(value instanceof String){
                // if it is a string cuz of implementation reasons we must hash it as string ranges are actually the hash function which maps strings to longs
                value = Ranges.hashCode((String)value);
            }
            List range = this.ranges.get(col_name);
            // find the index where this colum falls 
            int this_dimension_index = -1;
            for (Object max_of_this_range : range) {
                
                // if it falls between [min, max[ 
                // Comparable min_of_this_range = (Comparable) htbl_min_max.get("min");
                // Comparable max_of_this_range = (Comparable) htbl_min_max.get("max");
                this_dimension_index++;
                int bigger_than = ((Comparable) max_of_this_range).compareTo(value);
                    if(bigger_than > 0){
                        break;
                    }
            }
            multi_dim_index_of_this_record.add(this_dimension_index);
        }
         // System.out.println(multi_dim_index_of_this_record);
         int[] index = get_int_from_arrayList(multi_dim_index_of_this_record);
         this.delete_from_bucket(index, colNameValue, p);
        
    }


    public void insert(Hashtable colNameValue, Page p) throws DBAppException, IOException {
        // 1 get the index it shold be inserted to 
        ArrayList<Integer> multi_dim_index_of_this_record = new ArrayList<>();
        for (String col_name : column_names) {
            Object value = colNameValue.get(col_name);
            if(value instanceof String){
                // if it is a string cuz of implementation reasons we must hash it as string ranges are actually the hash function which maps strings to longs
                value = Ranges.hashCode((String)value);
            }
            List range = this.ranges.get(col_name);
            // find the index where this colum falls 
            int this_dimension_index = -1;
            for (Object max_of_this_range : range) {
                
                // if it falls between [min, max[ 
                // Comparable min_of_this_range = (Comparable) htbl_min_max.get("min");
                // Comparable max_of_this_range = (Comparable) htbl_min_max.get("max");
                this_dimension_index++;
                int bigger_than = ((Comparable) max_of_this_range).compareTo(value);
                    if(bigger_than > 0){
                        break;
                    }
            }
            multi_dim_index_of_this_record.add(this_dimension_index);
            
        }

       // System.out.println(multi_dim_index_of_this_record);
        int[] index = get_int_from_arrayList(multi_dim_index_of_this_record);
        this.add_to_bucket(index, colNameValue, p);
    }




        
    

    private void add_to_bucket(int[] index, Hashtable colNameValue, Page p) throws DBAppException, IOException {
        //see if a bucket was created here
        String path_to_bucket = this.content.get(index);
        if(path_to_bucket==null){
            Bucket b = new Bucket(this.column_names.toArray(new String[this.column_names.size()])); // just to type cast
           // BucketRecord br = new BucketRecord(colNameValue, this.column_names, p);
            b.insert_in_bucket(colNameValue, p);
           // b.serialize();
           this.content.set(index, b.path);
        } else {
            Bucket b = Bucket.Decerialize(path_to_bucket);
            b.insert_in_bucket(colNameValue, p);
            //b.serialize();
            this.content.set(index, b.path);
        }
        // increase number of elements in this bucket
        Object number_of_elements_in_this_cell = this.size_of_each_bucket.get(index);
        int current_size = (number_of_elements_in_this_cell != null)? Integer.parseInt((String) number_of_elements_in_this_cell) : 0; // if the cell has null then assum it to be zero
        int new_size = current_size + 1;
        this.size_of_each_bucket.set(index, new_size+"");
    }


    private void delete_from_bucket(int[] index, Hashtable<String, Object> colNameValue, Page p) throws IOException, DBAppException {
        //see if a bucket was created here
        
        String path_to_bucket = this.content.get(index);
        if(path_to_bucket==null){
            Bucket b = new Bucket(this.column_names.toArray(new String[this.column_names.size()])); // just to type cast
           // BucketRecord br = new BucketRecord(colNameValue, this.column_names, p);
            b.delete_from_bucket(colNameValue, p);
            b.serialize();
        } else {
            Bucket b = Bucket.Decerialize(path_to_bucket);
            b.delete_from_bucket(colNameValue, p);
            b.serialize();
        }
        // decrease number of elements in this bucket
        Object number_of_elements_in_this_cell = this.size_of_each_bucket.get(index);
        int current_size = (number_of_elements_in_this_cell != null)? Integer.parseInt((String) number_of_elements_in_this_cell) : 0; // if the cell has null then assum it to be zero
        int new_size = current_size - 1;
        this.size_of_each_bucket.set(index, new_size+"");
    }

    private int[] get_int_from_arrayList(ArrayList<Integer> al) {
        int[] temp = new int[al.size()];
        for (int i = 0; i < al.size(); i++) {
            temp[i] = al.get(i);
        }
        return temp;
    }

   
    private static void test_inserting_in_grid_index() throws IOException, ParseException, DBAppException {
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
        htblColNameMax.put("id", "10");
        htblColNameMax.put("name", "ZZZZZZZZZZZ");
        htblColNameMax.put("gpa", "1");
        Table t = null;
        String[] cols_to_create_index_on = new String[]{"id", "gpa"};

        GridIndex g = new GridIndex(cols_to_create_index_on, htblColNameMin, htblColNameMax, htblColNameType,t);
        for (int i = 0; i < 10; i++) {
			Hashtable htblColNameValue = new Hashtable();
			htblColNameValue.put("id", new Integer(i));
			htblColNameValue.put("name", new String("Ahmed Noor " + i));
			htblColNameValue.put("gpa", new Double( i/10.0 ));

		//	g.insert(htblColNameValue);
		}

    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        String str =  super.toString() + "\n";
        str += "----------- Ranges ------------";
        for (String col_name : column_names) {
            str += col_name + ": " + this.ranges.get(col_name)+"\n";
        }
        // print the diagnoal in and dimensions
        for (int i = 0; i < 9; i++) {
            int[] index_to_print_on_diagonal = get_array_like_this_number(i);
            str +=  "at index: "+Arrays.toString(index_to_print_on_diagonal)+" -> "+this.size_of_each_bucket.get(index_to_print_on_diagonal) + "\n";
        }

        return str;
    }

    private int[] get_array_like_this_number(int i) {
        /*given 1 returns [1, 1, 1, 1] and 2 returns [2, 2...2, 2] having a lengthof the grid index dimensions like 3d = name, age, gpa then we get [2, 2, 2] */
        int[] res = new int[this.column_names.size()];
        for (int j = 0; j < res.length; j++) {
            res[j] = i;
        }
        return res;
        }

    public Vector<String> get_result_set(Vector<SQLTerm> v) throws DBAppException, IOException {
        Hashtable<String, Vector<Integer>> colName_ranges = new Hashtable<>();
        for (SQLTerm sqlTerm : v) {
            String colum_name = sqlTerm._strColumnName;
            if(!this.column_names.contains(colum_name))
                continue;
            String operator = sqlTerm._strOperator;
            Object value = sqlTerm._objValue;
            List range = this.ranges.get(colum_name);
            Vector<Integer> indecies_containing_valid_records = get_valid_ranges(range, operator, value);
            colName_ranges.put(colum_name, indecies_containing_valid_records);

        }
        // for all non mentioend indecies used like if we have name, age, gpa but selection only on name then age and gpa get -1
        // put 2d array of answer 
        Vector<Vector<Integer>> indecies_of_each_colum = new Vector<>();
        for ( int i = 0; i < this.column_names.size();i++) {
            String temp_col_name = this.column_names.get(i);
            if(colName_ranges.containsKey(temp_col_name)){
                // put the right range 
                indecies_of_each_colum.add(colName_ranges.get(temp_col_name));
            } else {
                // put constant range [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
                indecies_of_each_colum.add(get_constant_from_0_to_9_vector());
            }
        }

        //////////////////////////// aaaaaaaaaaaaddddddddddddddfffffffffffuuuuuuuuuunnnnnnnccccccccttttttttttttiiiiiiiiiooooooooonnnnnnnnnnnn here
        Vector<String> paths_to_buckets=this.content.get_set(indecies_of_each_colum); // TODO: JOE
        // put in set to remove duplicates if nessasary 
        Vector<String> paths_to_pages = new Vector<>();
        for (String path_bucket : paths_to_buckets) {
            if(path_bucket == null)
                continue;
            Bucket b = Bucket.Decerialize(path_bucket);
            paths_to_pages.addAll(b.get_all_pages());
        }
        // fake_get_path_to_pages_that_should_totoal_not_be_used_and_only_exsits_so_that_i_can_continue_working();// get all the paths to pages and remove duplicates
        // sent these pages to the fillter method 
        paths_to_pages = paths_to_pages.stream().distinct().collect(Collectors.toCollection(Vector::new)); // remove duplicates so that we do not load 1 page multiple times
        

        // this fillter method will check everything in each page that matches all the SQL Terms with their conjuction like or , and , XOR 

        return paths_to_pages;
    }

    private Vector<String> fake_get_path_to_pages_that_should_totoal_not_be_used_and_only_exsits_so_that_i_can_continue_working() throws DBAppException {
        // rest everything a a test method i created
        Vector<String> all_pages_paths = new Vector<>();
        Vector<Page> p = new Vector<>();

        for (int i = 1; i < 5; i++) {
            all_pages_paths.add("src/main/resources/data/Student/Student_0_.._"+i+".ser");
        }
        
        
        
        return all_pages_paths;
    }

    private Vector<Integer> get_constant_from_0_to_9_vector() {
        Vector<Integer> ans = new Vector<>();
        for (int i = 0; i < 10; i++) {
            ans.add(i);
        }
            return ans;
        }

    public Vector<BucketRecord> get_result_set(String colum_name, String operator, Object value) throws DBAppException {
        // get range of this colum
        List range = this.ranges.get(colum_name);
        if(range==null)
            throw new DBAppException("this grid index is not on this column name " + colum_name);
        Vector<Integer> indecies_containing_valid_records = get_valid_ranges(range, operator, value);
        Hashtable<Integer, Vector<Integer>> ans = new Hashtable<>();
        for (int i = 0; i < this.column_names.size(); i++) {
            String col_name = this.column_names.get(i);
            if(col_name.equals(colum_name)){
                ans.put(i, indecies_containing_valid_records);
            } else {
                Vector<Integer> select_all_vector_simbolizing_selecting_evrything = new Vector<>();
                select_all_vector_simbolizing_selecting_evrything.add(-1);
                ans.put(i, select_all_vector_simbolizing_selecting_evrything);
            }
        }


        return null;
    }

    private static Vector<Integer> get_valid_ranges(List range, String operator, Object value) throws DBAppException {
        Vector<Integer> indexes_of_valid_buckets = new Vector<>();

        for (int i = 0; i < range.size(); i++) {
            Object object = range.get(i);
            Comparable max_in_this_range = (Comparable) object;
            boolean is_less_than_max_allower_value_in_range = (max_in_this_range.compareTo(value) > 0);
            boolean is_greater_than_or_equal_allower_value_in_range = (max_in_this_range.compareTo(value) <= 0);
            switch(operator){
                case "=": {
                    if(is_less_than_max_allower_value_in_range){
                        Vector<Integer> one_bucket_where_u_will_find_it = new Vector<Integer>();
                        one_bucket_where_u_will_find_it.add(i);
                        
                        return one_bucket_where_u_will_find_it;
                    }
                    break;
                }
                case "<=":
                case "<": { // X < value
                    if(i != 0){ // if this is not the 1st bucket
                        Comparable last_element = (Comparable) range.get(i-1);
                        // if the last bucket had a max stricktly bigger than this value 
                        boolean did_last_bucket_have_a_max_bigger_than_this_value = ((last_element).compareTo(value) > 0);
                        if(did_last_bucket_have_a_max_bigger_than_this_value){
                           
                            return indexes_of_valid_buckets;     // do not add buckets any more
                        }
                    }
                    indexes_of_valid_buckets.add(i);// add all indecies till
                    break;
                }
                case ">=":
                case ">":{
                    if(is_less_than_max_allower_value_in_range){ // if the max allower range in this bucket is less than that of the value
                        indexes_of_valid_buckets.add(i);
                    } else {
                        // value is bigger than the max so no element in this range satisfies
                    }
                    break;
                }
                case "!=":{ // return everything and let fillter handel the rest
                    indexes_of_valid_buckets.add(i);
                    break;
                }
                default: throw new DBAppException("unknown operator " + operator);
                
            }
            
        }
        return indexes_of_valid_buckets;
    }
   
    private void test_ranges() throws DBAppException{
        Vector range = new Vector<>();
        for (int i = 10; i <= 100; i = i+10) {
            range.add(i);
        }
        System.out.println(range);
        Vector<Integer> indecies_to_serch_in = get_valid_ranges(range, "!=", 29);
        System.out.println(indecies_to_serch_in);
    }

    public void update(Hashtable<String, Object> record, Page p) throws DBAppException, IOException {
        this.delete(record, p);
        this.insert(record, p);
    }

   

    

}
