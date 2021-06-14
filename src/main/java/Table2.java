import java.io.IOException;
import java.text.ParseException;
import java.util.Hashtable;
import java.util.Vector;

public class Table2 extends Table {
    /**
	 * 
	 */
	private static final long serialVersionUID = -2899020044193606740L;
	Vector<Tuple> contents = new Vector<>();
    public Table2(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
            Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax, boolean writable)
            throws DBAppException, IOException {
        super(tableName, clusteringKey, colNameType, colNameMin, colNameMax, writable);
        
    }
    @Override
    public  Vector<Hashtable<String,Object>> getAllRecordsOfTable() throws IOException{
        Vector<Page> actualPages = new Vector<>();
        Vector<Hashtable<String,Object>> allRecords= new Vector<Hashtable<String,Object>>();
        for(Tuple key_page:this.contents){
            Page singlePage = Page.DeserializePage(key_page.path);
            actualPages.add(singlePage);
        }

        for(Page singlePage:actualPages){
            Vector<Hashtable<String,Object>> recordsOfOnePage = singlePage.get_all_records();
            allRecords.addAll(recordsOfOnePage);
        }
        return allRecords;

    }


   
    public boolean insert(Hashtable<String, Object> record) throws IOException, DBAppException {
        Page p = null;
        if(this.contents.isEmpty()){
            p = new Page(this);
            p.insert(record);
            String path = p.path;
            Tuple first = new Tuple(p.min, path);
            this.contents.add(first);
            return true;
        }else{
        Object primary_key_value = record.get(this.clusteringKey);
        Tuple min_pagePath = get_page_to_insert_into(primary_key_value);
        String page_path = min_pagePath.path;
        Page page = Page.DeserializePage(page_path);
        p = page;
        Object update_min = page.insert(record);
        boolean is_this_the_last_page_in_contents = (this.contents.indexOf(min_pagePath) == (this.contents.size() -1) );
        if(is_this_the_last_page_in_contents){ // if this page is the last page
            // check if it created a next page
            if(page.next_page!=null){
                // add new page as last page
                Page new_page = Page.DeserializePage(page.next_page);
                Comparable min = new_page.min;
                this.contents.add(new Tuple(min, new_page.path));
            }
        }
        min_pagePath.key = update_min;
        super.updateIndecies(record, p, tableOperations.INSERT);
        return true;
        }

    }
    private Tuple get_page_to_insert_into(Object primary_key_value) throws IOException, DBAppException {
        
        for (int i = 0; i < contents.size(); i++) {
            Tuple t1 = this.contents.get(i);
            Comparable min = (Comparable)t1.key;
            if(min.compareTo(primary_key_value) > 0){
                // insert into previous page 
                if(i-1 >= 0)
                    return this.contents.get(i-1);
                else
                    return this.contents.firstElement();
            }
        }
        // return the last page
        return this.contents.lastElement();
    }
    public boolean delete(Comparable key) throws IOException, DBAppException 
    {
        Tuple min_pagePath = get_page_to_insert_into(key);
        String page_path = min_pagePath.path;
        Page page = Page.DeserializePage(page_path);
        Object update_min = page.delete(key);
        min_pagePath.key = update_min;
        if(page.isEmpty()){
            page.remove_from_disk();
            this.contents.remove(min_pagePath); // delete refrences to this page
        }
        this.SerializeTable();
        return true;
    }
    @Override
    protected void createIndex(String[] cols_to_create_index_on) throws IOException {
        // TODO check that all cloumn names are valid 
        // TODO: JOE -> if there are alrady inserts don not forget to put them in the new index
        //Karim done !!
        try {
            GridIndex g = new GridIndex(cols_to_create_index_on, this.colNameMin, this.colNameMax, this.colNameType,this);
            this.indices.add(g);
            if(this.contents.size()!=0){
                boolean flag = false;
                for(Tuple col:this.contents){
                    if(col!=null){
                        flag = true;
                        break;
                    }
                }
                if(flag){
                    for(Tuple col2:this.contents){
                        
                            Page tempPage = Page.DeserializePage((String)col2.path, this);
                            Vector<Hashtable<String,Object>> content = tempPage.contents;
                            for(Hashtable<String,Object> pagerecord:content){
                                g.insert(pagerecord, tempPage);
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

   
    private static void test_table_overflows() throws DBAppException, IOException {
        String tableName = "test table";
		String clusteringKey = "id";
		int max_page_size = 128;
		Hashtable<String, String> colNameMin ;
		Hashtable<String, String> colNameMax ;
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

		htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.double");

		Table2 t = new Table2(tableName, clusteringKey, htblColNameType, htblColNameMax, htblColNameMax,true);
		// insert values from 1 to 100 
        for (int i = 0; i < 100; i++) {
			Hashtable<String, Object> htblColNameValue = new Hashtable();
			htblColNameValue.put("id", new Integer(i));
			htblColNameValue.put("name", new String("Ahmed Noor " + i));
			htblColNameValue.put("gpa", new Double(0.95 + i));

			t.insert(htblColNameValue);
		}
        // insert values from 300 to 400
        for (int i = 300; i < 400; i++) {
			Hashtable<String, Object> htblColNameValue = new Hashtable();
			htblColNameValue.put("id", new Integer(i));
			htblColNameValue.put("name", new String("Ahmed Noor " + i));
			htblColNameValue.put("gpa", new Double(0.95 + i));

			t.insert(htblColNameValue);
		}
        
        // insert values from 200 to 300
        for (int i = 200; i < 300; i++) {
			Hashtable<String, Object> htblColNameValue = new Hashtable();
			htblColNameValue.put("id", new Integer(i));
			htblColNameValue.put("name", new String("Ahmed Noor " + i));
			htblColNameValue.put("gpa", new Double(0.95 + i));

			t.insert(htblColNameValue);
		}
        //System.out.println(t);

        // delete many 10..20.30
    }
    static void test_table_class() throws IOException, DBAppException {
		String tableName = "test table";
		String clusteringKey = "id";
		int max_page_size = 128;
		Hashtable<String, String> colNameMin ;
		Hashtable<String, String> colNameMax ;
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

		htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.double");

		Table2 t = new Table2(tableName, clusteringKey, htblColNameType, htblColNameMax, htblColNameMax,true);
		System.out.println("<<Empty Table>>");
		System.out.println(t);
		Hashtable htblColNameValue = new Hashtable();
		htblColNameValue.put("id", new Integer(2343432));
		htblColNameValue.put("name", new String("Ahmed Noor"));
		htblColNameValue.put("gpa", new Double(0.95));

		t.insert(htblColNameValue);
		System.out.println("<<Table with entrie>>");
		System.out.println(t);

		for (int i = 0; i < max_page_size; i++) {
			htblColNameValue = new Hashtable();
			htblColNameValue.put("id", new Integer(i));
			htblColNameValue.put("name", new String("Ahmed Noor " + i));
			htblColNameValue.put("gpa", new Double(0.95 + i));

			t.insert(htblColNameValue);
		}
		System.out.println("<<Table with 1 page maxed out >>");
		System.out.println(t);

		htblColNameValue = new Hashtable();
		htblColNameValue.put("id", new Integer(max_page_size));
		htblColNameValue.put("name", new String("karim in page 2 "));
		htblColNameValue.put("gpa", new Double(1.00));

		t.insert(htblColNameValue);

		System.out.println("<<Table with 2 pages  cuz of overflow>>");
		System.out.println(t);

		htblColNameValue = new Hashtable();
		htblColNameValue.put("id", new Integer(max_page_size+1));
		htblColNameValue.put("name", new String("karim in page 2 "));
		htblColNameValue.put("gpa", new Double(1.00));

		t.insert(htblColNameValue);

		System.out.println("<<Table with 2 pages  cuz of overflow>>");
		System.out.println(t);
		
		for (int i = 0; i < 200; i++) {
			htblColNameValue = new Hashtable();
			htblColNameValue.put("id", new Integer(i));
			htblColNameValue.put("name", new String("Ahmed Noor " + i));
			htblColNameValue.put("gpa", new Double(0.95 + i));

			t.insert(htblColNameValue);
		}
		System.out.println("<<Table with few page maxed out pages and also a very agressive input algorithm>>");
		System.out.println(t);
		
		htblColNameValue = new Hashtable();
		htblColNameValue.put("id", new Integer(11));
		htblColNameValue.put("name", new String("assme"));
		htblColNameValue.put("gpa", new Double(0.9));

		t.insert(htblColNameValue);
		
		System.out.println("<<Table with few page maxed out pages and also a very agressive input algorithm>>");
		System.out.println(t);

		
    }
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
		for (Tuple t1 : this.contents) {
            Page p = null;
            try {
                p = Page.DeserializePage(t1.path);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            str += p;
			}
		

		str += "||||||||||||||||||||     |||||||||||||||||||| \n \n";
		return str;
	}


    public boolean update(Comparable key, Hashtable<String, Object> newrecord) throws DBAppException {
       
        try {
            Tuple min_pagePath = get_page_to_insert_into(key);
            String page_path = min_pagePath.path;
            Page page;
            page = Page.DeserializePage(page_path);
            Object update_min = page.update(key, newrecord);
            min_pagePath.key = update_min;
            if(page.isEmpty()){
                page.remove_from_disk();
                this.contents.remove(min_pagePath); // delete refrences to this page
            }
            this.updateIndecies(newrecord, page, tableOperations.UPDATE);
            this.SerializeTable();
            return true;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            throw new DBAppException("IO problem while updating");
        }
       
    }
    

}
