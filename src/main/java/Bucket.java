import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class Bucket implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = -4330099911369490651L;
	static String basic_directory_path = "src/main/resources/Buckets";
    static int uniqueIDGenerator = 0;
    int uniqueID ;
    Vector<BucketRecord> content = new Vector<>();
    public String[] columns_of_keys_of_each_bucket_record;
    public int max_cap;
    public String path;

    private Bucket(){
        uniqueID = uniqueIDGenerator++;
        path = this.basic_directory_path + "/"+"bucket_"+uniqueID+".ser";
    }
    public Bucket(String[] columns_of_bucket_key) throws IOException {
        this();   
        this.columns_of_keys_of_each_bucket_record = columns_of_bucket_key;  
        this.max_cap = get_max_capacity_of_bucket_from_metadata_file();  
    }
    public Bucket(Bucket parent_bucket) {
    this();
     this.columns_of_keys_of_each_bucket_record = parent_bucket.columns_of_keys_of_each_bucket_record;
     this.max_cap = parent_bucket.max_cap;

    }
    private int get_max_capacity_of_bucket_from_metadata_file() throws IOException {
    	FileInputStream fis = new FileInputStream("src/main/resources/DBApp.config");
        Properties props = new Properties();
        props.load(fis);
        int bucketmax = Integer.parseInt(props.getProperty("MaximumKeysCountinIndexBucket"));
        return bucketmax;
    }
    String overflow_bucket_path = null;// = path_to_bucket
   
      void search_records(Record r, Page p){
          // TODO
      }
      void insert_in_bucket(Hashtable<String, Object> r, Page p) throws DBAppException // inserts into overflows recersivly
      , IOException
      {
          BucketRecord new_bucket_record = new BucketRecord(r, this.columns_of_keys_of_each_bucket_record, p);
          if(content.size() >= max_cap){ // if the Bucket is full
             if(this.overflow_bucket_path != null){ // if overflow bucket exists
                Bucket overflow_bucket = this.getOverflowBucket();
                overflow_bucket.insert_in_bucket(r, p);
             }else{ // create overflow bucket then insert in it
                Bucket overflow_bucket = new Bucket(this);
                overflow_bucket.insert_in_bucket(r, p);
                String path_of_saved_overflow_bucket=overflow_bucket.serialize();
                this.overflow_bucket_path = path_of_saved_overflow_bucket;
            }
          } else { // normally insert without sort or anything
            this.content.add(new_bucket_record);
          }
          this.serialize();          
      }
    public void delete_from_bucket(Hashtable<String, Object> colNameValue, Page p) throws IOException, DBAppException {
        // scan every bucketRecrod and then just return when something mathches delete it 
        for (BucketRecord record: this.content) {
            if(record.has_same_primary_key(colNameValue)){
                this.content.remove(record);
                return;
            }    
        }
        // if not found check overflow
        if(this.overflow_bucket_path!=null){
            Bucket overflow_Bucket = Bucket.Decerialize(this.overflow_bucket_path);
            overflow_Bucket.delete_from_bucket(colNameValue, p);
        } else {
            throw new DBAppException("can not delete record " + colNameValue + " from bucket as it does not exist");
        }
    }
    public String serialize() throws IOException, DBAppException {
            //Saving of object in a file
            String filename = this.path;
            File directory_createor = new File(filename);
            if (directory_createor.getParentFile().mkdirs() || directory_createor.getParentFile().exists()) {
                directory_createor.createNewFile();
            } else {
                throw new DBAppException("Failed to create directory for the pages " + directory_createor.getParent());
            }
            FileOutputStream file = new FileOutputStream(filename);
            ObjectOutputStream out = new ObjectOutputStream(file);
              
            // Method for serialization of object
            out.writeObject(this);
              
            out.close();
            file.close();
            return filename;
              
            
    }
    private Bucket getOverflowBucket() throws DBAppException {
        try
        {   
            // Reading the object from a file
            FileInputStream file = new FileInputStream(this.overflow_bucket_path);
            ObjectInputStream in = new ObjectInputStream(file);  
            // Method for deserialization of object
            Bucket overflow = (Bucket)in.readObject();       
            in.close();
            file.close();     
            return overflow;  
        }catch(Exception e){
            throw new DBAppException("can not read the overflow bucket file");
        } 
        
      
    }
    @Override
    public String toString() {
        String str = "==== " + Arrays.toString(columns_of_keys_of_each_bucket_record) + " ===\n";

        for (BucketRecord entry : this.content) {
            str += entry + "\n";
        }
        if(this.overflow_bucket_path!= null){
            str += "||  ||   || Overflow || || ||\n";
            try {
                str += getOverflowBucket();
            } catch (DBAppException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return str + "----------- DONE ------------\n-----------------------------\n";
    }
   public static void main(String[] args) throws Exception {
       String[] str = new String[]{"id", "name", "age"};
       Bucket b = new Bucket(str);
       for (int i = 0; i < 10; i++) {
        Hashtable htblColNameValue = new Hashtable();
        htblColNameValue.put("id", new Integer(i));
        htblColNameValue.put("name", new String("Ahmed Noor " + i));
        htblColNameValue.put("gpa", new Double(0.95 + i));
         
           b.insert_in_bucket(htblColNameValue, null);
       }
       System.out.println(b);
       for (int i = 0; i < 50; i++) {
        Hashtable htblColNameValue = new Hashtable();
        htblColNameValue.put("id", new Integer(i));
        htblColNameValue.put("name", new String("Ahmed Noor " + i));
        htblColNameValue.put("gpa", new Double(0.95 + i));

           b.insert_in_bucket(htblColNameValue, null);
       }
       System.out.println(b);
   }
public static Bucket Decerialize(String path_to_bucket) throws IOException {
    String filename = path_to_bucket;
    Bucket bucket = null;

            FileInputStream file = new FileInputStream(filename);
            ObjectInputStream in = new ObjectInputStream(file);

            try {
                bucket = (Bucket)in.readObject();
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            in.close();
            file.close();
    return bucket;
}
public Vector<String> get_all_pages() throws IOException {
    Vector<String> res = new Vector<>();
	for (BucketRecord bucket_Record : this.content){
        res.add(bucket_Record.page_path);
    }
    Vector<String> remainig ;
    if(this.overflow_bucket_path!=null){
        Bucket over_Bucket = Bucket.Decerialize(this.overflow_bucket_path);
        remainig = over_Bucket.get_all_pages();
    }else {
        remainig = new Vector<>();
    }
    res.addAll(remainig);
    return res;
}

   }
   