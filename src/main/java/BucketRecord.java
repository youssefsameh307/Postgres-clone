import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map.Entry;

public class BucketRecord implements Serializable {
     /**
	 * 
	 */
	private static final long serialVersionUID = -5727604034601344480L;
	Hashtable<String, Object> key = new Hashtable<>();
     private Hashtable<String, Object> actuall_record ;
     // [name:karim, age: 19, -> path]
    public String page_path;
    public BucketRecord(Hashtable<String, Object> colNameValue, String[] columns_of_keys_of_each_bucket_record, Page p) {
        for (String col_name : columns_of_keys_of_each_bucket_record) {
            key.put(col_name, colNameValue.get(col_name));
        }
        page_path = p.path;
        actuall_record = colNameValue;
    }

@Override
public String toString() {
    return ""+key+"\n";
}

public boolean equals_record(Hashtable<String, Object> record){
    boolean answer = true; // {id:30} -> {id"30, name:karim}

    return record.equals(actuall_record);
}
public boolean has_same_primary_key(Hashtable<String, Object> primry_key){
    for (Entry<String, Object> key_value : primry_key.entrySet()) {
        String cluster_key_name = key_value.getKey();
        Comparable value = (Comparable) key_value.getValue();
        Object value_in_this_record = this.actuall_record.get(cluster_key_name);
        if(value.compareTo(value_in_this_record) == 0){
            return true;
        }
    }
    return false;
}
public static void main(String[] args) throws IOException, DBAppException {
    Hashtable<String, Object> col = new Hashtable<>();
    col.put("name", "karim");
    col.put("id", 15);
    col.put("age", 16);
    Page p = null;

    BucketRecord b = new BucketRecord(col, new String[]{"name", "id"}, p);
    Hashtable<String, Object> col2 = new Hashtable<>();
    col2.put("name", "karim");
    col2.put("id", 15);
    col2.put("age", 16);

}
}