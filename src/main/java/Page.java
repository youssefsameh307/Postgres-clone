import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class Page implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = -2671538668274311914L;
	String overflow_page ;
    String next_page ;
    private int maxsize;
    Vector<Hashtable<String, Object>> contents = new Vector<>();
    static int uniqueIDGenerator = 0;
    int uniqueID;
    private String clusteringKey;
    public Comparable min;
    private Comparable max;
    private String parentName;
    private static String base_path = "src/main/resources/data";
    public String path ;
    public String parent_table_name ;
    public Page(Table parent) throws IOException, DBAppException {
        this.uniqueID = uniqueIDGenerator++;
        maxsize = get_max_size_from_config_file();
        this.clusteringKey = parent.clusteringKey;
        this.parent_table_name = parent.tableName;
        path =this.base_path + "/"+parent_table_name+"/" + uniqueID+".ser";
    }
    public Page(Page page2) throws IOException, DBAppException {
        this.uniqueID = uniqueIDGenerator++;
        maxsize = get_max_size_from_config_file();
        this.clusteringKey = page2.clusteringKey;
        this.parent_table_name = page2.parent_table_name;
        path =this.base_path + "/"+parent_table_name+"/" + uniqueID+".ser";
        this.next_page = page2.next_page;
    }
    public Page(Hashtable<String, Object> record, Table table) throws IOException, DBAppException {
        this(table);
        this.insert(record);
    }
    private int get_max_size_from_config_file() throws IOException {
        FileInputStream fis = new FileInputStream("src/main/resources/DBApp.config");
        Properties props = new Properties();
        props.load(fis);
        int pagemax = Integer.parseInt(props.getProperty("MaximumRowsCountinPage"));
        return pagemax;
    }
    public boolean isFull(){
        return contents.size()==maxsize;
    }
    public Comparable insert(Hashtable<String, Object> record) throws IOException, DBAppException {
        if(isFull()){
            if(next_page != null){ // if i have a next page 
                // insret in overflow page
                if(overflow_page != null){
                    Page overflow = Page.DeserializePage(overflow_page);
                    overflow.next_page = this.next_page;
                    overflow.insert(record);
                } else {
                    // create overflow page
                    Page overflow = new Page(this);
                    this.overflow_page = overflow.path;
                    overflow.next_page = this.next_page;
                    overflow.insert(record);
                   
                }
            } else {
                Page next = new Page(this);
                next.insert(record);
                this.next_page = next.path;
            }
        } else {
            int size = contents.size();
            Comparable id = (Comparable) record.get(this.clusteringKey);
            if(id==null){
                throw new DBAppException("no clustering key was provided of type " + this.clusteringKey);
            }
            boolean inserted=false;
            for (int i = 0; i < size; i++) {
                Comparable x = (Comparable) contents.get(i).get(this.clusteringKey);
                if (x.compareTo(id) < 0) 
                    continue;
                contents.add(i, record);
                inserted=true;
                break;
            }
            if (!inserted)
                contents.add(record);
        }

        this.update_page();
        this.SerializePage();
        return min;

    }

    String SerializePage() throws IOException, DBAppException {
        
        File directory_createor = new File(path);
        // System.out.println(directory_createor.getParentFile());
         if (directory_createor.getParentFile().mkdirs() || directory_createor.getParentFile().exists()) {
             directory_createor.createNewFile();
         } else {
             throw new DBAppException("Failed to create directory for the pages " + directory_createor.getParent());
         }
         FileOutputStream file = new FileOutputStream(path);
         ObjectOutputStream out = new ObjectOutputStream(file);

         out.writeObject(this);
         out.close();
         file.close();

         //System.out.println("Page serialized to path:" + path);
         return path;
    }
    public static Page DeserializePage(String path) throws IOException {

        Page page = null;

            FileInputStream file = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(file);

            try {
                page = (Page)in.readObject();
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            in.close();
            file.close();
        return page;
    }
	public Comparable delete(Comparable key) throws IOException, DBAppException {
		for (Hashtable<String,Object> record : contents) {
            Object o = record.get(this.clusteringKey);
            if(key.compareTo(o) == 0){
                this.contents.remove(record);
                update_page();
                this.SerializePage();
                return this.min;
            }
        }
        if(this.overflow_page!=null){
            Page p = Page.DeserializePage(this.overflow_page);
                p.delete(key);
                if(p.isEmpty()){
                p.remove_from_disk();
                this.overflow_page = null;
                }
        }else{
            throw new DBAppException("no such record exists with clustering key = " + key);
        }
        
        update_page();
        this.SerializePage();
        return this.min;
	}
   private void update_page() {
       if(this.contents.size() != 0){
    min = (Comparable) contents.firstElement().get(this.clusteringKey);
    max = (Comparable) contents.lastElement().get(this.clusteringKey);
       }
    }
        void remove_from_disk() {
            String path = this.path;
            File file = new File(path);
            if(file.delete())
            {
                System.out.println("File deleted successfully");
            }
            else
            {
                System.out.println("Failed to delete the file");
            }
        }
    boolean isEmpty() throws IOException {
        if(this.overflow_page!=null){
            Page p = Page.DeserializePage(this.overflow_page);
            return p.isEmpty();
        }
        return this.contents.isEmpty();
    }
    @Override
    public String toString() {
        String str = "Page " + uniqueID + " min " + this.min + " max " + this.max +" overflow " + (this.overflow_page!=null)+"\n";
		for (Hashtable<String, Object> row : this.contents) {
			str += row + "\n";
		} 
        if(overflow_page!= null){
            str += "||||||||  OVERFLOW ||||||||\n";
            try {
                Page overflow = DeserializePage(this.overflow_page);
                str += overflow;
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
           
        }
        str += "--------------DONE-----------\n";
		return str;
    }
    public static Page DeserializePage(String string, Table table) throws IOException {
        return Page.DeserializePage(string);
    }
    public Vector<Hashtable<String, Object>> get_all_records() throws IOException {
        Vector<Hashtable<String, Object>> remaining = new Vector<>();
        if(this.overflow_page != null){
            Page overflow = Page.DeserializePage(this.overflow_page);
            remaining = overflow.get_all_records();
        }
        remaining.addAll(this.contents);
        return remaining;
    }
    public Object update(Comparable key, Hashtable<String, Object> newrecord) throws IOException, DBAppException {
        for (Hashtable<String,Object> record : contents) {
            Object o = record.get(this.clusteringKey);
            if(key.compareTo(o) == 0){
                this.contents.remove(record);
                this.insert(newrecord);
                update_page();
                this.SerializePage();
                return this.min;
            }
        }
        if(this.overflow_page!=null){
            Page p = Page.DeserializePage(this.overflow_page);
                p.delete(key);
                if(p.isEmpty()){
                p.remove_from_disk();
                this.overflow_page = null;
                }
        }else{
            throw new DBAppException("no such record exists with clustering key = " + key);
        }
        
        update_page();
        this.SerializePage();
        return this.min;

    }
}
