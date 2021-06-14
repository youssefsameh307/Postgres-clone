import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class ndIndex implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 5115422509248844484L;
	private ndArray content;

    public ndIndex(int num_of_dim) {
        int[] dims = new int[num_of_dim];
        for (int i = 0; i < dims.length; i++) {
            dims[i] = 10;
        }

        this.content = new ndArray(dims);
    }

    void set(int[] index, String inserted_value){
        this.content.set(index, inserted_value);
    }
    
    String get(int[] index){
        return (String) this.content.get(index); // TODO: validate the bucet 
    }

    ArrayList<Object> get_set(int[] index){
        return this.content.get_set(index);
    }

    public Vector<String> get_set(Vector<Vector<Integer>> indecies_of_each_colum) {
        Vector<Object> res = this.content.get_set(indecies_of_each_colum);
        Vector<String> ans = new Vector<>();
        for (Object object : res) {
            ans.add((String)object);
        }
        return ans;
    }
    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return this.content.toString();
    }
}
