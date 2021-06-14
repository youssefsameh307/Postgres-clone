import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Vector;

import javax.naming.spi.DirStateFactory.Result;

public class ndArray implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = 4985973534373619725L;
	Vector<Integer> dim;
    Vector<Object> content;
    ndArray(int[] dim){
        this.dim = get_vector_from_array(dim);
        int max_pos = 1;
        for (int i : dim) {
            max_pos *= i;
        }
        final int max_size = max_pos;
        this.content = new Vector<Object>(){{setSize(max_size);}};
    }

    public static Vector<Integer> get_vector_from_array(int[] dim) {
        Vector<Integer> temp = new Vector<>();
        for (int i : dim) {
            temp.add(i);
        }
        return temp;
    }

    public static void main(String[] args) {
        // the point is to create a function that when given coordinates to a tensor can get its value easly
        int[][][][] arr = new int[10][10][10][10]; 
        ndArray my_arr = new ndArray(new int[]{10,10,10, 10}); // create array with 10 rows, 10 cols, 10 values in each row/col create 3d tensor with dim 10,10,10
        int count = 0;
        for (int k = 0; k < 10; k++) {
            for(int i= 0; i < 10; i++){
                for (int j = 0; j < 10; j++) {
                    for (int j2 = 0; j2 < 10; j2++) {
                        arr[k][i][j][j2] = count;
                        my_arr.set(new int[]{k, i, j, j2}, count);
                        count++;
                    }
                }
            }
        }
        Vector<Vector<Integer>> mainVec = new Vector<>();
        Vector<Integer> temp1=new Vector<>();
        temp1.add(1);
        temp1.add(2);
        Vector<Integer> temp2=new Vector<>();
        temp2.add(2);
        Vector<Integer> temp3=new Vector<>();
        temp3.add(3);
        Vector<Integer> temp4=new Vector<>();
        temp4.add(4);
        temp4.add(5);
        temp4.add(6);
        mainVec.add(temp1);
        mainVec.add(temp2);
        mainVec.add(temp3);
        mainVec.add(temp4);
        
        Vector<Object> a = my_arr.get_set(mainVec);
            for (Object object : a) {
                System.out.println(object);
            }

        int[][][] arr2 = new int[10][10][10]; 
        ndArray my_arr2 = new ndArray(new int[]{10,10,10, 10, 10}); // create array with 10 rows, 10 cols, 10 values in each row/col create 3d tensor with dim 10,10,10
        
       
            // for(int i= 0; i < 10; i++){
            //     for (int j = 0; j < 10; j++) {
            //         for (int j2 = 0; j2 < 10; j2++) {
            //             arr[k][i][j][j2] = count;
            //             my_arr.set(new int[]{k, i, j, j2}, count);
            //             count++;
            //         }
            //     }
            // }
        
      


    }
   
  

   
    ArrayList<Object> get_set(int[] index){
        /*
        index = [1, 2, -1, 5, -1]
            <[[1],[2],[0,1,2,3,4,5, 6, 7, 8, 9],[5],[[0,1,2,3,4,5, 6, 7, 8, 9]]]>
         */
        
        ArrayList<int[]> loop_index = new ArrayList<int []>();
        
        for (int i = 0; i < index.length; i++) {
            int index_value = index[i];
            if(index_value == -1){
                // create all the premutaions of this index like of the max of xol is 5 then we need to loop on 0,1,2,3,4
                int max_size_of_this_dim = this.dim.get(i);
                int[] premutaions = new int[max_size_of_this_dim];
                for(int j =0; j < max_size_of_this_dim; j++){
                    premutaions[j] = j;
                }
                loop_index.add(premutaions);
            }else {
                loop_index.add(new int[]{index_value});
            }

           
        }
        

        ArrayList<ArrayList<Integer>> indecies = getPremutaions(0, loop_index);
        ArrayList<Object> results = new ArrayList<>();
        for (ArrayList<Integer> position_index : indecies) {
            int[] position_index_in_array_form = position_index.stream().mapToInt(i -> i).toArray();//new int[position_index.size()];
           // System.arraycopy(position_index, 0, position_index_in_array_form, 0, position_index.size()); // method equivilent to int[] = position_index.toArrays() but this doesn work cuz java we keda
            results.add(this.get(position_index_in_array_form));
        }
        return results;
    }
   
    Object get(int[] index){
        int pos = get_1d_pos(index);
        return this.content.get(pos);
    }

    void set(int[] index, Object inserted_value){
        int pos = this.get_1d_pos(index);
        this.content.set(pos , inserted_value);
    }

    int get_1d_pos(int [] index){
        
        int pos = 0;
        for (int c = 0; c < index.length; c++) {
            int index_value = index[c];
            int acc = 1; // multilply by the max size for each dimenstion
            for(int starting_dim = 1+c; starting_dim < this.dim.size(); starting_dim++){
                acc *= this.dim.get(starting_dim);
            }
            acc*= index_value; // multply by index-value to offset based on dims
            pos += acc;
        }

        return pos;
    }

    static ArrayList<ArrayList<Integer>> getPremutaions(int depth,  ArrayList<int[]> index){
        if(depth == index.size()-1){
            ArrayList<ArrayList<Integer>> temp = new ArrayList<>();
            
                for (int e : index.get(depth)) {
                    ArrayList<Integer> temp2 = new ArrayList<>();
                    temp2.add(e);
                    temp.add(temp2);
                }
            
            return temp;
        }

        ArrayList<ArrayList<Integer>> res = new ArrayList<>();
        for (int element : index.get(depth)) {
            ArrayList<ArrayList<Integer>> old = getPremutaions(depth+1, index);
            for (ArrayList<Integer> arrayList : old) {
                arrayList.add(0, element);
                res.add(arrayList);
            }
        }
        return res;
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return this.content.toString();
    }

    public Vector<Object> get_set(Vector<Vector<Integer>> indecies_of_each_colum) {
        return this.get_setv(indecies_of_each_colum);
    }
    public Vector<Object> get_setv(Vector<Vector<Integer>> input ){
        /*
            <[[1],[2],[0,1,2,3,4,5, 6, 7, 8, 9],[5],[[0,1,2,3,4,5, 6, 7, 8, 9]]]>
            index = [ [1-1], [2-2], [0-9] [5-5] [0-9]]
            buckets of (<[[1],[2],[0,1,2,3,4,5, 6, 7, 8, 9],[5],[[0,1,2,3,4,5, 6, 7, 8, 9]]]>)
         */
        Vector<Hashtable<String,Integer>> index= getSetVHelper(input);
        Vector<Vector<Integer>> loop_index = new Vector<>();

        for(Hashtable<String,Integer>range:index) {
            int min = range.get("min");
            int max =range.get("max");
            //-N query <
                // create all the premutaions of this index like of the max of xol is 5 then we need to loop on 0,1,2,3,4
                Vector<Integer> temp = new Vector<>();
                for(int j =min; j <= max; j++){
                    temp.add(j);
                }
                loop_index.add(temp);
            }
            Vector<Vector<Integer>> indecies = getPremutaionsV(0, loop_index);
        Vector<Object> results = new Vector<>();
        for (Vector<Integer> position_index : indecies) {
            int[] position_index_in_array_form = position_index.stream().mapToInt(i -> i).toArray();//new int[position_index.size()];
            // System.arraycopy(position_index, 0, position_index_in_array_form, 0, position_index.size()); // method equivilent to int[] = position_index.toArrays() but this doesn work cuz java we keda
            results.add(this.get(position_index_in_array_form));
        }
        return results;
        }
        static Vector<Vector<Integer>> getPremutaionsV(int depth,  Vector<Vector<Integer>> index){
            if(depth == index.size()-1){
                Vector<Vector<Integer>> temp = new Vector<>();
    
                for (int e : index.get(depth)) {
                    Vector<Integer> temp2 = new Vector<>();
                    temp2.add(e);
                    temp.add(temp2);
                }
    
                return temp;
            }
    
            Vector<Vector<Integer>> res = new Vector<>();
            for (int element : index.get(depth)) {
                Vector<Vector<Integer>> old = getPremutaionsV(depth+1, index);
                for (Vector<Integer> arrayList : old) {
                    arrayList.add(0, element);
                    res.add(arrayList);
                }
            }
            return res;
        }
        public Vector<Hashtable<String,Integer>> getSetVHelper(Vector<Vector<Integer>> input ){
            Vector<Hashtable<String,Integer>> setInput= new Vector<>();
            for (Vector<Integer>temp:input){
                Collections.sort(temp);
                int min =temp.stream().min(Integer::compare).get();
                int max =temp.stream().max(Integer::compare).get();
                Hashtable<String,Integer> tempRange =new Hashtable<>();
                tempRange.put("min",min);
                tempRange.put("max",max);
                setInput.add(tempRange);
            }
            return setInput;
        }
}
