import java.io.Serializable;

public class Tuple implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = 5909240917415701317L;
	public Object key;
    public String path;

    public Tuple(Object min, String path){
        this.key = min;
        this.path = path;
    }
}
