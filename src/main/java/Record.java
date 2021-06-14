import java.util.Hashtable;

public class Record {
    Hashtable<String, Object> values_in_record ;
       /*{
        "name" : "kiko",
        "age": 19,
        }*/
    
        Page continge_record; //Path of page
        Object primay_key_value; // example = 19 <-- this is the ID of the student
        
        @Override
        public String toString() {
            // TODO this is a sudo method only for testing puroposes
           return "{name: 5aled, age:15}";
        }
        }