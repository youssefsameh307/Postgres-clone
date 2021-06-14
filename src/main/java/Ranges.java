import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

public class Ranges {
	
	public static List getRange(Object min, Object max) throws IOException, DBAppException {
        if(min instanceof Integer)
            return Arrays.asList(getRange((Integer)min, (Integer)max));
        if(min instanceof Double)
            return Arrays.asList(getRange((Double)min, (Double)max));
        if(min instanceof String)
            return Arrays.asList(getRange((String)min, (String)max));
        if(min instanceof Date)
            return Arrays.asList(getRange((Date)min, (Date)max));
        throw new DBAppException("error in getting ranges");
    }

    public static Integer[] getRange(Integer min, Integer max) {
        Integer difference = max - min;
        Integer increment = difference/10;
        Vector<Hashtable<String, Integer>> result = new Vector<Hashtable<String, Integer>>(10);

        Integer currentMin = min;
        Integer currentMax = min + increment;
        Hashtable<String, Integer> current = new Hashtable<String, Integer>();
        current.put("min", min);
        current.put("max", currentMax);
        result.insertElementAt(current, 0);
        //
        Integer[] retArr = new Integer[10];
        retArr[0]=currentMax;
        //
        currentMin = currentMax + 1;
        currentMax = min + increment*(2);

        for(int i=1; i<9; i++) {
            current = new Hashtable<String, Integer>();
            current.put("min", currentMin);
            current.put("max", currentMax);
            result.insertElementAt(current, i);
            //
            retArr[i]=currentMax;
            //
            currentMin = currentMax + 1;
            currentMax = min + increment*(i+2);
        }
        current = new Hashtable<String, Integer>();
        current.put("min", currentMin);
        current.put("max", max);
        result.insertElementAt(current, 9);
        //
        retArr[9]=max;
        //
//		return result;
        return retArr;
    }

    public static Double[] getRange(Double min, Double max) {
        Double difference = max - min;
        Double increment = difference/10;
        Vector<Hashtable<String, Double>> result = new Vector<Hashtable<String, Double>>(10);
        Double currentMin = min;
        Double currentMax = min + increment;
        Hashtable<String, Double> current = new Hashtable<String, Double>();
        current.put("min", min);
        current.put("max", currentMax);
        result.insertElementAt(current, 0);
        //
        Double[] retArr = new Double[10];
        retArr[0]=currentMax;
        //
        currentMin = currentMax + 1e-10;
        currentMax = min + increment*(2);
        for(int i=1; i<9; i++) {
            current = new Hashtable<String, Double>();
            current.put("min", currentMin);
            current.put("max", currentMax);
            result.insertElementAt(current, i);
            //
            retArr[i]=currentMax;
            //
            currentMin = currentMax + 1e-10;
            currentMax = min + increment*(i+2);
        }
        current = new Hashtable<String, Double>();
        current.put("min", currentMin);
        current.put("max", max);
        result.insertElementAt(current, 9);
        //
        retArr[9]=max;
        //
        return retArr;
    }

    public static Long[] getRange(String min, String max) throws IOException {
        Long minHashCode = hashCode(min.toLowerCase());
        Long maxHashCode = hashCode(max.toLowerCase());
        Long difference = maxHashCode - minHashCode;
        Long increment = difference/10;
        Vector<Hashtable<String, Long>> result = new Vector<Hashtable<String, Long>>(10);

        Long currentMin = minHashCode;
        Long currentMax = minHashCode + increment;
        Hashtable<String, Long> current = new Hashtable<String, Long>();
        current.put("min", minHashCode);
        current.put("max", currentMax);
        result.insertElementAt(current, 0);
        //
        Long[] retArr = new Long[10];
        retArr[0]=currentMax;
        //
        currentMin = currentMax + 1;
        currentMax = minHashCode + increment*(2);
        for(int i=1; i<9; i++) {
            current = new Hashtable<String, Long>();
            current.put("min", currentMin);
            current.put("max", currentMax);
            result.insertElementAt(current, i);
            //
            retArr[i]=currentMax;
            //
            currentMin = currentMax + 1;
            currentMax = minHashCode + increment*(i+2);
        }
        current = new Hashtable<String, Long>();
        current.put("min", currentMin);
        current.put("max", maxHashCode);
        result.insertElementAt(current, 9);
        //
        retArr[9]=maxHashCode;
        //
        return retArr;
    }

    @SuppressWarnings("deprecation")
    public static Date[] getRange(Date min, Date max) throws IOException {
        int yearMin = min.getYear();
        int yearMax = max.getYear();

        int monthMin = min.getMonth();
        int monthMax = max.getMonth();

        int dayMin = min.getDate();
        int dayMax = max.getDate();

        int yearDiff = (yearMax - yearMin)/10;
        int monthDiff = (monthMax - monthMin)/10;
        int dayDiff = (dayMax - dayMin)/10;

        Vector<Hashtable<String, Date>> result = new Vector<Hashtable<String, Date>>(10);

        Date currentMin = min;
        Date currentMax = new Date(yearMin+yearDiff, monthMin+monthDiff, dayMin+dayDiff);
        Hashtable<String, Date> current = new Hashtable<String, Date>();
        current.put("min", min);
        current.put("max", currentMax);
        result.insertElementAt(current, 0);
        //
        Date[] retArr = new Date[10];
        retArr[0]=currentMax;
        //
        currentMin = new Date(currentMax.getYear(), currentMax.getMonth(), currentMax.getDate()+1);
        currentMax = new Date(yearMin+2*yearDiff, monthMin+2*monthDiff, dayMin+2*dayDiff);
        for(int i=1; i<9; i++) {
            current = new Hashtable<String, Date>();
            current.put("min", currentMin);
            current.put("max", currentMax);
            result.insertElementAt(current, i);
            //
            retArr[i]=currentMax;
            //
            currentMin = new Date(currentMax.getYear(), currentMax.getMonth(), currentMax.getDate()+1);
            currentMax = new Date(yearMin+(i+2)*yearDiff, monthMin+(i+2)*monthDiff, dayMin+(i+2)*dayDiff);
        }
        current = new Hashtable<String, Date>();
        current.put("min", currentMin);
        current.put("max", max);
        result.insertElementAt(current, 9);
        //
        retArr[9]=max;
        //
        return retArr;
    }

    public static long hashCode(String s) {
        long result = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            result += c*Math.pow(32, s.length() - i - 1);
        }
        return result;
    }

    private static void test_ranges() {
        
    }
	public static Hashtable<String,Integer> getPos (Hashtable<String,Comparable> value,Hashtable<String,List> colranges) {
        Hashtable<String,List> ranges = colranges;
        Hashtable<String,Integer> position= new Hashtable<>();
        for (String colName:value.keySet()){
            Comparable colValue = value.get(colName);
            List range = colranges.get(colName);
            Integer searchRes=Collections.binarySearch(range,colValue);
            // if conditions short hand format
            int colPos = (searchRes<0)?  -1-searchRes  :  searchRes;
            //
            position.put(colName,colPos);
        }
        return position;
    }
	public static void main(String[] args) throws InterruptedException, IOException, DBAppException {
        Hashtable<String,Comparable> value = new Hashtable<>();
        Hashtable<String,List>colranges=new Hashtable<>();
        value.put("id",13);
        value.put("name",hashCode("3"));
        Integer [] intRanges = {10,20,30,40};
        List intList = Arrays.asList(intRanges);
        List stringList = getRange((Object)"aaa",(Object) "zzz");
        colranges.put("id",intList);
        colranges.put("name",stringList);
		System.out.println(colranges.get("name"));
        System.out.println(getPos(value,colranges));

		//[a, a, aaa] aaa -> [aab, aac, bbt]bbb -> []ccc
    }
	
//	public static byte[] add(byte[] b1, byte[] b2) {
//		byte[] result = new byte[b1.length];
//		
//		for(int i=0; i<result.length; i++)
//			result[i] = (byte) (b1[i] + b2[i]);
//		
//		return result;
//	}
//	
//	public static byte getCorrectByte(byte min, byte max, byte difference, byte correct) {
//		if(correct <= max && correct >= min)
//			return correct;
//		else{
//			if(correct > max)
//				return getCorrectByte(min, max, difference, (byte) (correct%max));
//			else //if(correct<min)
//				return getCorrectByte(min, max, difference, (byte) (correct+min));
//		}
//	}
//	
//	public static String[] getDifference(String min, String max) throws IOException {
//	if(min.split("-").length>1) {
//		ObjectMapper mapper = new CBORMapper();
//		
//		String min1 = min.split("-")[0];
//		String min2 = min.split("-")[1];
//		String max1 = max.split("-")[0];
//		String max2 = max.split("-")[1];
//		
//		byte[] min1Byte = mapper.writeValueAsBytes(min1);
//		byte[] min2Byte = mapper.writeValueAsBytes(min2);
//		byte[] max1Byte = mapper.writeValueAsBytes(max1);
//		byte[] max2Byte = mapper.writeValueAsBytes(max2);
//		byte[] difference1Byte = new byte[max1Byte.length];
//		byte[] difference2Byte = new byte[max2Byte.length];
//
//		for(int i=0; i<difference1Byte.length; i++) {
//			difference1Byte[i] = (byte) (max1Byte[i] - min1Byte[i]);
//		}
//		
//		for(int i=0; i<difference2Byte.length; i++) {
//			difference2Byte[i] = (byte) (max2Byte[i] - min2Byte[i]);
//		}
//		
//		String[] result1 = new String[10];
//		String[] result2 = new String[10];
//		String[] result = new String[10];
//		
//		for(int i=0; i<result.length; i++) {
//			byte[] temp1 = new byte[difference1Byte.length];
//			byte[] temp2 = new byte[difference2Byte.length];
//
//			temp1[0] = max1Byte[0];
//			for(int j=1; j<difference1Byte.length; j++) {
//				temp1[j] = (byte) (min1Byte[j] + difference1Byte[j]*((2*i+1)/10));
//			}
//			temp2[0] = max2Byte[0];
//			for(int j=1; j<difference2Byte.length; j++) {
//				temp2[j] = (byte) (min2Byte[j] + difference2Byte[j]*((2*i+1)/10));
//			}
//			
//			result1[i] = mapper.readValue(temp1, String.class);
//			result2[i] = mapper.readValue(temp2, String.class);
//			result[i] = result1[i]+"-"+result2[i];
//		}
//		
//		return result;
//	}
//	
//	else {
//		ObjectMapper mapper = new CBORMapper();
//		byte[] minByte = mapper.writeValueAsBytes(min);
//		byte[] maxByte = mapper.writeValueAsBytes(max);
//		byte[] differenceByte = new byte[maxByte.length];
//		
//		for(int i=0; i<differenceByte.length; i++) {
//			differenceByte[i] = (byte) (maxByte[i] - minByte[i]);
//		}
//		
//		String[] result = new String[10];
//		
//		for(int i=0; i<10; i++) {
//			byte[] resultByte2 = new byte[maxByte.length];
//			for(int j=0; j<resultByte2.length; j++) {
//				resultByte2[j] = (byte) (minByte[j] + differenceByte[j]*(i+1)/10);
//			}
//			result[i] = mapper.readValue(resultByte2, String.class);
//		}
//		
//		return result;
//			
////	}
//	
//}
	
}