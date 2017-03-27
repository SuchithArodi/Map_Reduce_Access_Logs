import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * 
 * This class extends the MapReduce class to implement 
 * the solution for analysing log files.
 * 
 * @author Rajashree Krishnadevamaiya
 * @author Suchith Chandrashekara Arodi
 * @author Dipesh Nainani
 *
 */
public class WebLog extends MapReduce {

	// Data structures used in post processing
	
	static Map<String, List<String>> IPAddressLog = new HashMap<String, List<String>>();
	static Map<String, List<String>> IPAddressCount = new HashMap<String, List<String>>();
	static Map<String, Integer> IPCountLog = new HashMap<String, Integer>();

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);
		System.out.println("enter number of files");
		int numOfFiles = sc.nextInt();
		String[] filename = new String[numOfFiles];
		sc = new Scanner(System.in);
		System.out.println("enter the path of " + numOfFiles + " files");
		
		for(int i = 0 ; i<filename.length;i++)
		{
			String line = sc.nextLine();
			filename[i] = line;
		}
		
		for(int i = 0 ; i<filename.length;i++)
		{
			System.out.println(filename[i]);
			WebLog w = new WebLog();
			w.execute(filename[i]);
			TreeMap<String, LinkedList<String>> reducerResults = w.getResults();
			postProcess(reducerResults);	
		}
		
		Map<String, Integer> IPCounts = new HashMap<String, Integer>();
		/** 
		 * The commented out line considers the IP addresses 
		 * accessed by accounts multiple times
		 * **/
//		for (Entry<String, List<String>> entry : IPAddressCount.entrySet()) {
		
		/* This for loop traverses through the post processed 
		   results to sort and print the final results */
		for (Entry<String, List<String>> entry : IPAddressLog.entrySet()) {
			String key = entry.getKey();
			List<String> value = entry.getValue();
			IPCounts.put(key, value.size());
		}
		
		// Get map values as entry set to sort by values
		Set<Entry<String, Integer>> entrySet = IPCounts.entrySet();
		
		List<Entry<String, Integer>> listToSort = new ArrayList<Entry<String, Integer>>(
				entrySet);
		
		Collections.sort(listToSort, new Comparator<Map.Entry<String, Integer>>() {
			@ Override
			public int compare(Map.Entry<String, Integer> value1,
					Map.Entry<String, Integer> value2) {
				return (value2.getValue()).compareTo(value1.getValue());
			}
		});
		
		/**
		 * The commented out code in this section was to get IP address count taking 
		 * the count ties to account. This was producing large results to print on the console.
		 * This output is written into a file. 
		 * **/
		int count = 0;
//		int prev = listToSort.get(0).getValue();
//		System.out.println("#occurrence " + count + " is: " + listToSort.get(0).getKey());
//		System.out.println("Accounts accessed: "+ IPAddressLog.get(listToSort.get(0).getKey()));
		
		/* This results prints the top 15 IP addresses 
		 * without taking count ties into consideration*/
		for (int i = 0; i< listToSort.size();i++) {
			String key = listToSort.get(i).getKey();
			int value = listToSort.get(i).getValue();
			if(count<15){
				int j = count;
				System.out.println("#occurrence " + j + " is: " + key);
				System.out.println("Accounts accessed: "+ IPAddressLog.get(key));
				count++;
			}else {
//				int j = count;
//				System.out.println("#occurrence " + j + " is: " + key);
//				System.out.println("Accounts accessed: "+ IPAddressLog.get(key));
				break;
			}
		}

	}

	/**
	 * 
	 * Method to post process the output from reducer to a desired format
	 * 
	 * @param reducerResults: The result from the reducer
	 */
	private static void postProcess(
			TreeMap<String, LinkedList<String>> reducerResults) {
		for (Entry<String, LinkedList<String>> entry : reducerResults
				.entrySet()) {
			String key = entry.getKey();
			LinkedList<String> value = entry.getValue();
			if(IPAddressCount.containsKey(key)) {
				List<String> val = IPAddressCount.get(key);
				val.addAll(value);
				IPAddressCount.put(key, val);
			}
			else {
				IPAddressCount.put(key, value);
			}
			// Convert list to set to remove duplicate account names.
			Set<String> s = new HashSet<String>(value);
			
			//Convert back to list and re-write in the IPAddress Log.
			LinkedList<String> newValue = new LinkedList<String>(s);
			if(IPAddressLog.containsKey(key)){
				List<String> first_list = new ArrayList<>(newValue);
				List<String> second_list = IPAddressLog.get(key);
				first_list.removeAll(second_list);
				second_list.addAll(first_list);
				IPAddressLog.put(key, second_list);
			}else{
				IPAddressLog.put(key, newValue);
			}	
		}
	}

	@Override
	public void mapper(String var1) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(var1));
			String line;
			while ((line = br.readLine()) != null) {

				String[] elements = line.split(" ");
				
				String pattern = "~[A-Za-z0-9]*";
				Pattern regex = Pattern.compile(pattern);
				Matcher accountName = regex.matcher(elements[6]);
				while (accountName.find()) {
					String name = accountName.group();
					emit_intermediate(elements[0], name.substring(1));
				}
			}

		} catch (Exception e) {

		}

	}

	@Override
	public void reducer(String var1, LinkedList<String> var2) {
		for (String value : var2) {
			emit(var1, value);
		}
	}

}
