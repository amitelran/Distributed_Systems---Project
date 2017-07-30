package semanticSimilarity;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;


public class CooccurrencesVector implements Writable {

	// Mapping of a word as the 'key', and Map of the form <dependancy label, count of performances> as the 'value' of the mappings
	
	protected Map<String, Map<String, Integer>> CooccurMap = new HashMap<String, Map<String, Integer>>();
	protected String lexeme;


	/*********** 	Constructors	 ***********/


	public CooccurrencesVector(){}


	public CooccurrencesVector(String word) {
		this.lexeme = word;
	}



	/*********** 	Write data to DataOutput	 ***********/


	public void write(DataOutput out) throws IOException 
	{
		out.writeUTF(lexeme);

		// Convert hash map into bytes array in order to write to DataOutput

		ByteArrayOutputStream byteArrOutputStream = new ByteArrayOutputStream();
		ObjectOutput outputConvert = null;

		try {
			outputConvert = new ObjectOutputStream(byteArrOutputStream);   
			outputConvert.writeObject(CooccurMap);
			outputConvert.flush();
			byte[] byteConvertedMap = byteArrOutputStream.toByteArray();

			out.writeInt(byteConvertedMap.length); 				// 'Push' size of bytes array to know how much bytes needed to read
			out.write(byteConvertedMap);
		} 
		finally {
			try {
				byteArrOutputStream.close();
			} catch (IOException ex) {
				System.out.println("Byte Array output stream close exception occured\n");
			}
		}
	}

	

	/*********** 	Read data from DataInput	 ***********/

	

	@SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException 
	{
		lexeme = in.readUTF();
		int bytesArraySize = in.readInt();				// Read 'pushed' integer indicating size of bytes array
		
		byte[] bytesArray = new byte[bytesArraySize];
		in.readFully(bytesArray);

		/* Convert bytes array into hash map */

		ByteArrayInputStream bytesInputStream = new ByteArrayInputStream(bytesArray);
		ObjectInput inputStream = null;
		try {
			inputStream = new ObjectInputStream(bytesInputStream);
			CooccurMap = (Map<String, Map<String, Integer>>) inputStream.readObject(); 
		} 
		catch (ClassNotFoundException e) {
			System.out.println("Error occurred while reading object from DataInput");
		} 
		finally {
			try {
				if (inputStream != null) {
					inputStream.close();
				}
			} catch (IOException ex) {
				System.out.println("Input stream close exception occured\n");
			}
		}
	}

	
	
	/*********** 	Add feature to co-occurrences vector	 ***********/

	

	public void addFeature(String word, String dep_label, int total_count) {

		Map<String, Integer> depLabelMap = CooccurMap.get(word);

		/* If dependancy label map already exists for 'word', update mapping to dep_label */

		if (depLabelMap != null) 
		{
			if (depLabelMap.containsKey(dep_label)) 				// If dependancy label already exists for 'word' --> update count
			{				
				int currCount = depLabelMap.get(dep_label);
				depLabelMap.put(dep_label, currCount + total_count);
			}
			else {
				depLabelMap.put(dep_label, total_count);			// If dependancy label doesn't exist for 'word' --> create entry
			}
		}

		/* If null, then no mapping exists for this key --> create map for 'word' as key, and store new entry of dependancy label with total count */

		else {
			depLabelMap = new HashMap<String, Integer>();
			depLabelMap.put(dep_label, total_count);
			CooccurMap.put(word, depLabelMap);
		}
	}
	
	
	
	/*********** 	Deep copy all features (mappings) from another co-occurrences vector into co-occurrences vector 	 ***********/

	
	
	public void copyFeatures(CooccurrencesVector otherVector) 
	{
		/*	Check lexemes correspondence before performing the copy operation	*/
		if (otherVector.getLexeme() != this.lexeme) 
		{
			System.out.println("copyFeatures: Don't perform features copy, as different lexemes co-occurrences vectors");
			return;
		}
		
		
		Map<String, Map<String, Integer>> otherMap = otherVector.getCooccurMap();			// Get other co-occurrences vector's map
		
		/*	Iterate through map to copy features from the other's map to our map	*/
		
		for (Map.Entry<String, Map<String, Integer>> feature : otherMap.entrySet()) 
		{
			String key = feature.getKey();							// The other word
			Map<String, Integer> value = feature.getValue();		// <dependancy label, total count>
			
			for (Map.Entry<String, Integer> valueEntry : value.entrySet()) 			// Iterate through all mappings for current 'word' key
			{
				this.addFeature(key, valueEntry.getKey(), valueEntry.getValue());
			}
		}
	}
	
	

	/*********** 	Getters	 ***********/


	public String getLexeme() { return lexeme; }
	public Map<String, Map<String, Integer>> getCooccurMap() { return CooccurMap; }


	/*********** 	To string	 ***********/

	
	@Override
	public String toString(){
		return "";
	}


}

