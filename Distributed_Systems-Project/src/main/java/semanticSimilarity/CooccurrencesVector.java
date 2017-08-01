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

	
	protected Map<Integer, Feature> coOccurMap = new HashMap<Integer, Feature>(); 	// Mapping of a hash code as the 'key', and Features as the 'value
	protected String word;


	/*********** 	Constructors	 ***********/


	public CooccurrencesVector(){}


	public CooccurrencesVector(String word) {
		this.word = word;
	}



	/*********** 	Write data to DataOutput	 ***********/


	public void write(DataOutput out) throws IOException 
	{
		out.writeUTF(word);

		// Convert hash map into bytes array in order to write to DataOutput

		ByteArrayOutputStream byteArrOutputStream = new ByteArrayOutputStream();
		ObjectOutput outputConvert = null;

		try {
			outputConvert = new ObjectOutputStream(byteArrOutputStream);   
			outputConvert.writeObject(coOccurMap);
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
		word = in.readUTF();
		int bytesArraySize = in.readInt();				// Read 'pushed' integer indicating size of bytes array
		
		byte[] bytesArray = new byte[bytesArraySize];
		in.readFully(bytesArray);

		/* Convert bytes array into hash map */

		ByteArrayInputStream bytesInputStream = new ByteArrayInputStream(bytesArray);
		ObjectInput inputStream = null;
		try {
			inputStream = new ObjectInputStream(bytesInputStream);
			coOccurMap = (Map<Integer, Feature>) inputStream.readObject(); 
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

	
	/*
	public void addFeature(Feature feature) {

		int hashKey = Feature.getHashCode(feature.getWord(), feature.getDependancyLabel());		// Get hash code value (which is the key to the entry in the hash map)
		Feature currFeature = coOccurMap.get(hashKey);											// Get the current feature in the hash map (if exists)
		
		/* Feature exists in hash map --> update total count of feature */
		/*
		if (currFeature != null) {
			int currCount = currFeature.getTotalCount();
			currFeature.setTotalCount(currCount + feature.getTotalCount());
			coOccurMap.put(hashKey, currFeature);
		}
		
		/* Feature doesn't exist in hash map --> Add feature to the hash map */
		/*
		else {
			coOccurMap.put(hashKey, feature);
		}
	}
	*/
	
	
	/*********** 	Deep copy all features (mappings) from another co-occurrences vector into co-occurrences vector 	 ***********/

	
	
	public void copyFeatures(CooccurrencesVector otherVector) 
	{
		/*	Check lexemes correspondence before performing the copy operation	*/
		if (!this.word.equals(otherVector.getWord())) 
		{
			System.out.println("copyFeatures: Don't perform features copy, as different lexemes co-occurrences vectors");
			return;
		}
		
		Map<Integer, Feature> otherMap = otherVector.getCoOccurMap();			// Get other co-occurrences vector's map
		
		/*	Iterate through map to copy features from the other's map to our map	*/
		
		for (Map.Entry<Integer, Feature> entry : otherMap.entrySet()) 
		{
			Feature feature = entry.getValue();						// <other word, dependancy label, total count>
			//this.addFeature(feature);
		}
	}
	
	

	/*********** 	Getters	 ***********/


	public String getWord() { return this.word; }
	public Map<Integer, Feature> getCoOccurMap() { return coOccurMap; }


	/*********** 	To string	 ***********/

	
	@Override
	public String toString(){
		return "";
	}


}

