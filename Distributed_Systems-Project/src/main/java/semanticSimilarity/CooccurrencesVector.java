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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class CooccurrencesVector implements WritableComparable<CooccurrencesVector> {

	protected Text lexeme;
	protected Map<Feature, MeasuresWritable> featuresMap = new HashMap<Feature, MeasuresWritable>(); 	// Mapping of a hash code as the 'key', and Features as the 'value

	// Lexeme's vector norm according to each measure of association with context
	protected double normRawFrequency = -1;
	protected double normRelativeFrequency = -1;
	protected double normPMI = -1;
	protected double normTtest = -1;

	
	
	/*********** 	Constructors	 ***********/


	public CooccurrencesVector(){}


	public CooccurrencesVector(Text lexeme) {
		this.lexeme = lexeme;
	}



	/*********** 	Write data to DataOutput	 ***********/


	public void write(DataOutput out) throws IOException 
	{
		out.writeUTF(lexeme.toString());

		// Convert hash map into bytes array in order to write to DataOutput

		ByteArrayOutputStream byteArrOutputStream = new ByteArrayOutputStream();
		ObjectOutput outputConvert = null;

		try {
			outputConvert = new ObjectOutputStream(byteArrOutputStream);   
			outputConvert.writeObject(featuresMap);
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
		
		out.writeDouble(normRawFrequency);
		out.writeDouble(normRelativeFrequency); 
		out.writeDouble(normPMI);
		out.writeDouble(normTtest);
	}



	/*********** 	Read data from DataInput	 ***********/



	@SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException 
	{
		lexeme.set(in.readUTF());
		int bytesArraySize = in.readInt();				// Read 'pushed' integer indicating size of bytes array

		byte[] bytesArray = new byte[bytesArraySize];
		in.readFully(bytesArray);

		/* Convert bytes array into hash map */

		ByteArrayInputStream bytesInputStream = new ByteArrayInputStream(bytesArray);
		ObjectInput inputStream = null;
		try {
			inputStream = new ObjectInputStream(bytesInputStream);
			featuresMap = (Map<Feature, MeasuresWritable>) inputStream.readObject(); 
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
		
		normRawFrequency = in.readDouble();
		normRelativeFrequency = in.readDouble();
		normPMI = in.readDouble();
		normTtest = in.readDouble();
	}



	/*********** 	Add feature to co-occurrences vector	 ***********/


	
	public void addFeature(Feature feature) 
	{
		MeasuresWritable featureMeasures = new MeasuresWritable(feature.getLexeme(), 
																feature.getFeature().toString(), 
																feature.getRawFrequency(), 
																feature.getRelativeFrequency(), 
																feature.getPMI(), 
																feature.getTtest());
		this.featuresMap.put(feature, featureMeasures);
	}
	 


	/*********** 	Deep copy all features (mappings) from another co-occurrences vector into co-occurrences vector 	 ***********/



	public void copyFeatures(CooccurrencesVector otherVector) 
	{
		/*	Check lexemes correspondence before performing the copy operation	*/
		if (!this.lexeme.equals(otherVector.getLexeme())) 
		{
			System.out.println("copyFeatures: Don't perform features copy, as different lexemes co-occurrences vectors");
			return;
		}

		Map<Feature, MeasuresWritable> otherMap = otherVector.getFeaturesMap();			// Get other co-occurrences vector's map

		/*	Iterate through map to copy features from the other's map to our map	*/

		for (Map.Entry<Feature, MeasuresWritable> entry : otherMap.entrySet()) 
		{
			MeasuresWritable measures = entry.getValue();						// <other word, dependancy label, total count>
			//this.addFeature(measures);
		}
	}



	/*********** 	Getters	 ***********/


	public Text getLexeme() { return this.lexeme; }
	public Map<Feature, MeasuresWritable> getFeaturesMap() { return this.featuresMap; }
	
	public MeasuresWritable getCoordinate(Feature feature) 
	{
		return this.featuresMap.get(feature);
	}
	
	
	public double getNormRawFrequency() { return this.normRawFrequency; }
	public double getNormRelativeFrequency() { return this.normRelativeFrequency; }
	public double getNormPMI() { return this.normPMI; }
	public double getNormTtest() { return this.normTtest; }
	
	
	/*********** 	Setters	 ***********/
	
	public void setLexeme(Text newLexeme) { this.lexeme = newLexeme; }
	
	public void setLexemeNorms(float rawFreqNorm, float relFrequencyNorm, double pmiNorm, double TtestNorm) { 
		this.normRawFrequency = rawFreqNorm; 
		this.normRelativeFrequency = relFrequencyNorm;
		this.normPMI = pmiNorm;
		this.normTtest = TtestNorm;
	}

	
	
	/*********** 	Compute co-occurrences vector norms 	 ***********/

	
	
	public void computeVectorNorms() 
	{
		int rawMeasure = 0;
		float relativeMeasure = 0;
		double pmiMeasure = 0;
		double tTestMeasure = 0;
		
		float rawFrequencyNorm = 0;
		float relatvieFrequencyNorm = 0;
		double pmiNorm = 0;
		double tTestNorm = 0;
		
		for (Map.Entry<Feature, MeasuresWritable> entry : featuresMap.entrySet()) 
		{
			MeasuresWritable measures = entry.getValue();
			rawMeasure = measures.getRawFrequency();
			relativeMeasure = measures.getRelativeFrequency();
			pmiMeasure = measures.getPMI();
			tTestMeasure = measures.getTtest();
			
			rawFrequencyNorm += (rawMeasure * rawMeasure);
			relatvieFrequencyNorm += (relativeMeasure * relativeMeasure);
			pmiNorm += (pmiMeasure * pmiMeasure);
			tTestNorm += (tTestMeasure * tTestMeasure);
		}
		
		this.normRawFrequency = Math.sqrt(rawFrequencyNorm);
		this.normRelativeFrequency = Math.sqrt(relatvieFrequencyNorm);
		this.normPMI = Math.sqrt(pmiNorm);
		this.normTtest = Math.sqrt(tTestNorm);
	}
	
	
	
	

	
	/*********** 	To string	 ***********/


	@Override
	public String toString(){
		return "";
	}


	public int compareTo(CooccurrencesVector o) {
		// TODO Auto-generated method stub
		return 0;
	}


}

