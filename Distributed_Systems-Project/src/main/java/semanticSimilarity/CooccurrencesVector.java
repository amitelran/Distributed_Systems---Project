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

import org.apache.hadoop.io.WritableComparable;


public class CooccurrencesVector implements WritableComparable<CooccurrencesVector> {

	
	protected String lexeme;
	protected Map<Feature, MeasuresWritable> featuresMap = new HashMap<Feature, MeasuresWritable>(); 	// Mapping of a hash code as the 'key', and Features as the 'value

	// Lexeme's vector norm according to each measure of association with context
	protected double normRawFrequency = 0;
	protected double normRelativeFrequency = 0;
	protected double normPMI = 0;
	protected double normTtest = 0;

	
	
	/*********** 	Constructors	 ***********/


	public CooccurrencesVector(){}


	public CooccurrencesVector(String lexeme) { this.lexeme = lexeme; }
	
	
	public CooccurrencesVector(CooccurrencesVector otherVector)
	{
		this.lexeme = otherVector.getLexeme();
		this.normRawFrequency = otherVector.getNormRawFrequency();
		this.normRelativeFrequency = otherVector.getNormRelativeFrequency();
		this.normPMI = otherVector.getNormPMI();
		this.normTtest = otherVector.getNormTtest();
		this.copyFeatures(otherVector);
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
		lexeme = in.readUTF();
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


	
	public void addFeature(Feature inputFeature) 
	{
		Feature feature = new Feature(inputFeature);
		MeasuresWritable featureMeasures = new MeasuresWritable(inputFeature.getMeasures());
		this.featuresMap.put(feature, featureMeasures);
	}
	 
	
	
	/*********** 	Deep copy other vector to this vector 	 ***********/

	

	public void copyVector(CooccurrencesVector otherVector) 
	{
		this.resetVector();
		this.lexeme = otherVector.getLexeme();
		this.normRawFrequency = otherVector.getNormRawFrequency();
		this.normRelativeFrequency = otherVector.getNormRelativeFrequency();
		this.normPMI = otherVector.getNormPMI();
		this.normTtest = otherVector.getNormTtest();
		this.copyFeatures(otherVector);
		
	}

	
	/*********** 	Deep copy all features (mappings) from another co-occurrences vector into co-occurrences vector 	 ***********/

	

	public void copyFeatures(CooccurrencesVector otherVector) 
	{
		
		Map<Feature, MeasuresWritable> otherMap = otherVector.getFeaturesMap();			// Get other co-occurrences vector's map

		/*	Iterate through map to copy features from the other's map to our map	*/

		for (Map.Entry<Feature, MeasuresWritable> entry : otherMap.entrySet()) 
		{
			this.addFeature(entry.getKey());
		}
	}



	/*********** 	Getters	 ***********/


	public String getLexeme() { return this.lexeme; }
	public Map<Feature, MeasuresWritable> getFeaturesMap() { return this.featuresMap; }
	
	public double getNormRawFrequency() { return this.normRawFrequency; }
	public double getNormRelativeFrequency() { return this.normRelativeFrequency; }
	public double getNormPMI() { return this.normPMI; }
	public double getNormTtest() { return this.normTtest; }
	
	public MeasuresWritable getCoordinate(Feature feature) 
	{
		return this.featuresMap.get(feature);
	}
	
	
	
	/*********** 	Setters	 ***********/
	
	
	public void setLexeme(String newLexeme) { this.lexeme = newLexeme; }
	
	
	public void setLexemeNorms(float rawFreqNorm, float relFrequencyNorm, double pmiNorm, double TtestNorm) 
	{ 
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
	
	
	
	/*********** 	Compute co-occurrences vectors similarities 	 ***********/
	
	
	// numerator = "mone" of fraction
	// denominator = "mechane" of fraction
	
	
	public VectorsSimilaritiesWritable vectorsSim(CooccurrencesVector otherVector)
	{
		MeasuresWritable thisMeasures = new MeasuresWritable();
		MeasuresWritable otherMeasures = new MeasuresWritable();
		VectorsSimilaritiesWritable vectorsSimilarities = new VectorsSimilaritiesWritable(this.lexeme, otherVector.getLexeme());
		double thisRawFreq;
		double thisRelFreq;
		double thisPMI;
		double thisTtest;
		double otherRawFreq;
		double otherRelFreq;
		double otherPMI;
		double otherTtest;
		
		double rawFreq_ManhattanDis = 0;
		double rawFreq_EuclideanDis = 0;
		double rawFreq_CosineSim_numerator = 0;
		double rawFreq_JaccardSim_DiceSim_numerator = 0;
		double rawFreq_JaccardSim_denominator = 0;
		double rawFreq_DiceSim_denominator = 0;

		double relFreq_ManhattanDis = 0;
		double relFreq_EuclideanDis = 0;
		double relFreq_CosineSim_numerator = 0;
		double relFreq_JaccardSim_DiceSim_numerator = 0;
		double relFreq_JaccardSim_denominator = 0;
		double relFreq_DiceSim_denominator = 0;
		
		double pmi_ManhattanDis = 0;
		double pmi_EuclideanDis = 0;
		double pmi_CosineSim_numerator = 0;
		double pmi_JaccardSim_DiceSim_numerator = 0;
		double pmi_JaccardSim_denominator = 0;
		double pmi_DiceSim_denominator = 0;
		
		double tTest_ManhattanDis = 0;
		double tTest_EuclideanDis = 0;
		double tTest_CosineSim_numerator = 0;
		double tTest_JaccardSim_DiceSim_numerator = 0;
		double tTest_JaccardSim_denominator = 0;
		double tTest_DiceSim_denominator = 0;
		
		
		Map<Feature, MeasuresWritable> otherMap = otherVector.getFeaturesMap();			// Get other co-occurrences vector's map
		
		/*	Iterate through all features of this vector and compare with other vector's features	*/
		
		for (Map.Entry<Feature, MeasuresWritable> currEntry : featuresMap.entrySet()) 
		{
			thisMeasures = currEntry.getValue();
			
			thisRawFreq = thisMeasures.getRawFrequency();
			thisRelFreq = thisMeasures.getRelativeFrequency();
			thisPMI = thisMeasures.getPMI();
			thisTtest = thisMeasures.getTtest();
			
			otherMeasures = otherMap.get(currEntry.getKey());
			
			if (otherMeasures != null) 							// Feature exists in both maps!
			{								
				otherRawFreq = otherMeasures.getRawFrequency();
				otherRelFreq = otherMeasures.getRelativeFrequency();
				otherPMI = otherMeasures.getPMI();
				otherTtest = otherMeasures.getTtest();
				
				/* Raw Frequency similarities */

				rawFreq_ManhattanDis += Math.abs(thisRawFreq - otherRawFreq);						
				rawFreq_EuclideanDis += Math.pow((thisRawFreq - otherRawFreq), 2);					
				rawFreq_CosineSim_numerator += (thisRawFreq * otherRawFreq); 						
				rawFreq_JaccardSim_DiceSim_numerator += Math.min(thisRawFreq, otherRawFreq); 
				rawFreq_JaccardSim_denominator += Math.max(thisRawFreq, otherRawFreq); 		
				rawFreq_DiceSim_denominator += (thisRawFreq + otherRawFreq);				
			
				/* Relative Frequency similarities */
				
				relFreq_ManhattanDis += Math.abs(thisRelFreq - otherRelFreq);				
				relFreq_EuclideanDis += Math.pow((thisRelFreq - otherRelFreq), 2);			
				relFreq_CosineSim_numerator += (thisRelFreq * otherRelFreq); 				
				relFreq_JaccardSim_DiceSim_numerator += Math.min(thisRelFreq, otherRelFreq); 	
				relFreq_JaccardSim_denominator += Math.max(thisRelFreq, otherRelFreq); 		
				relFreq_DiceSim_denominator += (thisRelFreq + otherRelFreq);		
				
				/* PMI Frequency similarities */

				pmi_ManhattanDis += Math.abs(thisPMI - otherPMI);				
				pmi_EuclideanDis += Math.pow((thisPMI - otherPMI), 2);			
				pmi_CosineSim_numerator += (thisPMI * otherPMI); 				
				pmi_JaccardSim_DiceSim_numerator += Math.min(thisPMI, otherPMI); 	
				pmi_JaccardSim_denominator += Math.max(thisPMI, otherPMI); 		
				pmi_DiceSim_denominator += (thisPMI + otherPMI);
				
				/* T-test Frequency similarities */
				
				tTest_ManhattanDis += Math.abs(thisTtest - otherTtest);				
				tTest_EuclideanDis += Math.pow((thisTtest - otherTtest), 2);			
				tTest_CosineSim_numerator += (thisTtest * otherTtest); 				
				tTest_JaccardSim_DiceSim_numerator += Math.min(thisTtest, otherTtest); 	
				tTest_JaccardSim_denominator += Math.max(thisTtest, otherTtest); 		
				tTest_DiceSim_denominator += (thisTtest + otherTtest);
			}
			else 												// Feature exists only in 'this' vector
			{												
				/* Raw Frequency similarities */

				rawFreq_ManhattanDis += thisRawFreq;						
				rawFreq_EuclideanDis += Math.pow(thisRawFreq, 2);					
				rawFreq_JaccardSim_denominator += thisRawFreq; 		
				rawFreq_DiceSim_denominator += thisRawFreq;				
			
				/* Relative Frequency similarities */
				
				relFreq_ManhattanDis += thisRelFreq;				
				relFreq_EuclideanDis += Math.pow(thisRelFreq, 2);			
				relFreq_JaccardSim_denominator += thisRelFreq; 		
				relFreq_DiceSim_denominator += thisRelFreq;		
				
				/* PMI Frequency similarities */

				pmi_ManhattanDis += thisPMI;				
				pmi_EuclideanDis += Math.pow(thisPMI, 2);			
				pmi_JaccardSim_denominator += thisPMI; 		
				pmi_DiceSim_denominator += thisPMI;
				
				/* T-test Frequency similarities */
				
				tTest_ManhattanDis += thisTtest;				
				tTest_EuclideanDis += Math.pow(thisTtest, 2);			
				tTest_JaccardSim_denominator += thisTtest; 		
				tTest_DiceSim_denominator += thisTtest;
			}
		}
		
		
		/*	Iterate through all features of 'other' vector, and pay attention only to such that don't exist in 'this' vector	*/
		
		for (Map.Entry<Feature, MeasuresWritable> otherEntry : otherMap.entrySet()) 
		{
			otherMeasures = otherEntry.getValue();	
			
			otherRawFreq = otherMeasures.getRawFrequency();
			otherRelFreq = otherMeasures.getRelativeFrequency();
			otherPMI = otherMeasures.getPMI();
			otherTtest = otherMeasures.getTtest();
			
			thisMeasures = featuresMap.get(otherEntry.getValue());
			
			if (thisMeasures != null)					// Feature exists in both vectors -> already computed in previous loop, ignore
			{
				continue;
			}
			else 										// Feature exists only in 'other' vector
			{
				/* Raw Frequency similarities */

				rawFreq_ManhattanDis += otherRawFreq;						
				rawFreq_EuclideanDis += Math.pow(otherRawFreq, 2);					
				rawFreq_JaccardSim_denominator += otherRawFreq; 		
				rawFreq_DiceSim_denominator += otherRawFreq;				
			
				/* Relative Frequency similarities */
				
				relFreq_ManhattanDis += otherRelFreq;				
				relFreq_EuclideanDis += Math.pow(otherRelFreq, 2);			
				relFreq_JaccardSim_denominator += otherRelFreq; 		
				relFreq_DiceSim_denominator += otherRelFreq;		
				
				/* PMI Frequency similarities */

				pmi_ManhattanDis += otherPMI;				
				pmi_EuclideanDis += Math.pow(otherPMI, 2);			
				pmi_JaccardSim_denominator += otherPMI; 		
				pmi_DiceSim_denominator += otherPMI;
				
				/* T-test Frequency similarities */
				
				tTest_ManhattanDis += otherTtest;				
				tTest_EuclideanDis += Math.pow(otherTtest, 2);			
				tTest_JaccardSim_denominator += otherTtest; 		
				tTest_DiceSim_denominator += otherTtest;
			}
		}
		
		/* Raw Frequency similarities */

		vectorsSimilarities.setRawFrequency_ManhattanDis(rawFreq_ManhattanDis);
		vectorsSimilarities.setRawFrequency_EuclideanDis(Math.sqrt(rawFreq_EuclideanDis));
		vectorsSimilarities.setRawFrequency_CosineSim(rawFreq_CosineSim_numerator / (this.normRawFrequency * otherVector.getNormRawFrequency()));
		vectorsSimilarities.setRawFrequency_JaccardSim(rawFreq_JaccardSim_DiceSim_numerator / rawFreq_JaccardSim_denominator);
		vectorsSimilarities.setRawFrequency_DiceSim((2 * rawFreq_JaccardSim_DiceSim_numerator) / rawFreq_DiceSim_denominator);				
	
		/* Relative Frequency similarities */
		
		vectorsSimilarities.setRelativeFrequency_ManhattanDis(relFreq_ManhattanDis);
		vectorsSimilarities.setRelativeFrequency_EuclideanDis(Math.sqrt(relFreq_EuclideanDis));
		vectorsSimilarities.setRelativeFrequency_CosineSim(relFreq_CosineSim_numerator / (this.normRelativeFrequency * otherVector.getNormRelativeFrequency()));
		vectorsSimilarities.setRelativeFrequency_JaccardSim(relFreq_JaccardSim_DiceSim_numerator / relFreq_JaccardSim_denominator);
		vectorsSimilarities.setRelativeFrequency_DiceSim((2 * relFreq_JaccardSim_DiceSim_numerator) / relFreq_DiceSim_denominator);			
		
		/* PMI Frequency similarities */

		vectorsSimilarities.setPMI_ManhattanDis(pmi_ManhattanDis);
		vectorsSimilarities.setPMI_EuclideanDis(Math.sqrt(pmi_EuclideanDis));
		vectorsSimilarities.setPMI_CosineSim(pmi_CosineSim_numerator / (this.normPMI * otherVector.getNormPMI()));
		vectorsSimilarities.setPMI_JaccardSim(pmi_JaccardSim_DiceSim_numerator / pmi_JaccardSim_denominator);
		vectorsSimilarities.setPMI_DiceSim((2 * pmi_JaccardSim_DiceSim_numerator) / pmi_DiceSim_denominator);	
		
		/* T-test Frequency similarities */
		
		vectorsSimilarities.setTtest_ManhattanDis(tTest_ManhattanDis);
		vectorsSimilarities.setTtest_EuclideanDis(Math.sqrt(tTest_EuclideanDis));
		vectorsSimilarities.setTtest_CosineSim(tTest_CosineSim_numerator / (this.normTtest * otherVector.getNormTtest()));
		vectorsSimilarities.setTtest_JaccardSim(tTest_JaccardSim_DiceSim_numerator / tTest_JaccardSim_denominator);
		vectorsSimilarities.setTtest_DiceSim((2 * tTest_JaccardSim_DiceSim_numerator) / tTest_DiceSim_denominator);	
		
		return vectorsSimilarities;
	}

	
	
	
	/*********** 	Co-occurrences Vector reset	 ***********/


	public void resetVector() 
	{
		this.lexeme = null;
		this.normRawFrequency = 0;
		this.normRelativeFrequency = 0;
		this.normPMI = 0;
		this.normTtest = 0;
		this.featuresMap.clear();
	}
	

	
	/*********** 	To string	 ***********/


	@Override
	public String toString(){
		return "";
	}

	
	/*********** 	Compare To	 ***********/
	

	public int compareTo(CooccurrencesVector other) {
		return this.lexeme.compareTo(other.getLexeme());
	}


}

