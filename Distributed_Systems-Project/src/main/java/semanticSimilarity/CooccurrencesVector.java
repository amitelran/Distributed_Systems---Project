package semanticSimilarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.WritableComparable;


public class CooccurrencesVector implements WritableComparable<CooccurrencesVector> {

	
	protected String lexeme;
	protected Map<String, Feature> featuresMap; 	// Mapping of a hash code as the 'key', and Features as the 'value

	// Lexeme's vector norm according to each measure of association with context
	protected double normRawFrequency = 0;
	protected double normRelativeFrequency = 0;
	protected double normPMI = 0;
	protected double normTtest = 0;

	
	
	/*********** 	Constructors	 ***********/


	public CooccurrencesVector(){ this.featuresMap = new HashMap<String, Feature>(); }


	public CooccurrencesVector(String lexeme) { 
		this.lexeme = lexeme; 
		this.featuresMap = new HashMap<String, Feature>();
	}
	
	
	public CooccurrencesVector(CooccurrencesVector otherVector)
	{
		this.lexeme = otherVector.getLexeme();
		this.normRawFrequency = otherVector.getNormRawFrequency();
		this.normRelativeFrequency = otherVector.getNormRelativeFrequency();
		this.normPMI = otherVector.getNormPMI();
		this.normTtest = otherVector.getNormTtest();
		this.featuresMap = new HashMap<String, Feature>();
		this.copyFeatures(otherVector);
	}



	/*********** 	Write data to DataOutput	 ***********/


	public void write(DataOutput out) throws IOException 
	{
		out.writeUTF(lexeme);
		
		int mapSize = featuresMap.size();
		out.writeInt(mapSize);			// 'Push' size of map

		for (Map.Entry<String, Feature> entry : featuresMap.entrySet()) 
		{
			entry.getValue().write(out);
		}
		
		out.writeDouble(normRawFrequency);
		out.writeDouble(normRelativeFrequency); 
		out.writeDouble(normPMI);
		out.writeDouble(normTtest);
	}



	/*********** 	Read data from DataInput	 ***********/



	public void readFields(DataInput in) throws IOException 
	{
		lexeme = in.readUTF();
		int mapSize = in.readInt();				// Read 'pushed' size of map
		Map<String, Feature> readMap = new HashMap<String, Feature>();
		
		for (int i = 0; i < mapSize; i++)
		{
			Feature feature = new Feature();
			feature.readFields(in);
			readMap.put(feature.getFeature(), feature);
		}
		this.featuresMap = readMap;
		
		normRawFrequency = in.readDouble();
		normRelativeFrequency = in.readDouble();
		normPMI = in.readDouble();
		normTtest = in.readDouble();
	}



	/*********** 	Add feature to co-occurrences vector	 ***********/


	
	public void addFeature(Feature inputFeature) 
	{
		Feature feature = new Feature(inputFeature);
		this.featuresMap.put(feature.getFeature(), feature);
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
		
		Map<String, Feature> otherMap = otherVector.getFeaturesMap();			// Get other co-occurrences vector's map

		/*	Iterate through map to copy features from the other's map to our map	*/

		for (Map.Entry<String, Feature> entry : otherMap.entrySet()) 
		{
			this.addFeature(entry.getValue());
		}
	}



	/*********** 	Getters	 ***********/


	public String getLexeme() { return this.lexeme; }
	public Map<String, Feature> getFeaturesMap() { return this.featuresMap; }
	
	public double getNormRawFrequency() { return this.normRawFrequency; }
	public double getNormRelativeFrequency() { return this.normRelativeFrequency; }
	public double getNormPMI() { return this.normPMI; }
	public double getNormTtest() { return this.normTtest; }
	
	public Feature getCoordinate(Feature feature) { return this.featuresMap.get(feature.getFeature()); }
	
	
	
	/*********** 	Setters	 ***********/
	
	
	public void setLexeme(String newLexeme) { this.lexeme = newLexeme; }
	
	
	public void setLexemeNorms(double rawFreqNorm, double relFrequencyNorm, double pmiNorm, double TtestNorm) 
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
		double relativeMeasure = 0;
		double pmiMeasure = 0;
		double tTestMeasure = 0;
		
		double rawFrequencyNorm = 0;
		double relatvieFrequencyNorm = 0;
		double pmiNorm = 0;
		double tTestNorm = 0;
		
		for (Map.Entry<String, Feature> entry : featuresMap.entrySet()) 
		{
			MeasuresWritable measures = entry.getValue().getMeasures();
			
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
	
	
	public String vectorsSim(CooccurrencesVector otherVector, String similar)
	{
		MeasuresWritable thisMeasures = new MeasuresWritable();
		MeasuresWritable otherMeasures = new MeasuresWritable();
		
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
		
		double normRawFreqMult = (this.normRawFrequency * otherVector.getNormRawFrequency());
		double normRelFreqMult = (this.normRelativeFrequency * otherVector.getNormRelativeFrequency());
		double normPmiMult = (this.normPMI * otherVector.getNormPMI());
		double normTtestMult = (this.normTtest * otherVector.getNormTtest());

		
		Map<String, Feature> otherMap = otherVector.getFeaturesMap();			// Get other co-occurrences vector's map
		
		/*	Iterate through all features of this vector and compare with other vector's features	*/
		
		for (Map.Entry<String, Feature> thisEntry : featuresMap.entrySet()) 
		{
			thisMeasures = thisEntry.getValue().getMeasures();
			
			thisRawFreq = thisMeasures.getRawFrequency();
			thisRelFreq = thisMeasures.getRelativeFrequency();
			thisPMI = thisMeasures.getPMI();
			thisTtest = thisMeasures.getTtest();
			
			if (otherMap.containsKey(thisEntry.getKey())) 		// Feature exists in both maps!
			{
				otherMeasures = otherMap.get(thisEntry.getKey()).getMeasures();		// Get corresponding measures from other vector
								
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
		
		
		
		/*	Now, iterate through all features of 'other' vector, and pay attention only to such that don't exist in 'this' vector	*/
		
		for (Map.Entry<String, Feature> otherEntry : otherMap.entrySet()) 
		{
			otherMeasures = otherEntry.getValue().getMeasures();		// Get measures from the other vector feature
			
			otherRawFreq = otherMeasures.getRawFrequency();
			otherRelFreq = otherMeasures.getRelativeFrequency();
			otherPMI = otherMeasures.getPMI();
			otherTtest = otherMeasures.getTtest();
			
			if (this.featuresMap.containsKey(otherEntry.getKey()))		// Feature exists in both vectors -> already computed in previous loop, ignore
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
		
		String similarities ="";
		
		/* Raw Frequency similarities */
		
		similarities += rawFreq_ManhattanDis + "," + (Math.sqrt(rawFreq_EuclideanDis)) + "," +
						(rawFreq_CosineSim_numerator / normRawFreqMult) + "," + (rawFreq_JaccardSim_DiceSim_numerator / rawFreq_JaccardSim_denominator) + "," +
						((2 * rawFreq_JaccardSim_DiceSim_numerator) / rawFreq_DiceSim_denominator) + ",";
		
		/* Relative Frequency similarities */
		
		similarities += relFreq_ManhattanDis + "," + (Math.sqrt(relFreq_EuclideanDis)) + "," +
						(relFreq_CosineSim_numerator / normRelFreqMult) + "," + (relFreq_JaccardSim_DiceSim_numerator / relFreq_JaccardSim_denominator) + "," +
						((2 * relFreq_JaccardSim_DiceSim_numerator) / relFreq_DiceSim_denominator) + ",";
		
		/* PMI Frequency similarities */
		
		similarities += pmi_ManhattanDis + "," + (Math.sqrt(pmi_EuclideanDis)) + "," +
						(pmi_CosineSim_numerator / normPmiMult) + "," + (pmi_JaccardSim_DiceSim_numerator / pmi_JaccardSim_denominator) + "," +
						((2 * pmi_JaccardSim_DiceSim_numerator) / pmi_DiceSim_denominator) + ",";

		/* T-test Frequency similarities */
		
		similarities += tTest_ManhattanDis + "," + (Math.sqrt(tTest_EuclideanDis)) + "," +
						(tTest_CosineSim_numerator / normTtestMult) + "," + (tTest_JaccardSim_DiceSim_numerator / tTest_JaccardSim_denominator) + "," +
						((2 * tTest_JaccardSim_DiceSim_numerator) / tTest_DiceSim_denominator) + ",";
		
		similarities += similar;
		return similarities;
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
		String ret = "\nCo-occurrences Vector:\n";
		for (Map.Entry<String, Feature> entry : featuresMap.entrySet()) 
		{
			ret += "\t" + entry.getValue().toString();
		} 
		ret += "\traw frequency norm: " + normRawFrequency + ",";
		ret += "\trelative frequency norm: " + normRelativeFrequency + ",";
		ret += "\tpmi norm: " + normPMI + ",";
		ret += "\tt-test norm: " + normTtest;

		return ret;
	}

	
	/*********** 	Compare To	 ***********/
	

	public int compareTo(CooccurrencesVector other) {
		return this.lexeme.compareTo(other.getLexeme());
	}


}

