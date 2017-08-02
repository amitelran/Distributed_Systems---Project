package semanticSimilarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.WritableComparable;



public class VectorsSimilaritiesWritable implements WritableComparable<VectorsSimilaritiesWritable> {

	protected String lexeme1;
	protected String lexeme2;
	
	/* 4 measures of association with context * 5 similarity equations = 20 combinations of vectors similarities */
	
	// Raw Frequency measure
	protected double rawFreq_ManhattanSim = 0;
	protected double rawFreq_EuclideanSim = 0;
	protected double rawFreq_CosineSim = 0;
	protected double rawFreq_JaccardSim = 0;
	protected double rawFreq_DiceSim = 0;

	// Relative Frequency measure
	protected double relativeFreq_ManhattanSim = 0;
	protected double relativeFreq_EuclideanSim = 0;
	protected double relativeFreq_CosineSim = 0;
	protected double relativeFreq_JaccardSim = 0;
	protected double relativeFreq_DiceSim = 0;

	// PMI measure
	protected double pmi_ManhattanSim = 0;
	protected double pmi_EuclideanSim = 0;
	protected double pmi_CosineSim = 0;
	protected double pmi_JaccardSim = 0;
	protected double pmi_DiceSim = 0;

	// T-test measure
	protected double tTest_ManhattanSim = 0;
	protected double tTest_EuclideanSim = 0;
	protected double tTest_CosineSim = 0;
	protected double tTest_JaccardSim = 0;
	protected double tTest_DiceSim = 0;

	
	
	/*********** 	Constructor 	 ***********/

	
	VectorsSimilaritiesWritable(){}
	
	
	
	/*********** 	Constructor given two co-occurrences vectors	 ***********/

	
	
	VectorsSimilaritiesWritable(CooccurrencesVector vector1, CooccurrencesVector vector2)
	{
		this.lexeme1 = vector1.getLexeme().toString();
		this.lexeme2 = vector2.getLexeme().toString();
		
		computeSimilarities(vector1, vector2);
	}
	
	
	
	
	/*********** 	Compute similarities between two co-occurrences vectors	 ***********/

	
	
	private void computeSimilarities(CooccurrencesVector vector1, CooccurrencesVector vector2) 
	{
		for (Map.Entry<Feature, MeasuresWritable> entry : vector1.featuresMap.entrySet()) 
		{
			MeasuresWritable vector1Measures = entry.getValue();						
			
			//this.addFeature(measures);
		}
		
	}




	/*********** 	Read fields	 ***********/

	
	public void readFields(DataInput in) throws IOException 
	{
		lexeme1 = in.readUTF();
		lexeme2 = in.readUTF();
		
		rawFreq_ManhattanSim = in.readDouble();
		rawFreq_EuclideanSim = in.readDouble();
		rawFreq_CosineSim = in.readDouble();
		rawFreq_JaccardSim = in.readDouble();
		rawFreq_DiceSim = in.readDouble();

		relativeFreq_ManhattanSim = in.readDouble();
		relativeFreq_EuclideanSim = in.readDouble();
		relativeFreq_CosineSim = in.readDouble();
		relativeFreq_JaccardSim = in.readDouble();
		relativeFreq_DiceSim = in.readDouble();

		pmi_ManhattanSim = in.readDouble();
		pmi_EuclideanSim = in.readDouble();
		pmi_CosineSim = in.readDouble();
		pmi_JaccardSim = in.readDouble();
		pmi_DiceSim = in.readDouble();

		tTest_ManhattanSim = in.readDouble();
		tTest_EuclideanSim = in.readDouble();
		tTest_CosineSim = in.readDouble();
		tTest_JaccardSim = in.readDouble();
		tTest_DiceSim = in.readDouble();
	}

	
	/*********** 	Write fields	 ***********/

	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(lexeme1);
		out.writeUTF(lexeme2);
		
		out.writeDouble(rawFreq_ManhattanSim);
		out.writeDouble(rawFreq_EuclideanSim);
		out.writeDouble(rawFreq_CosineSim);
		out.writeDouble(rawFreq_JaccardSim);
		out.writeDouble(rawFreq_DiceSim);
		
		out.writeDouble(relativeFreq_ManhattanSim);
		out.writeDouble(relativeFreq_EuclideanSim);
		out.writeDouble(relativeFreq_CosineSim);
		out.writeDouble(relativeFreq_JaccardSim);
		out.writeDouble(relativeFreq_DiceSim);
		
		out.writeDouble(pmi_ManhattanSim);
		out.writeDouble(pmi_EuclideanSim);
		out.writeDouble(pmi_CosineSim);
		out.writeDouble(pmi_JaccardSim);
		out.writeDouble(pmi_DiceSim);
		
		out.writeDouble(tTest_ManhattanSim);
		out.writeDouble(tTest_EuclideanSim);
		out.writeDouble(tTest_CosineSim);
		out.writeDouble(tTest_JaccardSim);
		out.writeDouble(tTest_DiceSim);
	}

	
	
	/*********** 	CompareTo	 ***********/

	
	public int compareTo(VectorsSimilaritiesWritable otherVecSimiliarities) {
		// TODO Auto-generated method stub
		return 0;
	}

}
