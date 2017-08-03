package semanticSimilarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;



public class VectorsSimilaritiesWritable implements WritableComparable<VectorsSimilaritiesWritable> {

	protected String lexeme1;
	protected String lexeme2;
	
	/* 4 measures of association with context * 5 similarity equations = 20 combinations of vectors similarities */
	
	// Raw Frequency measure
	protected double rawFreq_ManhattanDis = 0;
	protected double rawFreq_EuclideanDis = 0;
	protected double rawFreq_CosineSim = 0;
	protected double rawFreq_JaccardSim = 0;
	protected double rawFreq_DiceSim = 0;

	// Relative Frequency measure
	protected double relativeFreq_ManhattanDis = 0;
	protected double relativeFreq_EuclideanDis = 0;
	protected double relativeFreq_CosineSim = 0;
	protected double relativeFreq_JaccardSim = 0;
	protected double relativeFreq_DiceSim = 0;

	// PMI measure
	protected double pmi_ManhattanDis = 0;
	protected double pmi_EuclideanDis = 0;
	protected double pmi_CosineSim = 0;
	protected double pmi_JaccardSim = 0;
	protected double pmi_DiceSim = 0;

	// T-test measure
	protected double tTest_ManhattanDis = 0;
	protected double tTest_EuclideanDis = 0;
	protected double tTest_CosineSim = 0;
	protected double tTest_JaccardSim = 0;
	protected double tTest_DiceSim = 0;

	
	
	/*********** 	Constructor 	 ***********/

	
	VectorsSimilaritiesWritable(){}
	
	
	
	/*********** 	Constructor given two co-occurrences vectors	 ***********/

	
	
	VectorsSimilaritiesWritable(String lexeme1, String lexeme2)
	{
		this.lexeme1 = lexeme1;
		this.lexeme2 = lexeme2;		
	}
	




	/*********** 	Read fields	 ***********/

	
	public void readFields(DataInput in) throws IOException 
	{
		lexeme1 = in.readUTF();
		lexeme2 = in.readUTF();
		
		rawFreq_ManhattanDis = in.readDouble();
		rawFreq_EuclideanDis = in.readDouble();
		rawFreq_CosineSim = in.readDouble();
		rawFreq_JaccardSim = in.readDouble();
		rawFreq_DiceSim = in.readDouble();

		relativeFreq_ManhattanDis = in.readDouble();
		relativeFreq_EuclideanDis = in.readDouble();
		relativeFreq_CosineSim = in.readDouble();
		relativeFreq_JaccardSim = in.readDouble();
		relativeFreq_DiceSim = in.readDouble();

		pmi_ManhattanDis = in.readDouble();
		pmi_EuclideanDis = in.readDouble();
		pmi_CosineSim = in.readDouble();
		pmi_JaccardSim = in.readDouble();
		pmi_DiceSim = in.readDouble();

		tTest_ManhattanDis = in.readDouble();
		tTest_EuclideanDis = in.readDouble();
		tTest_CosineSim = in.readDouble();
		tTest_JaccardSim = in.readDouble();
		tTest_DiceSim = in.readDouble();
	}

	
	/*********** 	Write fields	 ***********/

	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(lexeme1);
		out.writeUTF(lexeme2);
		
		out.writeDouble(rawFreq_ManhattanDis);
		out.writeDouble(rawFreq_EuclideanDis);
		out.writeDouble(rawFreq_CosineSim);
		out.writeDouble(rawFreq_JaccardSim);
		out.writeDouble(rawFreq_DiceSim);
		
		out.writeDouble(relativeFreq_ManhattanDis);
		out.writeDouble(relativeFreq_EuclideanDis);
		out.writeDouble(relativeFreq_CosineSim);
		out.writeDouble(relativeFreq_JaccardSim);
		out.writeDouble(relativeFreq_DiceSim);
		
		out.writeDouble(pmi_ManhattanDis);
		out.writeDouble(pmi_EuclideanDis);
		out.writeDouble(pmi_CosineSim);
		out.writeDouble(pmi_JaccardSim);
		out.writeDouble(pmi_DiceSim);
		
		out.writeDouble(tTest_ManhattanDis);
		out.writeDouble(tTest_EuclideanDis);
		out.writeDouble(tTest_CosineSim);
		out.writeDouble(tTest_JaccardSim);
		out.writeDouble(tTest_DiceSim);
	}
	
	
	
	/*********** 	Getters	 ***********/


	public String getLexeme1() { return this.lexeme1; }
	public String getLexeme2() { return this.lexeme2; }
	
	// Raw Frequency measure
	public double getRawFrequency_ManhattanDis() { return this.rawFreq_ManhattanDis; }
	public double getRawFrequency_EuclideanDis() { return this.rawFreq_EuclideanDis; }
	public double getRawFrequency_CosineSim() { return this.rawFreq_CosineSim; }
	public double getRawFrequency_JaccardSim() { return this.rawFreq_JaccardSim; }
	public double getRawFrequency_DiceSim() { return this.rawFreq_DiceSim; }
	
	// Relative Frequency measure
	public double getRelativeFrequency_ManhattanDis() { return this.relativeFreq_ManhattanDis; }
	public double getRelativeFrequency_EuclideanDis() { return this.relativeFreq_EuclideanDis; }
	public double getRelativeFrequency_CosineSim() { return this.relativeFreq_CosineSim; }
	public double getRelativeFrequency_JaccardSim() { return this.relativeFreq_JaccardSim; }
	public double getRelativeFrequency_DiceSim() { return this.relativeFreq_DiceSim; }

	// PMI measure
	public double getPMI_ManhattanDis() { return this.pmi_ManhattanDis; }
	public double getPMI_EuclideanDis() { return this.pmi_EuclideanDis; }
	public double getPMI_CosineSim() { return this.pmi_CosineSim; }
	public double getPMI_JaccardSim() { return this.pmi_JaccardSim; }
	public double getPMI_DiceSim() { return this.pmi_DiceSim; }

	// T-test measure
	public double getTtest_ManhattanDis() { return this.tTest_ManhattanDis; }
	public double getTtest_EuclideanDis() { return this.tTest_EuclideanDis; }
	public double getTtest_CosineSim() { return this.tTest_CosineSim; }
	public double getTtest_JaccardSim() { return this.tTest_JaccardSim; }
	public double getTtest_DiceSim() { return this.tTest_DiceSim; }
	
	
	
	/*********** 	Setters	 ***********/
	
	
	public void setLexeme1(String lexeme1) { this.lexeme1 = lexeme1; }
	public void setLexeme2(String lexeme2) { this.lexeme2 = lexeme2; }
	
	// Raw Frequency measure
	public void setRawFrequency_ManhattanDis(double newValue) { this.rawFreq_ManhattanDis = newValue; }
	public void setRawFrequency_EuclideanDis(double newValue) { this.rawFreq_EuclideanDis = newValue; }
	public void setRawFrequency_CosineSim(double newValue) { this.rawFreq_CosineSim = newValue; }
	public void setRawFrequency_JaccardSim(double newValue) { this.rawFreq_JaccardSim = newValue; }
	public void setRawFrequency_DiceSim(double newValue) { this.rawFreq_DiceSim = newValue; }
	
	// Relative Frequency measure
	public void setRelativeFrequency_ManhattanDis(double newValue) { this.relativeFreq_ManhattanDis = newValue; }
	public void setRelativeFrequency_EuclideanDis(double newValue) { this.relativeFreq_EuclideanDis = newValue; }
	public void setRelativeFrequency_CosineSim(double newValue) { this.relativeFreq_CosineSim = newValue; }
	public void setRelativeFrequency_JaccardSim(double newValue) { this.relativeFreq_JaccardSim = newValue; }
	public void setRelativeFrequency_DiceSim(double newValue) { this.relativeFreq_DiceSim = newValue; }

	// PMI measure
	public void setPMI_ManhattanDis(double newValue) { this.pmi_ManhattanDis = newValue; }
	public void setPMI_EuclideanDis(double newValue) { this.pmi_EuclideanDis = newValue; }
	public void setPMI_CosineSim(double newValue) { this.pmi_CosineSim = newValue; }
	public void setPMI_JaccardSim(double newValue) { this.pmi_JaccardSim = newValue; }
	public void setPMI_DiceSim(double newValue) { this.pmi_DiceSim = newValue; }

	// T-test measure
	public void setTtest_ManhattanDis(double newValue) { this.tTest_ManhattanDis = newValue; }
	public void setTtest_EuclideanDis(double newValue) { this.tTest_EuclideanDis = newValue; }
	public void setTtest_CosineSim(double newValue) { this.tTest_CosineSim = newValue; }
	public void setTtest_JaccardSim(double newValue) { this.tTest_JaccardSim = newValue; }
	public void setTtest_DiceSim(double newValue) { this.tTest_DiceSim = newValue; }

	
	
	/*********** 	CompareTo	 ***********/

	
	public int compareTo(VectorsSimilaritiesWritable otherVecSimiliarities) {
		int checkArg = this.lexeme1.compareTo(otherVecSimiliarities.getLexeme1());
    	if (checkArg != 0) {
    		return checkArg;
    	}
		return (this.lexeme2.compareTo(otherVecSimiliarities.getLexeme2()));
	}
}
