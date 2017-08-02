package semanticSimilarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;



public class MeasuresWritable implements WritableComparable<MeasuresWritable> {

	protected String lexeme;
	protected String feature;
	
	// Measures of association with context
	protected int raw_frequency = -1;				// Frequency of feature & lexeme appearances = count(F=f, L=l)
	protected float relative_frequency = -1;
	protected double pmi = -1;						// Pointwise Mutual Information
	protected double t_test = -1;
	
	
	MeasuresWritable(){}
	
	
	MeasuresWritable(String lexeme, String feature) 
	{
		this.lexeme = lexeme;
		this.feature = feature;
	}
	
	
	MeasuresWritable(String lexeme, String feature, int rawFreq, float relFreq, double pmi, double tTest)
	{
		this.lexeme = lexeme;
		this.feature = feature;
		this.raw_frequency = rawFreq;
		this.relative_frequency = relFreq;
		this.pmi = pmi;
		this.t_test = tTest;
	}
	
	
	public void readFields(DataInput in) throws IOException {
		lexeme = in.readUTF();
		feature = in.readUTF();
		raw_frequency = in.readInt();
		relative_frequency = in.readFloat();
		pmi = in.readDouble();
		t_test = in.readDouble();

	}

	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(lexeme);
		out.writeUTF(feature);
		out.writeInt(raw_frequency);
		out.writeFloat(relative_frequency);
		out.writeDouble(pmi);
		out.writeDouble(t_test);

	}

	
	/*********** 	Getters	 ***********/


	public String getFeature() { return this.feature; }
	public String getLexeme() { return this.lexeme; }
	public int getRawFrequency() { return this.raw_frequency; }
	public float getRelativeFrequency() { return this.relative_frequency; }
	public double getPMI() { return this.pmi; }
	public double getTtest() { return this.t_test; }
	
	
/*********** 	CompareTo	 ***********/
	

	public int compareTo(MeasuresWritable other) {
		int checkArg = this.feature.compareTo(other.getFeature());
    	if (checkArg != 0) {
    		return checkArg;
    	}
		return (this.lexeme.compareTo(other.getLexeme()));
	}

}
