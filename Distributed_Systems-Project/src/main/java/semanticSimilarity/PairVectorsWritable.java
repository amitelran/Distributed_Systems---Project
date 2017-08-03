package semanticSimilarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PairVectorsWritable implements WritableComparable<PairVectorsWritable> {
	
	
	protected CooccurrencesVector vector1 = new CooccurrencesVector();
	protected CooccurrencesVector vector2 = new CooccurrencesVector();
	
	
	/*********** 	Default Constructor	 ***********/

	
	PairVectorsWritable(){}
	
	
	/*********** 	Constructor	 ***********/

	
	PairVectorsWritable(CooccurrencesVector vector1, CooccurrencesVector vector2) 
	{
		this.vector1 = vector1;
		this.vector2 = vector2;
	}
	

	/*********** 	Read & Write methods	 ***********/

	
	public void readFields(DataInput in) throws IOException 
	{
		vector1.readFields(in); 
		vector2.readFields(in);
	}

	
	public void write(DataOutput out) throws IOException 
	{
		vector1.write(out);
		vector2.write(out);
	}
	
	
	/*********** 	CompareTo	 ***********/
	
	
	public int compareTo(PairVectorsWritable other) 
	{
		int checkArg = this.vector1.compareTo(other.getVector1());
    	if (checkArg != 0) {
    		return checkArg;
    	}
		return this.vector2.compareTo(other.getVector2());
	}
	
	
	
	/*********** 	hashing method implementation (idea from Effective Java)	 ***********/
	
	// Using prime number 17 & 31, generating hash code for the Feature object
	
	
	@Override
	public int hashCode() {
		int result = 17;
		result = 31 * result + vector1.getLexeme().hashCode();
        result = 31 * result + vector2.getLexeme().hashCode();
        return result;
	}
	
	
	
	/*********** 	Getters	 ***********/

	
	public CooccurrencesVector getVector1() { return this.vector1; }
	public CooccurrencesVector getVector2() { return this.vector2; }
	
	public String getLexeme1() { return this.vector1.getLexeme(); }
	public String getLexeme2() { return this.vector2.getLexeme(); }
	
	
	
	/*********** 	Setters	 ***********/

	
	public void setVector1(CooccurrencesVector vector1) { this.vector1 = vector1; }
	public void setVector2(CooccurrencesVector vector2) { this.vector2 = vector2; }
	
	
	public void setPairVectorsWritable(CooccurrencesVector vector1, CooccurrencesVector vector2) 
	{
		this.vector1 = vector1;
		this.vector2 = vector2;
	}

}
