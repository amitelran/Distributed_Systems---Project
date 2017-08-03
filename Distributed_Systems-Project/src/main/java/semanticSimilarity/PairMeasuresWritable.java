package semanticSimilarity;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;



public class PairMeasuresWritable implements WritableComparable<PairMeasuresWritable> {
	
	protected MeasuresWritable measures1;
	protected MeasuresWritable measures2;
	
	
	/*********** 	Default Constructor	 ***********/

	
	PairMeasuresWritable(){}
	
	
	/*********** 	Constructor	 ***********/

	
	PairMeasuresWritable(MeasuresWritable measures1, MeasuresWritable measures2) {
		this.measures1 = measures1;
		this.measures2 = measures2;
	}
	

	/*********** 	Read & Write methods	 ***********/

	
	public void readFields(DataInput in) throws IOException {
		measures1.readFields(in);
		measures2.readFields(in);
		
	}

	
	public void write(DataOutput out) throws IOException {
		measures1.write(out);
		measures2.write(out);
	}
	
	
	/*********** 	CompareTo	 ***********/

	
	public int compareTo(PairMeasuresWritable other) 
	{
		int checkArg = this.measures1.compareTo(other.getMeasures1());
    	if (checkArg != 0) {
    		return checkArg;
    	}
		return this.measures2.compareTo(other.getMeasures2());
	}
	
	
	/*********** 	hashing method implementation (idea from Effective Java)	 ***********/
	
	// Using prime number 17 & 31, generating hash code for the Feature object
	
	
	@Override
	public int hashCode() {
		int result = 17;
        result = 31 * result + measures1.hashCode();
        result = 31 * result + measures2.hashCode();
        return result;
	}
	
	
	
	/*********** 	Getters	 ***********/


	public MeasuresWritable getMeasures1() { return this.measures1; }
	public MeasuresWritable getMeasures2() { return this.measures2; }
	
	
	/*********** 	Setters	 ***********/
	
	
	public void setPairMeasuresWritable(MeasuresWritable newMeasures1, MeasuresWritable newMeasures2) 
	{
		this.measures1.copyMeasuresWritable(newMeasures1);
		this.measures2.copyMeasuresWritable(newMeasures2);
	}

	
	

}
