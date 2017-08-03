package semanticSimilarity;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;



public class PairCoordinateWritable implements WritableComparable<PairCoordinateWritable> {
	
	protected String lexeme1;
	protected String lexeme2;
	protected String feature_name;
	
	
	/*********** 	Default Constructor	 ***********/

	
	PairCoordinateWritable(){}
	
	
	/*********** 	Constructor	 ***********/

	
	PairCoordinateWritable(String lexeme1, String lexeme2, String feature_name) {
		this.lexeme1 = lexeme1;
		this.lexeme2 = lexeme2;
		this.feature_name = feature_name;
	}
	

	/*********** 	Read & Write methods	 ***********/

	
	public void readFields(DataInput in) throws IOException {
		lexeme1 = in.readUTF();
		lexeme2 = in.readUTF();
		feature_name = in.readUTF();
		
	}

	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(lexeme1);
		out.writeUTF(lexeme2);
		out.writeUTF(feature_name);
	}
	
	
	/*********** 	CompareTo	 ***********/
	
	// Check if ( lexeme1 = lexeme1')
	// ( lexeme1 = lexeme1' ) ?  -----> Check if ( lexeme2 = lexeme2')
	// ( lexeme1 = lexeme1' ) && ( lexeme2 = lexeme2' ) -----> Check if ( feature = feature' )
	
	
	public int compareTo(PairCoordinateWritable other) 
	{
		int checkArg = this.lexeme1.compareTo(other.getLexeme1());
    	if (checkArg != 0) {
    		return checkArg;
    	}
		checkArg = this.lexeme2.compareTo(other.getLexeme2());
    	if (checkArg != 0) {
    		return checkArg;
    	}
		return this.feature_name.compareTo(other.getFeatureName());
	}
	
	
	/*********** 	hashing method implementation (idea from Effective Java)	 ***********/
	
	// Using prime number 17 & 31, generating hash code for the Feature object
	
	
	@Override
	public int hashCode() {
		int result = 17;
        result = 31 * result + feature_name.hashCode();
        return result;
	}
	
	
	
	/*********** 	Getters	 ***********/


	public String getFeatureName() { return this.feature_name; }
	public String getLexeme1() { return this.lexeme1; }
	public String getLexeme2() { return this.lexeme2; }
	
	
	/*********** 	Setters	 ***********/

	
	public void setFeatureName(String newFeatureName) { this.feature_name = newFeatureName; }
	public void setLexeme1(String newLexeme) { this.lexeme1 = newLexeme; }
	public void setLexeme2(String newLexeme) { this.lexeme2 = newLexeme; }
	
	
	public void setPairCoordinateWritable(String newLexeme1, String newLexeme2, String newFeatureName) 
	{
		this.feature_name = newFeatureName;
		this.lexeme1 = newLexeme1;
		this.lexeme2 = newLexeme2;
	}


}
