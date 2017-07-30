package semanticSimilarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Feature {
	
	protected String word;
	protected String dep_label;
	protected int total_count;
	
	// Measures of association with context
	protected int raw_frequency = -1;
	protected float relative_frequency = -1;
	protected float pmi = -1;						// Pointwise Mutual Information
	protected float t_test = -1;
	
	
	/*********** 	Constructors	 ***********/
	
	
	Feature(){}
	
	
	Feature(String word, String dep_label, int total_count) {
		this.word = word;
		this.dep_label = dep_label;
		this.total_count = total_count;
	}
	
	
	
	/*********** 	Write data to DataOutput	 ***********/


	public void write(DataOutput out) throws IOException 
	{
		out.writeUTF(word);
		out.writeUTF(dep_label);
		out.writeInt(total_count);
		out.writeInt(raw_frequency);
		out.writeFloat(relative_frequency);
		out.writeFloat(pmi);
		out.writeFloat(t_test);
	}

	

	/*********** 	Read data from DataInput	 ***********/

	
	public void readFields(DataInput in) throws IOException 
	{
		word = in.readUTF();
		dep_label = in.readUTF();
		total_count = in.readInt();
		raw_frequency = in.readInt();
		relative_frequency = in.readFloat();
		pmi = in.readFloat();
		t_test = in.readFloat();
	}

	
	
	/*********** 	equals method implementation	 ***********/

	
	@Override
	public boolean equals(Object other) 
	{
		if (other == this) {
			return true;
		}
		if (!(other instanceof Feature)) {
			return false;
		}
		String otherWord = ((Feature) other).getWord();
		String otherDepLabel = ((Feature) other).getDependancyLabel();
		return ((word.equals(otherWord)) && (dep_label.equals(otherDepLabel)));
	}
	
	
	
	/*********** 	hashing method implementation (idea from Effective Java)	 ***********/
	
	// Using prime number 17 & 31, generating hash code for the Feature object
	
	@Override
	public int hashCode() {
		int result = 17;
        result = 31 * result + word.hashCode();
        result = 31 * result + dep_label.hashCode();
        return result;
	}
	
	
	public static int getHashCode(String word, String dep_label) {
		int result = 17;
        result = 31 * result + word.hashCode();
        result = 31 * result + dep_label.hashCode();
        return result;
	}
	
	
	
	/*********** 	Compute measures of association with context	 ***********/
	
	
	public void computeRelativeFrequency(int lemmataCounts) {
		relative_frequency = (raw_frequency / lemmataCounts);
	}
	
	
	public void computePMI( ) {
		
	}


	public void computeTtest() {
		
	}
	

	
	/*********** 	Getters	 ***********/


	public String getWord() { return this.word; }
	public String getDependancyLabel() { return this.dep_label; }
	public int getTotalCount() { return this.total_count; }
	public int getRawFrequency() { return this.raw_frequency; }
	public float getRelativeFrequency() { return this.relative_frequency; }
	public float getPMI() { return this.pmi; }
	public float getTtest() { return this.t_test; }
	
	
	/*********** 	Setters	 ***********/

	
	public void setTotalCount(int newCount) { this.total_count = newCount; }

	

	/*********** 	To string	 ***********/

	
	@Override
	public String toString(){
		return "";
	}
}
