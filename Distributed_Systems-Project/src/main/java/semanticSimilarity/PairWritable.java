package semanticSimilarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class PairWritable implements WritableComparable<PairWritable> {
	
	protected String first;
	protected String second;
	
	
	/*********** 	Default Constructor	 ***********/

    
	public PairWritable(){}
    
	
	/*********** 	Constructor	by two Strings  ***********/

	
	public PairWritable(String word_i, String word_j) 
	{
		this.first = word_i;
		this.second = word_j;
		
		/*
		int compare = word_i.compareTo(word_j);
		
		// If: 	word_i = "*"	OR 		word_i > word_j (lexicographically)
		
		if (((word_i.equals("*")) || (compare > 0)))
		{
			this.first = word_i;
			this.second = word_j;
		}
		
		// If reached here: 	word_j = "*" 	OR 		word_i <= word_j
		else											
		{
			this.first = word_j;
			this.second = word_i;
		}*/
	}

	
	/*********** 	Read & Write methods	 ***********/


	
	public void readFields(DataInput in) throws IOException 
	{
		first = in.readUTF();
		second = in.readUTF();
	}
	
	
	
	public void write(DataOutput out) throws IOException 
	{
		out.writeUTF(first);
		out.writeUTF(second);
	}

	
	
	/*********** 	CompareTo	 ***********/
	
	// Give precedence to the "*" containing pair: 
	// When a "*" caracter is encountered on the right that particular object is pushed to the top.
	
	
    public int compareTo(PairWritable other) 
    {
    	int checkArg = this.first.compareTo(other.getFirst());
    	if (checkArg != 0) {
    		return checkArg;
    	}
    	
    	if (this.second.equals("*")) {					// If 'this' pair contains "*" --> Make 'this' pair arrive first at the reducer
    		return -1;
    	}
    	else if (other.getSecond().equals("*")) {		// If 'other' pair contains "*" --> Make 'other' pair arrive first at the reducer
    		return 1;
    	}
    	return (this.second.compareTo(other.getSecond()));		// Else, just compare usually
    	
    }
    
    
    
	/*********** 	hashCode	 ***********/

    
    
    @Override
    public int hashCode() {
    	int result = 17;
        result = 31 * result + first.hashCode();
        //result = 31 * result + second.hashCode();
        return result;
    }
    
    
    
	/*********** 	equals	 ***********/

    
    @Override
    public boolean equals(Object other) {
        if (this == other) {
        	return true;
        }
        
        if (other == null || getClass() != other.getClass()) {
        	return false;
        }

		PairWritable otherPair = (PairWritable) other;

        if (second != null ? !second.equals(otherPair.getSecond()) : otherPair.getSecond() != null) {
        	return false;
        }
        if (first != null ? !first.equals(otherPair.getFirst()) : otherPair.getFirst() != null) {
        	return false;
        }

        return true;
    }

    
    
	/*********** 	Order pair elements by lexicographical order	 ***********/

    // We will use it to ensure <i, j> is equal to <j, i>, by sorting --> <i, j>
    
    
    public void orderPairLexicograph() 
	{
    	if (first.equals("*") || second.equals("*") || second.equals("1")) 
    	{
    		return;
    	}
    	
		int compare = first.compareTo(second);
		if (compare > 0) 
		{
			String tmp = this.first;
			this.first = this.second;
			this.second = tmp;
		}
	}
    
    
    public void setFirst(String first) {
        this.first = first;
    }

    public void setSecond(String second) {
        this.second = second;
    }

    public String getFirst() {
        return first;
    }

    public String getSecond() {
        return second;
    }


}
