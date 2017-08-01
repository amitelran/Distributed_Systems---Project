package semanticSimilarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class PairWritable extends Pair<Text, Text> implements WritableComparable<PairWritable>{
	
    
	public PairWritable(){}
    
	public PairWritable(Text first, Text second) {
		this.first = first;
		this.second = second;
	}


	public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
	}

	
	public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
	}

	
	
	
    public int compareTo(PairWritable other) {
    	int checkArg = this.first.toString().compareTo(other.getFirst().toString());
    	if (checkArg != 0) {
    		return checkArg;
    	}
    	if (this.second.toString().equals("*")) {
    		return -1;
    	}
    	else if (other.getSecond().toString().equals("*")) {
    		return 1;
    	}
    	return (this.second.toString().compareTo(other.getSecond().toString()));
    	
    }
    
    
    
    @Override
    public int hashCode() {
    	int result = 17;
        result = 31 * result + first.hashCode();
        result = 31 * result + second.hashCode();
        return result;
    }
    
    
    @Override
    public boolean equals(Object other) {
        if (this == other) {
        	return true;
        }
        
        if (other == null || getClass() != other.getClass()) {
        	return false;
        }

		@SuppressWarnings("unchecked")
		Pair<Text, Text> otherPair = (Pair<Text,Text>) other;

        if (second != null ? !second.equals(otherPair.getSecond()) : otherPair.getSecond() != null) {
        	return false;
        }
        if (first != null ? !first.equals(otherPair.getFirst()) : otherPair.getFirst() != null) {
        	return false;
        }

        return true;
    }



}
