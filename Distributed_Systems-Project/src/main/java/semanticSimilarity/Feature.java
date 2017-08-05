package semanticSimilarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class Feature implements WritableComparable<Feature> {
	
	protected String feature;
	protected String lexeme;
	protected int feature_total_independent_count;
	protected int lexeme_total_independent_count;
		
	// Measures of association with context
	protected MeasuresWritable measures = new MeasuresWritable();
	
	
	
	/*********** 	Constructors	 ***********/
	
	
	Feature(){}
	
	
	Feature(String feature, String lexeme, int total_count_of_feature, int total_count_of_lexeme) 
	{
		this.feature = feature;
		this.lexeme = lexeme;
		this.feature_total_independent_count = total_count_of_feature;
		this.lexeme_total_independent_count = total_count_of_lexeme;
		measures.setLexeme(lexeme);
		measures.setFeature(feature);
	}
	
	
	Feature(Feature other) 
	{
		this.feature = other.getFeature();
		this.lexeme = other.getLexeme();
		this.feature_total_independent_count = other.getTotalIndependentCountOfFeature();
		this.lexeme_total_independent_count = other.getTotalIndependentCountOfLexeme();
		this.measures = new MeasuresWritable(other.getMeasures());
	}
	
	
	/*********** 	Write data to DataOutput	 ***********/


	public void write(DataOutput out) throws IOException 
	{
		out.writeUTF(feature.toString());
		out.writeUTF(lexeme);
		out.writeInt(feature_total_independent_count);
		out.writeInt(lexeme_total_independent_count);
		measures.write(out);
	}

	

	/*********** 	Read data from DataInput	 ***********/

	
	public void readFields(DataInput in) throws IOException 
	{
		feature = in.readUTF();
		lexeme = in.readUTF();
		feature_total_independent_count = in.readInt();
		lexeme_total_independent_count = in.readInt();
		measures = new MeasuresWritable();
		measures.readFields(in);
		
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
		String otherFeature = ((Feature) other).getFeature();
		//String otherLexeme = ((Feature) other).getLexeme();
		//return ((word.equals(otherWord)) && (dep_label.equals(otherDepLabel)));
		return (feature.equals(otherFeature));
	}
	
	
	
	/*********** 	hashing method implementation (idea from Effective Java)	 ***********/
	
	// Using prime number 17 & 31, generating hash code for the Feature object
	
	@Override
	public int hashCode() {
		int result = 17;
		result = 31 * result + lexeme.hashCode();
        result = 31 * result + feature.hashCode();
        return result;
	}
	
	/*
	public static int getHashCode(String word, String dep_label) {
		int result = 17;
        result = 31 * result + word.hashCode();
        result = 31 * result + dep_label.hashCode();
        return result;
	}
	*/
	
	
	/*********** 	Compute measures of association with context	 ***********/
	
	
	public void computeAllMeasures(int featureAndLexemeFrequency, long totalLexemesInCorpus, long totalFeaturesInCorpus) 
	{
		this.measures.setRawFrequency(featureAndLexemeFrequency);
		this.measures.setRelativeFrequency(featureAndLexemeFrequency / lexeme_total_independent_count);
		compute_PMI_and_Ttest(totalLexemesInCorpus, totalFeaturesInCorpus);
	}
	

	
	public void compute_PMI_and_Ttest(long totalLexemesInCorpus, long totalFeaturesInCorpus ) 
	{
		/* PMI */
		
		float probability_of_lexeme = (lexeme_total_independent_count / totalLexemesInCorpus);		// P(l) = count(L=l) / count(L)
		float probability_of_feature = (feature_total_independent_count / totalFeaturesInCorpus);	// P(f) = count(F=f) / count(F)
		float probablity_multiplication = probability_of_lexeme * probability_of_feature;			// P(l) * P(f)
		float joined_prob_lexeme_feature = (this.measures.getRawFrequency() / totalLexemesInCorpus);			// P(l,f) = count(F=f, L=l) / count(L)
		float div_joined_mult = (joined_prob_lexeme_feature / probablity_multiplication);			// P(l,f) / P(f)P(l)
		this.measures.setPMI(Math.log(div_joined_mult) / Math.log(2));								// log2[P(l,f) / P(f)P(l)]
		
		/* T-test */
		
		float numerator = joined_prob_lexeme_feature - probablity_multiplication;					// P(l,f) - P(l)P(f)
		double rooted_denominator = Math.sqrt(probablity_multiplication); 							// sqrt(P(l)P(f))
		this.measures.setTtest(numerator / rooted_denominator);										// [P(l,f) - P(l)P(f)] / [sqrt(P(l)P(f)]
	}

	

	
	/*********** 	Getters	 ***********/


	public String getFeature() { return this.feature; }
	public String getLexeme() { return this.lexeme; }
	public int getTotalIndependentCountOfFeature() { return this.feature_total_independent_count; }
	public int getTotalIndependentCountOfLexeme() { return this.lexeme_total_independent_count; }
	public MeasuresWritable getMeasures() { return this.measures; }
	
	
	/*********** 	Setters	 ***********/

	
	public void setFeatureTotalCount(int featureCount) { this.feature_total_independent_count = featureCount; }
	public void setLexemeTotalCount(int lexemeCount) { this.lexeme_total_independent_count = lexemeCount; }
	
	
	
	public void copyFeatureData(Feature other) 
	{
		this.feature = other.getFeature();
		this.lexeme = other.getLexeme();
		this.feature_total_independent_count = other.getTotalIndependentCountOfFeature();
		this.lexeme_total_independent_count = other.getTotalIndependentCountOfLexeme();
		this.measures.copyMeasuresWritable(other.getMeasures());
	}
	

	

	/*********** 	To String	 ***********/

	
	@Override
	public String toString(){
		return "Feature: " + feature.toString() + ", " + 
				"Lexeme: " + lexeme + ", " + 
				"Independent Count of feature: " + Integer.toString(feature_total_independent_count) + ", " + 
				"Independent Count of lexeme: " + Integer.toString(lexeme_total_independent_count) + "\n";
	}
	
	
	/*********** 	CompareTo	 ***********/
	

	public int compareTo(Feature other) {
		int checkArg = this.feature.compareTo(other.getFeature());
    	if (checkArg != 0) {
    		return checkArg;
    	}
		return (this.lexeme.compareTo(other.getLexeme()));
	}
	
	
}
