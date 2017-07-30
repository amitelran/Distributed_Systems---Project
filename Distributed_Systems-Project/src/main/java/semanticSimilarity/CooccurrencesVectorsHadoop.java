package semanticSimilarity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CooccurrencesVectorsHadoop {


	
	/*********** 	Mapper class for creating co-occurrences vectors per Syntactic Ngram line	 ***********/

	
	public static class CooccurrencesVectorMapper extends Mapper<String, SyntacticNgramLine, String, CooccurrencesVector> {


		// input: 		ngram head word, 	syntactic ngram line
		// output: 		lexeme, 	lexeme's co-occurrences vector
		
		public void map(String headWord, SyntacticNgramLine ngramLine, Context context) throws IOException,  InterruptedException 
		{
			
			Map<String, CooccurrencesVector> CooccurVectorMap = new HashMap<String, CooccurrencesVector>();		// Local hash map for producing co-occurrences vectors
			SyntacticNgram[] ngrams = ngramLine.getNgrams();			// Extract syntactic ngrams array for multiple uses
			int total_count = ngramLine.getTotalCount();				// Extract total count integer for multiple uses
			
			/*	Co-occurrences vectors creation/update for each of the syntactic ngrams in "ngrmams" */
			
			for (int i = 0; i < ngrams.length; i++) 
			{
				SyntacticNgram currNgram = ngrams[i];
				String currWord = currNgram.getWord();
				String dependancyLabel = currNgram.getDependancyLabel();
				
				SyntacticNgram headIndexNgram = ngrams[currNgram.getHeadIndex()];		// Get the 'head' word which points on current Ngram
				String headIndexWord = headIndexNgram.getWord(); 						// Get the head word from the index
				
				CooccurrencesVector CooccurVector = CooccurVectorMap.get(headIndexWord);
				
				// If co-occurrences vector of the head-index already exists --> update with feature
				if (CooccurVector != null) {
					CooccurVector.addFeature(currWord, dependancyLabel, total_count);
				}
				
				// Co-occurrences vector of the head-index doesn't exist --> create it, and update with feature
				else {
					CooccurVector = new CooccurrencesVector(headIndexWord);
					CooccurVector.addFeature(currWord, dependancyLabel, total_count);
					CooccurVectorMap.put(headIndexWord, CooccurVector);
				}
			}
			
			
			/*	Write co-occurrences vectors to context	*/
			
			for (Map.Entry<String, CooccurrencesVector> vectorEntry : CooccurVectorMap.entrySet()) {
				context.write(vectorEntry.getKey(), vectorEntry.getValue());
			}
		}
	}
	
	
	
	/*********** 	Reducer class for unifying co-occurrences vectors of Syntactic Ngram lines	 ***********/

	
	public static class CooccurrencesVectorReducer extends Reducer<String, CooccurrencesVector, String, CooccurrencesVector> {
		
		long numOfValues = 0;
		
		// input: 		lexeme, 	list of Co-occurrences vectors
		// output: 		lexeme, 	unified co-occurrences vector
		
		public void reduce(String lexeme, Iterable<CooccurrencesVector> vectors, Context context) throws IOException, InterruptedException 
		{
			CooccurrencesVector sumVector = new CooccurrencesVector(lexeme);
			
			/*	Iterate over all co-occurrences vectors and unify them to a single co-occurrences vector	*/
			
			for (Iterator<CooccurrencesVector> i = vectors.iterator(); i.hasNext();)
			{
				CooccurrencesVector vector = i.next();
				numOfValues++;
				sumVector.copyFeatures(vector);
			}
			
			context.write(lexeme, sumVector);
		}
	}

}


