package semanticSimilarity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Mapper;

public class CooccurrencesVectorsHadoop {


	public static class CooccurrencesVectorMapper extends Mapper<String, SyntacticNgramLine, String, CooccurrencesVector> {


		// input: 		ngram head word, 	syntactic ngram line
		// output: 		lexeme, 	lexeme's co-occurrences vector
		
		public void map(String headWord, SyntacticNgramLine ngramLine, Context context) throws IOException,  InterruptedException {
			
			Map<String, CooccurrencesVector> CooccurVectorMap = new HashMap<String, CooccurrencesVector>();		// Local hash map for producing co-occurrences vectors
			SyntacticNgram[] ngrams = ngramLine.getNgrams();
			int total_count = ngramLine.getTotalCount();
			
			for (int i = 0; i < ngrams.length; i++) 
			{
				SyntacticNgram currNgram = ngrams[i];
				String currWord = currNgram.getWord();
				
				SyntacticNgram pointedFromNgram = ngrams[currNgram.getHeadIndex()];		// Get the 'head' word which points on current Ngram
				String pointedFromWord = pointedFromNgram.getWord(); 					// Get the head word from the index
				
				CooccurrencesVector CooccurVector = CooccurVectorMap.get(currWord);
				if (CooccurVector != null) {
					CooccurVector.addFeature(word, dep_label);
				}
				else {
					CooccurVector = new CooccurrencesVector(currWord);
					CooccurVectorMap.put(currWord, value)
				}
			}
			
			
		}
	}

}


