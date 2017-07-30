package semanticSimilarity;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CooccurrencesVectorsHadoop {


	/*******************************************************************************************************/
	/*********** 	Mapper class for creating co-occurrences vectors per Syntactic Ngram line	 ***********/


	public static class CooccurrencesVectorMapper extends Mapper<String, SyntacticNgramLine, String, CooccurrencesVector> {

		private Set<String> goldStandardWords = new HashSet<String>();
		Stemmer stemmer = new Stemmer();

		
		
		/*********** 	Read gold standard dataset words before starting the mapper	 ***********/

		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException 
		{
			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
				URI mappingFileUri = context.getCacheFiles()[0];

				if (mappingFileUri != null) {
					readGoldStandardFile("./word-relatedness");
				} 
				else {
					System.out.println(">>>>>> NO MAPPING FILE");
				}
			} 
			else {
				System.out.println(">>>>>> NO CACHE FILES AT ALL");
			}
		}
		
		
		/*********** 	Read gold standard dataset file	 ***********/

		
		private void readGoldStandardFile(String path) {
			try {
				BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
				String wordsPairsLine = null;
				while((wordsPairsLine = bufferedReader.readLine()) != null) 
				{
					String[] splitWords = wordsPairsLine.split("\\t");
					String word1 = Stemmer.stemWord(splitWords[0]);
					String word2 = Stemmer.stemWord(splitWords[1]);
					goldStandardWords.add(word1);
					goldStandardWords.add(word2);
				}
				bufferedReader.close();
			} 
			catch(IOException ex) {
				System.err.println("Exception while reading gold standard dataset file: " + ex.getMessage());
			}
		}
		
		
		
		/*******************************************	Mapper	 ***********************************************/
		
		
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
				headIndexWord = Stemmer.stemWord(headIndexWord);						// Stem head-index word

				// If the head-index word is not a part of the gold standard dataset --> no need to build co-occurrences vector
				if (!(goldStandardWords.contains(headIndexWord))) {
					continue;
				}
				
				currWord = Stemmer.stemWord(currWord);									// Stem current word

				CooccurrencesVector CooccurVector = CooccurVectorMap.get(headIndexWord);

				// If co-occurrences vector of the head-index already exists --> update with feature
				if (CooccurVector != null) {
					CooccurVector.addFeature(new Feature(currWord, dependancyLabel, total_count));
				}

				// Co-occurrences vector of the head-index doesn't exist --> create it, and update with feature
				else {
					CooccurVector = new CooccurrencesVector(headIndexWord);
					CooccurVector.addFeature(new Feature(currWord, dependancyLabel, total_count));
					CooccurVectorMap.put(headIndexWord, CooccurVector);
				}
			}


			/*	Write co-occurrences vectors to context	*/

			for (Map.Entry<String, CooccurrencesVector> vectorEntry : CooccurVectorMap.entrySet()) {
				context.write(vectorEntry.getKey(), vectorEntry.getValue());
			}
		}
	}


	/*******************************************************************************************************/
	/*********** 	Reducer class for unifying co-occurrences vectors of Syntactic Ngram lines	 ***********/


	public static class CooccurrencesVectorReducer extends Reducer<String, CooccurrencesVector, String, CooccurrencesVector> {

		long numOfValues = 0;

		// input: 		word, 	list of Co-occurrences vectors
		// output: 		word, 	unified co-occurrences vector

		public void reduce(String word, Iterable<CooccurrencesVector> vectors, Context context) throws IOException, InterruptedException 
		{
			CooccurrencesVector sumVector = new CooccurrencesVector(word);

			/*	Iterate over all co-occurrences vectors and unify them to a single co-occurrences vector	*/

			for (Iterator<CooccurrencesVector> i = vectors.iterator(); i.hasNext();)
			{
				CooccurrencesVector vector = i.next();
				numOfValues++;
				sumVector.copyFeatures(vector);
			}

			context.write(word, sumVector);
		}
	}

}


