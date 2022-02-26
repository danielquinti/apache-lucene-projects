package es.udc.fi.ri.mri_searcher;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.math3.stat.inference.TTest;
import org.apache.commons.math3.stat.inference.WilcoxonSignedRankTest;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import es.udc.fi.ri.mri_searcher.util.ReadFile;
import es.udc.fi.ri.mri_searcher.util.Tuple;

import org.apache.lucene.document.Document;

public class TrainingTestNPL {

	private static double launchQuery(QueryParser parser, IndexSearcher searcher, IndexReader reader, String queryStr,
			List<Integer> relevants, int n, String metrica) {
		Query query = null;
		try {
			query = parser.parse(queryStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		TopDocs topDocs = null;

		try {
			topDocs = searcher.search(query, n);
		} catch (IOException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		}
		/*
		 * System.out.println( "\n" + topDocs.totalHits + " results for query \"" +
		 * query.toString() + "\" \n\nShowing for the first " + n+
		 * " documents the doc id, score and the content of the Contents field");
		 */
		int relevantCount = 0;
		double sumP = 0.0;
		for (int i = 0; i < Math.min(n, topDocs.totalHits.value); i++) {
			try {
				Document doc = reader.document(topDocs.scoreDocs[i].doc);
				Boolean relevant = relevants.contains(Integer.parseInt(doc.get("DocIDNPL")));
				if (i < n) {
					if (relevant) {
						relevantCount++;
						sumP = sumP + (double) relevantCount / (i + 1);
					}
				}
				/*
				 * System.out.println( doc.get("DocIDNPL") + " -- score: " +
				 * topDocs.scoreDocs[i].score + "-- relevant: " + relevant);
				 */
			} catch (CorruptIndexException e) {
				System.out.println("Graceful message: exception " + e);
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("Graceful message: exception " + e);
				e.printStackTrace();
			}
		} /*
			 * System.out.println(); System.out.println("Relevant Docs:"); for (Integer doc
			 * : relevants) System.out.print(doc + " "); System.out.println();
			 * System.out.println();
			 */

		double result = 0;

		switch (metrica) {
		case "P":
			result = (double) relevantCount / n;
			// System.out.println("Query: P@" + n + "=" + result);
			return result;
		case "R":
			result = (double) relevantCount / relevants.size();
			// System.out.println("Query: Recall@" + n + "=" + result);
			return result;
		case "MAP":
			result = (double) sumP / relevants.size();
			// System.out.println("Query: AP@" + n + "=" + result);
			return result;
		default:
			return result;
		}

	}

	private static double[] prepareQuery(boolean smoothing, IndexSearcher searcher, Tuple tuple, int min, int max,
			List<String> queriesL, List<List<Integer>> relevantsL, IndexReader reader, int n, String metrica, Boolean test) {
		QueryParser parser = new QueryParser("Contents", new StandardAnalyzer());
		Double acc = (double) 0;
		double array[] = new double[max-min+1];
		// System.out.println(hyperparameter);
		if (smoothing) {
			searcher.setSimilarity(new LMDirichletSimilarity(tuple.getHyperparameter()));
		} else {
			searcher.setSimilarity(new LMJelinekMercerSimilarity(tuple.getHyperparameter()));
		}
		
		int queryCount = 0;
		for (int idx = min; idx <= max; idx++) {
			String queryStr = queriesL.get(idx);
			String oldQuery = "";
			String queryTerms[] = queryStr.split(" ");
			for (String term : queryTerms) {
				if (term.equalsIgnoreCase("NOT") || term.equalsIgnoreCase("AND") || term.equalsIgnoreCase("OR")) {
					oldQuery = oldQuery + "\\" + term + " ";
				} else {
					oldQuery = oldQuery + term + " ";
				}
			}

			List<Integer> queryRelevants = relevantsL.get(idx);
			
			double l = launchQuery(parser, searcher, reader, oldQuery, queryRelevants, n, metrica);
			
			if (l != 0) {
				queryCount++;
				acc += l;
			}			
			if (test)
				array[idx-min]=l;
		}
		if (queryCount > 0) {
			Double avg = acc / queryCount;
			// System.out.println("Query: "+metrica+"@" + n + "=" + avg);
			tuple.setMetrica(avg);
		}
		if (test) {
			return array;
		} else {
			return null;
		}
	}

	public static double[] testAndTrain(boolean smoothing, IndexSearcher searcher, int minTest,int maxTest, int minTraining, int maxTraining,
			List<String> queriesL, List<List<Integer>> relevantsL, IndexReader reader, int n, String metrica) {
		List<Tuple> hyperparameters= new ArrayList<Tuple>();
		if (smoothing){
			hyperparameters.add((new Tuple((float) 0, (double) 0)));
			hyperparameters.add((new Tuple((float) 500, (double) 0)));
			hyperparameters.add((new Tuple((float) 1000, (double) 0)));
			hyperparameters.add((new Tuple((float) 1500, (double) 0)));
			hyperparameters.add((new Tuple((float) 2000, (double) 0)));
			hyperparameters.add((new Tuple((float) 2500, (double) 0)));
			hyperparameters.add((new Tuple((float) 3000, (double) 0)));
			hyperparameters.add((new Tuple((float) 3500, (double) 0)));
			hyperparameters.add((new Tuple((float) 4000, (double) 0)));
			hyperparameters.add((new Tuple((float) 4500, (double) 0)));
			hyperparameters.add((new Tuple((float) 5000, (double) 0)));
		}
		else {
			hyperparameters.add((new Tuple((float) 0.001, (double) 0)));
			hyperparameters.add((new Tuple((float) 0.1, (double) 0)));
			hyperparameters.add((new Tuple((float) 0.2, (double) 0)));
			hyperparameters.add((new Tuple((float) 0.3, (double) 0)));
			hyperparameters.add((new Tuple((float) 0.4, (double) 0)));
			hyperparameters.add((new Tuple((float) 0.5, (double) 0)));
			hyperparameters.add((new Tuple((float) 0.6, (double) 0)));
			hyperparameters.add((new Tuple((float) 0.7, (double) 0)));
			hyperparameters.add((new Tuple((float) 0.8, (double) 0)));
			hyperparameters.add((new Tuple((float) 0.9, (double) 0)));
			hyperparameters.add((new Tuple((float) 1, (double) 0)));
		}
		for (Tuple tuple : hyperparameters) {
			prepareQuery(smoothing, searcher, tuple, minTraining, maxTraining, queriesL, relevantsL, reader, n,
					metrica, false);
		}
		
		Comparator<Tuple> compareByMetric = (Tuple o1, Tuple o2) -> o1.getMetrica().compareTo(o2.getMetrica());
		Collections.sort(hyperparameters, compareByMetric.reversed());
		System.out.println("Test query range:" + (minTraining+1) + "-" + (maxTraining+1) + "\n");
		for (Tuple tuple : hyperparameters) {
			System.out.println("Parameter" + tuple.getHyperparameter() + "||" + tuple.getMetrica() + "\n");
		}
		Tuple best = hyperparameters.get(0);
		double array[] = prepareQuery(smoothing, searcher, best, minTest, maxTest, queriesL, relevantsL, reader, n, metrica, true);
		System.out.println();
		System.out.println();
		System.out.println("Test query range:" + (minTest+1) + "-" + (maxTest+1));

		System.out.println("Parameter:" + best.getHyperparameter() + " || " + best.getMetrica() + "\n");
		return array;
	}

	public static void main(String[] args) throws IOException {
		String usage = "";
		String test = "";
		String docsPath = null;
		String indexPath = "index";
		Integer n = null;
		Integer smoothing = null;
		String metrica = null;
		String[] minmaxTraining = null;
		String[] minmaxTest = null;
		Integer minTraining = null;
		Integer maxTraining = null;
		Integer minTest = null;
		Integer maxTest = null;
		Double alfa = null;
		/** Parse the args vector */
		for (int i = 0; i < args.length; i++) {
			if ("-evaljm".equals(args[i])) {
				smoothing = 0;
				minmaxTraining = args[i + 1].split("-");
				minTraining = Integer.parseInt(minmaxTraining[0])-1;
				maxTraining = Integer.parseInt(minmaxTraining[1])-1;
				minmaxTest = args[i + 2].split("-");
				minTest = Integer.parseInt(minmaxTest[0])-1;
				maxTest = Integer.parseInt(minmaxTest[1])-1;
				i += 2;
			} else if ("-evaldir".equals(args[i])) {
				smoothing = 1;
				minmaxTraining = args[i + 1].split("-");
				minTraining = Integer.parseInt(minmaxTraining[0]) - 1;
				maxTraining = Integer.parseInt(minmaxTraining[1]) - 1;
				minmaxTest = args[i + 2].split("-");
				minTest = Integer.parseInt(minmaxTest[0]) - 1;
				maxTest = Integer.parseInt(minmaxTest[1]) - 1;
				i += 2;
			} else if ("-compare".equals(args[i])) {
				smoothing = 2;
				minmaxTraining = args[i + 1].split("-");
				minTraining = Integer.parseInt(minmaxTraining[0]) - 1;
				maxTraining = Integer.parseInt(minmaxTraining[1]) - 1;
				minmaxTest = args[i + 2].split("-");
				minTest = Integer.parseInt(minmaxTest[0]) - 1;
				maxTest = Integer.parseInt(minmaxTest[1]) - 1;
				i += 2;
			} else if ("-indexin".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} else if ("-cut".equals(args[i])) {
				n = Integer.parseInt(args[i + 1]);

				i++;
			} else if ("-metrica".equals(args[i])) {
				metrica = args[i + 1];
				i++;
			} else if ("-coll".equals(args[i])) {
				docsPath = args[i + 1];
				i++;
			}else if ("-test".equals(args[i])) {
				test = args[i + 1];
				i++;
				alfa = Double.parseDouble(args[i + 1]);
				i++;
			}
		}
		
		if (docsPath == null) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		}
		

		try (Directory dir = FSDirectory.open(Paths.get(indexPath)); IndexReader reader = DirectoryReader.open(dir);) {

			IndexSearcher searcher = new IndexSearcher(reader);

			final Path docDir = Paths.get(docsPath);

			List<String> queriesL = null;
			List<List<Integer>> relevantsL = null;
			// Qualities
			if (Files.isDirectory(docDir)) {
				Path path = docDir.resolve("query-text");
				Path path2 = docDir.resolve("rlv-ass");
				if (Files.isReadable(path) && Files.isReadable(path2)) {
					queriesL = ReadFile.readQuery(path);
					relevantsL = ReadFile.readRelevance(path2);
				} else {
					System.exit(1);
				}
			} else {
				System.exit(1);
			}
			if (smoothing==2) {
				
				System.out.println("DIR");
				double jmResult[] = testAndTrain(true, searcher, minTest, maxTest,minTraining, maxTraining, queriesL, relevantsL, reader, n,
						metrica);
				
				System.out.println("JM");
				double dirResult[] = testAndTrain(false, searcher, minTest, maxTest,minTraining, maxTraining, queriesL, relevantsL, reader, n,
						metrica);
				
				double p = 0;
				switch (test) {
				case "t":
					TTest ttest = new TTest();
					p = ttest.pairedTTest(jmResult, dirResult);
					System.out.println("TTest");
					System.out.println("p-level: " + p + " between JM and DIR.");
					System.out.println("Significant: " + (p <= alfa));
					break;
				case "wilcoxon":
					WilcoxonSignedRankTest wil = new WilcoxonSignedRankTest();
					if (jmResult.length<=20) {
						p = wil.wilcoxonSignedRankTest(jmResult, dirResult, true);
					} else {
						p = wil.wilcoxonSignedRankTest(jmResult, dirResult, false);
					}				
					System.out.println("WilcoxonSignedRankTest");
					System.out.println("p-level: " + p + " between JM and DIR.");
					System.out.println("Significant: " + (p <= alfa));
					break;
				}			
				
			}
			else {
				testAndTrain(smoothing>0, searcher, minTest, maxTest,minTraining, maxTraining, queriesL, relevantsL, reader, n,
						metrica);
			}
		} catch (CorruptIndexException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		} catch (IOException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		}
	}
}
