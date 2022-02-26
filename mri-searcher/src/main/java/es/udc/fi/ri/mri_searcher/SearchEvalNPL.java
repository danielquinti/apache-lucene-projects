package es.udc.fi.ri.mri_searcher;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import es.udc.fi.ri.mri_searcher.util.ReadFile;

import org.apache.lucene.document.Document;

public class SearchEvalNPL {

	/**
	 * Project testlucene8_1_1 SimpleSearch class reads the index SimpleIndex
	 * created with the SimpleIndexing class, creates and Index Searcher and search
	 * for documents which contain the word "probability" in the field "Contents"
	 * using the StandardAnalyzer Also contains and example sorting the results by
	 * reverse document number (index order). Also contains an example of a boolean
	 * programmatic query
	 * 
	 */
	private static double launchQuery(QueryParser parser, IndexSearcher searcher, IndexReader reader,
			String queryStr, List<Integer> relevants, int n, int m, int metrica) {
		Query query = null;
		try {
			query = parser.parse(queryStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		TopDocs topDocs = null;

		try {
			topDocs = searcher.search(query, m);
		} catch (IOException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		}
		System.out.println(
				"\n" + topDocs.totalHits + " results for query \"" + query.toString() + "\" \n\nShowing for the first " + m
						+ " documents the doc id, score and the content of the Contents field");

		int relevantCount = 0;
		double sumP = 0.0;
		for (int i = 0; i < Math.min(m, topDocs.totalHits.value); i++) {
			try {
				Document doc = reader.document(topDocs.scoreDocs[i].doc);
				Boolean relevant = relevants.contains(Integer.parseInt(doc.get("DocIDNPL")));
				if (i < n && relevant) {
					relevantCount++;
					sumP = sumP + (double)relevantCount/(i+1);
				}
				String content = doc.get("Contents");
				System.out.println("DocIDNPL: "+doc.get("DocIDNPL") + " --Contents: " + content.substring(0, Math.min(30,content.length())) + " -- score: " + topDocs.scoreDocs[i].score + "-- relevant: " + relevant);
			} catch (CorruptIndexException e) {
				System.out.println("Graceful message: exception " + e);
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("Graceful message: exception " + e);
				e.printStackTrace();
			}
		}
		System.out.println();
		System.out.println("Relevant Docs:");
		for(Integer doc: relevants)
			System.out.print(doc + " ");
		System.out.println();System.out.println();
	
		double result = 0;

		switch (metrica) {
		case 1:
			result = (double) relevantCount / n;
			System.out.println("Query: P@" + n + "=" + result);
			return result;
		case 2:
			result = (double) relevantCount / relevants.size();
			System.out.println("Query: Recall@" + n + "=" + result);
			return result;
		case 3:
			result = (double) sumP / relevants.size();
			System.out.println("Query: AP@" + n + "=" + result);
			return result;
		default:
			return result;
		}
	}

	public static void main(String[] args) throws IOException {

		String docsPath = null;
		String indexPath = "index";
		Integer n = null;
		Integer m = null;
		String metrica = null;
		String queries = null;
		int min = 0;
		int max = 92;
		int metricaInt = 0;
		String indexingModel = null;
		Float x = null;
		/** Parse the args vector */
		for (int i = 0; i < args.length; i++) {
			if ("-search".equals(args[i])) {
				indexingModel = args[i + 1];
				i++;
				switch (indexingModel) {
				case "jm":
					x = Float.parseFloat(args[i + 1]);
					break;
				case "dir":
					x = Float.parseFloat(args[i + 1]);
					break;
				}
				i++;
			}
			if ("-indexin".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} else if ("-cut".equals(args[i])) {
				n = Integer.parseInt(args[i + 1]);
				i++;
			} else if ("-metrica".equals(args[i])) {
				metrica = args[i + 1];
				i++;
				switch (metrica) {
				case "P":
					metricaInt = 1;
					break;
				case "R":
					metricaInt = 2;
					break;
				case "MAP":
					metricaInt = 3;
					break;
				}
			} else if ("-top".equals(args[i])) {
				m = Integer.parseInt(args[i + 1]);
				i++;
			} else if ("-queries".equals(args[i])) {
				queries = args[i + 1];
				i++;
				if (!"all".equalsIgnoreCase(queries)) {
					String[] minmax = queries.split("-");
					if (minmax.length > 1) {
						min = Integer.parseInt(minmax[0])-1;
						max = Integer.parseInt(minmax[1])-1;
					} else {
						min = Integer.parseInt(minmax[0])-1;
						max = min;
					}

				}
			} else if ("-coll".equals(args[i])) {
				docsPath = args[i + 1];
				i++;
			}
		}
		
		if (docsPath == null) {
			// System.err.println("Usage: " + usage);
			System.exit(1);
		}
		// SimpleIndex is the folder where the index SimpleIndex is stored

		IndexReader reader = null;
		Directory dir = null;
		IndexSearcher searcher = null;
		QueryParser parser;

		try {
			dir = FSDirectory.open(Paths.get(indexPath));
			reader = DirectoryReader.open(dir);

		} catch (CorruptIndexException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		} catch (IOException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		}

		searcher = new IndexSearcher(reader);

		switch (indexingModel) {
		case ("jm"):
			searcher.setSimilarity(new LMJelinekMercerSimilarity(x));
			break;
		case ("dir"):
			searcher.setSimilarity(new LMDirichletSimilarity(x));
			break;
		case ("tfidf"):
			searcher.setSimilarity(new ClassicSimilarity());
			break;
		}

		parser = new QueryParser("Contents", new StandardAnalyzer());

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
		
		double result = 0;
		int queryCount = 0;
		for(int i = min;i<=max;i++) {
			String queryStr = queriesL.get(i);
			String oldQuery = "";
			String queryTerms[] = queryStr.split(" ");
			for (String term : queryTerms) {				
				if(term.equalsIgnoreCase("NOT") || term.equalsIgnoreCase("AND") || term.equalsIgnoreCase("OR")) {
					oldQuery = oldQuery + "\\" + term + " ";
				} else {
					oldQuery = oldQuery + term + " ";
				}	
			}
			System.out.println(oldQuery);
			
			List<Integer> queryRelevants = relevantsL.get(i);
			
			double met = launchQuery(parser, searcher, reader, oldQuery, queryRelevants, n, m, metricaInt);
			if (met != 0) {
				queryCount++;
				result += met;
			}
			System.out.println();
		}
		
		if (queryCount != 0) {
			switch (metricaInt) {
			case 1:
				System.out.println("Query: Mean P@" + n + "=" + result/queryCount);
				break;
			case 2:
				System.out.println("Query: Mean Recall@" + n + "=" + result/queryCount);
				break;
			case 3:
				System.out.println("Query: MAP@" + n + "=" + result/queryCount);
				break;
			}
		} else {
			System.out.println("Query: Significant queries not found.");
		}

		try {
			reader.close();
			dir.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
