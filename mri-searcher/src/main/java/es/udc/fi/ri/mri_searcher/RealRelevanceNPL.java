package es.udc.fi.ri.mri_searcher;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import es.udc.fi.ri.mri_searcher.util.DocumentRank;
import es.udc.fi.ri.mri_searcher.util.ReadFile;
import es.udc.fi.ri.mri_searcher.util.TermRank;

public class RealRelevanceNPL {

	private static Map<String, Double> terms = new HashMap<String, Double>();

	static void getTermScores(IndexReader reader, int docId, double rank, String fieldName, int max)
			throws IOException {
		Terms vector = reader.getTermVector(docId, fieldName);
		// lista de tipo doc rank
		// IndexReader.getTermVector(int docID, String field):
		// Retrieve term vector for this document and field, or null if term
		// vectors were not indexed.
		// The returned Fields instance acts like a single-document inverted
		// index (the docID will be 0).

		// Por esta razon al iterar sobre los terminos la totalTermFreq que es
		// la frecuencia
		// de un termino en la coleccion, en este caso es la frecuencia del
		// termino en docID,
		// es decir, el tf del termino en el documento docID
		Double score = null;
		TermsEnum termsEnum = null;
		termsEnum = vector.iterator();
		BytesRef text = null;
		while ((text = termsEnum.next()) != null) {
			String term = text.utf8ToString();
			int freq = (int) termsEnum.totalTermFreq();
			int df = reader.docFreq(new Term(fieldName, term));
			score = (freq * Math.log10(max / (double) df)) * rank;

			Double value = terms.putIfAbsent(term, score);
			if (value != null) {
				terms.replace(term, value + score);
			}
		}
	}

	private static List<DocumentRank> launchQuery(QueryParser parser, IndexSearcher searcher, IndexReader reader,
			String queryStr, List<Integer> relevants, int n, int m, int s, int metrica) {
		Query query = null;
		List<DocumentRank> rs = new ArrayList<DocumentRank>();
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
		
		int sCount = 0;
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
				if (sCount < s && relevant) {
					DocumentRank docR = new DocumentRank(Integer.parseInt(doc.get("DocIDNPL")),topDocs.scoreDocs[i].score);
					rs.add(docR);
					sCount++;
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
		
		if (!rs.isEmpty()) {
			System.out.println("RS Docs:");
			for(DocumentRank doc: rs)
				System.out.print(doc.doc + " ");
			System.out.println();System.out.println();
		}
		
		switch (metrica) {
		case 1:
			System.out.println("Query: P@" + n + "=" + (double) relevantCount/n);
			break;
		case 2:
			if (relevants.isEmpty()) {
				System.out.println("Query: Relevants not found.");
			} else {
				System.out.println("Query: Recall@" + n + "=" + (double) relevantCount/relevants.size());
			}
			break;
		case 3:
			if (relevants.isEmpty()) {
				System.out.println("Query: Relevants not found.");
			} else {
				System.out.println("Query: MAP@" + n + "=" + (double) sumP/relevants.size());
			}
			break;
		}
		return rs;
	}

	public static void main(String[] args) throws IOException {
		String docsPath = null;
		String indexPath = "index";
		Integer n = null;
		Integer m = null;
		Integer s = null;
		Integer t = null;
		Integer q = null;
		int metricaInt = 0;
		String metrica = null;
		String indexingModel = null;
		Float x = null;
		Float beta = null;
		boolean residual = false;
		/** Parse the args vector */
		for (int i = 0; i < args.length; i++) {
			if ("-retmodel".equals(args[i])) {
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
			} else if ("-rs".equals(args[i])) {
				s = Integer.parseInt(args[i + 1]);
				i++;
			} else if ("-exp".equals(args[i])) {
				t = Integer.parseInt(args[i + 1]);
				i++;
			} else if ("-query".equals(args[i])) {
				q = Integer.parseInt(args[i + 1])-1;
				i++;

			} else if ("-interp".equals(args[i])) {
				beta = Float.parseFloat(args[i + 1]);
				i++;
			} else if ("-coll".equals(args[i])) {
				docsPath = args[i + 1];
				i++;
			} else if ("-residual".equals(args[i])) {
				residual = "T".equals(args[i + 1]);
				i++;
			}
		}

		if (docsPath == null) {
			// System.err.println("Usage: " + usage);
			System.exit(1);
		}
		
		if (m<n) {
			System.err.println("Error: " + m + "<" + n);
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

		String queryStr = queriesL.get(q);
		
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
		List<Integer> queryRelevants = relevantsL.get(q);
		List<DocumentRank> rs = launchQuery(parser, searcher, reader, oldQuery, queryRelevants, n, m, s, metricaInt);
		
		int maxD = reader.numDocs();
		for (DocumentRank docR : rs) {
			getTermScores(reader, docR.doc-1, docR.rank, "Contents", maxD);
		}
		
		List<String> keySet = new ArrayList<String>(terms.keySet());
		List<Double> values = new ArrayList<Double>(terms.values());
		
		List<TermRank> oldTermRanks = new ArrayList<TermRank>();
		for(int i = 0; i<keySet.size(); i++) {
				oldTermRanks.add(new TermRank(keySet.get(i),values.get(i)));
		}
		
		Comparator<TermRank> compareByRank = (TermRank o1, TermRank o2) -> ((Double) o1.rank)
		          .compareTo((Double) o2.rank);
		Collections.sort(oldTermRanks, compareByRank.reversed());
	
		
		System.out.println("\nBest Terms: value(term) = ∑[doc in RS] tf(term, doc) x idf (term) x score (doc)");
		
		double sumRank = 0;
		for (int tCount = 0;tCount<Math.min(t,oldTermRanks.size());tCount++) {
			sumRank = sumRank + oldTermRanks.get(tCount).rank;
		}
		
		List<TermRank> termRanks = new ArrayList<TermRank>();
		for (int tCount = 0;tCount<Math.min(t,oldTermRanks.size());tCount++) {
			TermRank tRank = oldTermRanks.get(tCount);
			String key = tRank.term;
			Double value = tRank.rank;
			Double score = (1 - beta) * value / sumRank;
			termRanks.add(new TermRank(key, score));
			System.out.println("Term:" + key + "  Value:" + String.format("%3.2f", value));
		}

		String newQuery = "";
		int numTerms = queryTerms.length;
		for (String term : queryTerms) {
			if(term.equalsIgnoreCase("NOT") || term.equalsIgnoreCase("AND") || term.equalsIgnoreCase("OR")) {
				newQuery = newQuery + "\\" + term + "^" + String.format("%3.2f", beta / numTerms) + " ";
			} else {
				newQuery = newQuery + term + "^" + String.format("%3.2f", beta / numTerms) + " ";
			}		
		}

		for (TermRank termR : termRanks) {
			if(termR.term.equalsIgnoreCase("NOT") || termR.term.equalsIgnoreCase("AND") || termR.term.equalsIgnoreCase("OR")) {
				newQuery = newQuery + "\\" + termR.term + "^" + String.format("%3.2f", termR.rank) + " ";
			} else {
				newQuery = newQuery + termR.term + "^" + String.format("%3.2f", termR.rank) + " ";
			}	
		}
		
		newQuery = newQuery.replace(",", ".");
		System.out.println("\nExpanded Query:");
		System.out.println(newQuery);

		if (residual) {
			for(DocumentRank doc: rs) {				
				queryRelevants.remove(queryRelevants.indexOf(Integer.valueOf(doc.doc)));
			}
		}
		
		launchQuery(parser, searcher, reader, newQuery, queryRelevants, n, m, 0, metricaInt);
	}
}
