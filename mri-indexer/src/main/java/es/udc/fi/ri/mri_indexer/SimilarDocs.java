package es.udc.fi.ri.mri_indexer;

//Adapted from http://stackoverflow.com/questions/1844194/

//get-cosine-similarity-between-two-documents-in-lucene

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import es.udc.fi.ri.mri_indexer.util.CosineSimilarity;

public class SimilarDocs {
	private static class SimDocument {
		public int docId;
		public double similarity;

		public SimDocument(int docId, double similarity) {
			this.docId = docId;
			this.similarity = similarity;
		}
	}

	private static Set<String> terms = new HashSet<>();

	static Map<String, Integer> getTermFrequencies(IndexReader reader, int docId, int rep, String fieldName,
			boolean useTerms, int max) throws IOException {
		Terms vector = reader.getTermVector(docId, fieldName);
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

		TermsEnum termsEnum = null;
		termsEnum = vector.iterator();
		Map<String, Integer> frequencies = new HashMap<>();
		BytesRef text = null;
		while ((text = termsEnum.next()) != null) {
			String term = text.utf8ToString();
			int freq = (int) termsEnum.totalTermFreq();
			if (rep == 0) {
				if (freq > 0)
					freq = 1;
			}
			if (rep == 2) {
				int df = reader.docFreq(new Term(fieldName, term));
				freq = (int) (freq * Math.log10(max / (double) df));
			}
			frequencies.put(term, freq);
			if (useTerms)
				terms.add(term);
		}
		return frequencies;
	}

	static RealVector toRealVector(Map<String, Integer> map) {
		RealVector vector = new ArrayRealVector(terms.size());
		int i = 0;
		for (String term : terms) {
			int value = map.containsKey(term) ? map.get(term) : 0;
			vector.setEntry(i++, value);
		}
		return (RealVector) vector.mapDivide(vector.getL1Norm());
	}

	public static void main(String[] args) throws IOException {

		String usage = "java SimilarDocs "
				+ " -index INDEX_PATH -docId int -field FIELD_NAME [-rep {bin|tf|tfxidf}]\n\n";
		String indexFolder = null;
		int docId = -1;
		String fieldName = null;
		int rep = 1;
		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexFolder = args[i + 1];
				i++;
			}
			if ("-docId".equals(args[i])) {
				docId = Integer.parseInt(args[i + 1]);
				i++;
			}
			if ("-field".equals(args[i])) {
				fieldName = args[i + 1];
				i++;
			}
			if ("-rep".equals(args[i])) {
				String repStr = args[i + 1];
				switch (repStr) {
					case "bin":
						rep = 0;
						break;
					case "tf":
						rep = 1;
						break;
					case "tfxidf":
						rep = 2;
						break;
					default:
						rep = -1;
				}
				i++;
			}
		}

		if (indexFolder == null || docId == -1 || fieldName == null || rep == -1) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		}

		try (Directory dir = FSDirectory.open(Paths.get(indexFolder));
				IndexReader indexReader = DirectoryReader.open(dir)) {

			int max = indexReader.numDocs();
			List<SimDocument> docList = new ArrayList<SimDocument>();
			Comparator<SimDocument> compareBySim = (SimDocument o1, SimDocument o2) -> ((Double) o1.similarity)
					.compareTo((Double) o2.similarity);

			System.out.println("Document:");
			System.out.println(docId + " " + indexReader.document(docId).get("path") + "\n");

			Map<String, Integer> f1 = getTermFrequencies(indexReader, docId, rep, fieldName, true, max);
			RealVector v1 = toRealVector(f1);
			for (int i = 0; i < indexReader.numDocs(); i++) {
				if (i != docId) {
					Map<String, Integer> f2 = getTermFrequencies(indexReader, i, rep, fieldName, false, max);
					RealVector v2 = toRealVector(f2);

					double sim = new CosineSimilarity(v1, v2).calculate();
					docList.add(new SimDocument(i, sim));
					Collections.sort(docList, compareBySim.reversed());
					if (docList.size() == 11)
						docList.remove(10);
				}
			}
			System.out.println("Similar documents:");
			for (SimDocument documento : docList) {
				System.out.println(documento.docId + " " + indexReader.document(documento.docId).get("path") + " ("
						+ String.format("%3f", documento.similarity) + ")");
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
