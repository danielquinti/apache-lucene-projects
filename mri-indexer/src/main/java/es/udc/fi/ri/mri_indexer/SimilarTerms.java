package es.udc.fi.ri.mri_indexer;

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
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import es.udc.fi.ri.mri_indexer.util.CosineSimilarity;
import es.udc.fi.ri.mri_indexer.util.SimTerm;

public class SimilarTerms {

	/**
	 * 
	 * With FieldInfos and MultiTerms classes we get access to the fields, terms and
	 * postings for an index reader without accessing the leaves.
	 * 
	 * 
	 * Accessing the atomic leaves as it is shown in previous examples is faster.
	 * 
	 * These APIs are experimental and might change in incompatible ways in the next
	 * release.
	 * 
	 * In fact these functionalities were implemented in the MultiFields class in
	 * Lucene 7.x
	 * 
	 * @throws IOException
	 */

	public static Set<Integer> docs = new HashSet<>();

	static RealVector toRealVector(Map<Integer, Integer> map) {
		RealVector vector = new ArrayRealVector(docs.size());
		int i = 0;
		for (Integer term : docs) {
			int value = map.containsKey(term) ? map.get(term) : 0;
			vector.setEntry(i++, value);
		}
		return (RealVector) vector.mapDivide(vector.getL1Norm());
	}

	public static Map<Integer, Integer> getFrequency(TermsEnum termsEnum, IndexReader indexReader, String fieldName,
			int rep, String termName, int max) throws IOException {

		String termString = termsEnum.term().utf8ToString();

		if (termString.equals(termName)) {
			return null;
		}
		return getFrequency(indexReader, fieldName, rep, termString, max);
	}

	public static Map<Integer, Integer> getFrequency(IndexReader indexReader, String fieldName, int rep,
			String termName, int max) throws IOException {

		Map<Integer, Integer> frequencies = new HashMap<>();

		PostingsEnum posting = MultiTerms.getTermPostingsEnum(indexReader, fieldName, new BytesRef(termName));

		if (posting != null) { // if the term does not appear in any document, the posting object may be
								// null
			int docid;
			// Each time you call posting.nextDoc(), it moves the cursor of the posting list
			// to the next position
			// and returns the docid of the current entry (document). Note that this is an
			// internal Lucene docid.
			// It returns PostingsEnum.NO_MORE_DOCS if you have reached the end of the
			// posting list.

			while ((docid = posting.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
				int freq = posting.freq(); // get the frequency of the term in the current document
				if (rep == 0) {
					if (freq > 0)
						freq = 1;
				} else {
					if (rep == 2) {
						int df = indexReader.docFreq(new Term(fieldName, termName));
						freq = (int) (freq * Math.log10(max / (double) df));
					}
				}
				frequencies.put(docid, freq);
			}
			return frequencies;
		}
		return null;
	}

	public static void main(final String[] args) throws IOException {

		String usage = "java SimilarTerms -index INDEX_LOCATION -field FIELD -term TERM_NAME [-rep {bin|tf|tfxidf}]\n\n";
		String indexFolder = null;
		String fieldName = null;
		String termName = null;
		int rep = 1;
		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
				case "-index":
					indexFolder = args[i + 1];
					break;
				case "-field":
					fieldName = args[i + 1];
					break;
				case "-term":
					termName = args[i + 1];
					break;
				case "-rep":
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
					break;
			}
			i++;
		}

		if (indexFolder == null || termName == null || fieldName == null || rep == -1) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		}

		try (Directory dir = FSDirectory.open(Paths.get(indexFolder));
				DirectoryReader indexReader = DirectoryReader.open(dir);) {

			int max = indexReader.numDocs();
			docs = new HashSet<Integer>();
			for (int i = 0; i < max; i++) {
				docs.add(i);
			}

			System.out.println("Field = " + fieldName + ". Term = " + termName + ".");
			final Terms terms = MultiTerms.getTerms(indexReader, fieldName);
			Comparator<SimTerm> compareBySim = (SimTerm o1, SimTerm o2) -> ((Double) o1.similarity)
					.compareTo((Double) o2.similarity);
			List<SimTerm> termList = new ArrayList<SimTerm>();

			if (terms != null) {

				Map<Integer, Integer> f1 = getFrequency(indexReader, fieldName, rep, termName, max);
				RealVector v1 = toRealVector(f1);

				final TermsEnum termsEnum = terms.iterator();
				while (termsEnum.next() != null) {
					Map<Integer, Integer> f2 = getFrequency(termsEnum, indexReader, fieldName, rep, termName, max);
					if (f2 != null) {
						RealVector v2 = toRealVector(f2);
						double sim = new CosineSimilarity(v1, v2).calculate();

						if (!Double.isNaN(sim)) {
							String termStr = new String(termsEnum.term().utf8ToString());
							termList.add(new SimTerm(termStr, sim));
							Collections.sort(termList, compareBySim.reversed());
							if (termList.size() == 11)
								termList.remove(10);
						}
					}
				}

			}
			System.out.println("Similar terms:");
			for (SimTerm term : termList) {
				System.out.println(term.name + " (" + String.format("%3f", term.similarity) + ")");
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