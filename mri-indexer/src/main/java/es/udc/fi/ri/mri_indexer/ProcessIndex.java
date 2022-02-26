package es.udc.fi.ri.mri_indexer;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import es.udc.fi.ri.mri_indexer.util.FieldTypeCreator;
import es.udc.fi.ri.mri_indexer.util.IndexWriterCreator;
import es.udc.fi.ri.mri_indexer.util.ProcessTerm;

public class ProcessIndex {
	/*  */
	public static final FieldType TYPE_STORED = new FieldTypeCreator(
			IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, true, true, true, true).create();

	static void fillIndex(Document doc, Document newDoc, List<String> listBests, IndexWriter writer) {
		for (IndexableField field : doc.getFields()) {
			String fieldName = field.name();
			switch (fieldName) {
			case "path":
			case "thread":
			case "hostname":
				newDoc.add(new StringField(fieldName, field.stringValue(), Field.Store.YES));
				break;
			case "contents":
			case "bottom5Lines":
			case "top5Lines":
				newDoc.add(new Field(fieldName, field.stringValue(), TYPE_STORED));
				break;
			case "sizeKb":
				newDoc.add(new DoublePoint("sizeKb", field.numericValue().doubleValue()));
				newDoc.add(new StoredField("sizeKb", field.numericValue().doubleValue()));
				break;
			case "modified":
				long fieldNumVal = field.numericValue().longValue();
				newDoc.add(new LongPoint(fieldName, fieldNumVal));
				newDoc.add(new StoredField(fieldName, fieldNumVal));
				break;
			}

		}
		Field bestTermsContents = new StringField("bestTermsContents", listBests.get(0), Field.Store.YES);
		newDoc.add(bestTermsContents);

		Field bestTermsTop5Lines = new StringField("bestTermsTop5Lines", listBests.get(1), Field.Store.YES);
		newDoc.add(bestTermsTop5Lines);

		Field bestTermsBottom5Lines = new StringField("bestTermsBottom5Lines", listBests.get(2), Field.Store.YES);
		newDoc.add(bestTermsBottom5Lines);
	}

	public static void main(String[] args) throws IOException {
		String usage = "java ProcessIndex -input ORIGINAL_INDEX -output PROCESSED_INDEX";
		String indexFolder = null;
		String indexFolder2 = null;
		for (int i = 0; i < args.length; i++) {
			if ("-input".equals(args[i])) {
				indexFolder = args[i + 1];
				i++;
			}
			if ("-output".equals(args[i])) {
				indexFolder2 = args[i + 1];
				i++;
			}
		}
		if (indexFolder == null || indexFolder2 == null) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		}

		Document doc = null;
		String[] fieldNames = new String[] { "contents", "top5Lines", "bottom5Lines" };
		Comparator<ProcessTerm> compareByTfIdf = (ProcessTerm o1, ProcessTerm o2) -> (o1.tfidf.compareTo(o2.tfidf));

		try (Directory dir = FSDirectory.open(Paths.get(indexFolder));
				DirectoryReader indexReader = DirectoryReader.open(dir);
				Directory dir2 = FSDirectory.open(Paths.get(indexFolder2));

				IndexWriter writer = new IndexWriterCreator("create", dir2).getIndexWriter()) {
			int max = indexReader.numDocs();
			
			for (int i = 0; i < indexReader.numDocs(); i++) {
				List<String> listBests = new ArrayList<String>();
				/* indexReader.document(i) NOTE: only the content of a field is returned, if that field was stored 
				 * during indexing. Metadata like boost, omitNorm, IndexOptions, tokenized, etc., are not preserved. 
		         * For this reason, new fields and documents must be created to transfer this information into the 
		         * processed index. */
				doc = indexReader.document(i);
				for (String fieldName : fieldNames) {
					Terms terms = indexReader.getTermVector(i, fieldName);
					List<ProcessTerm> termList = new ArrayList<ProcessTerm>();
					if (terms != null) {
						final TermsEnum termsEnum = terms.iterator();

						while (termsEnum.next() != null) {
							String termString = termsEnum.term().utf8ToString();
							long tf = termsEnum.totalTermFreq();
							long df = indexReader.docFreq(new Term(fieldName, termString));
							double tfidf = tf * Math.log10(max/(double) df);
							termList.add(new ProcessTerm(termString, (int) tf, df, tfidf));
							Collections.sort(termList, compareByTfIdf.reversed());
							if (termList.size() == 6)
								termList.remove(5);
						}
					}
					String best = "";
					for (ProcessTerm term : termList) {
						best += term.name + " (" + term.tf + " " + term.df + " " +String.format("%.3f", term.tfidf) + ") ";
					}
					listBests.add(best);
				}
				Document newDoc = new Document();
				fillIndex(doc, newDoc, listBests, writer);
				writer.addDocument(newDoc);
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