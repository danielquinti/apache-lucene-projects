package es.udc.fi.ri.mri_searcher.util;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;

public final class IndexWriterCreator {
	private String openMode;
	private Directory dir;
	private String indexingModel;
	private Float x;

	public IndexWriterCreator(String openMode, Directory dir, String indexingModel, Float x) {
		this.openMode = openMode;
		this.dir = dir;
		this.indexingModel = indexingModel;
		this.x = x;
	}

	public IndexWriter getIndexWriter() throws IOException {
		/*
		 * Standard analyzer filters StandardTokenizer with StandardFilter,
		 * LowerCaseFilter and StopFilter, using a list of English stop words.
		 */
		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
		/*
		 * CREATE - creates a new index or overwrites an existing one. CREATE_OR_APPEND
		 * - creates a new index if one does not exist, otherwise it opens the index and
		 * documents will be appended. APPEND - opens an existing index.
		 */
		switch (openMode) {
		case ("create"):
			iwc.setOpenMode(OpenMode.CREATE);
			System.out.println("C");
			break;
		case ("append"):
			iwc.setOpenMode(OpenMode.APPEND);
			break;
		case ("create_or_append"):
			iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			break;
		}

		Similarity similarity = null;

		switch (indexingModel) {
		case ("jm"):
			similarity = new LMJelinekMercerSimilarity(x);
			break;
		case ("dir"):
			similarity = new LMDirichletSimilarity(x);
			break;
		case ("tfidf"):
			similarity = new ClassicSimilarity();
			break;
		default:
			similarity = new ClassicSimilarity();
			System.out.println("Indexing model unrecognized. DEFAULT: TFIDF");
		}
		iwc.setSimilarity(similarity);
		// Optional: for better indexing performance, if you
		// are indexing many documents, increase the RAM
		// buffer. But if you do this, increase the max heap
		// size to the JVM (eg add -Xmx512m or -Xmx1g):clone
		//
		// iwc.setRAMBufferSizeMB(256.0);

		return new IndexWriter(dir, iwc);
	}
}
