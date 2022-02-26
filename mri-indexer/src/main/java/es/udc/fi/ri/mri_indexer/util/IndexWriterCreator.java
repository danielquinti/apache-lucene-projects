package es.udc.fi.ri.mri_indexer.util;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;

public final class IndexWriterCreator {
	private String openMode;
	private Directory dir;

	public IndexWriterCreator(String openMode, Directory dir) {
		this.openMode = openMode;
		this.dir = dir;
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
			break;
		case ("append"):
			iwc.setOpenMode(OpenMode.APPEND);
			break;
		case ("create_or_append"):
			iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			break;
		}
		// Optional: for better indexing performance, if you
		// are indexing many documents, increase the RAM
		// buffer. But if you do this, increase the max heap
		// size to the JVM (eg add -Xmx512m or -Xmx1g):clone
		//
		// iwc.setRAMBufferSizeMB(256.0);

		return new IndexWriter(dir, iwc);
	}
}
