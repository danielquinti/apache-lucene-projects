package es.udc.fi.ri.mri_indexer.util;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

public class FieldTypeCreator {
	private IndexOptions options;
	private boolean tokenized;
	private boolean stored;
	private boolean tv;
	private boolean tvp;

	public FieldTypeCreator(IndexOptions options, boolean tokenized, boolean stored, boolean tv, boolean tvp) {
		this.options = options;
		this.tokenized = tokenized;
		this.stored = stored;
		this.tv = tv;
		this.tvp = tvp;
	}

	public FieldType create() {
		FieldType TYPE = new FieldType();
		TYPE.setIndexOptions(options);
		TYPE.setTokenized(tokenized);
		TYPE.setStored(stored);
		TYPE.setStoreTermVectors(tv);
		TYPE.setStoreTermVectorPositions(tvp);
		TYPE.freeze();
		return TYPE;
	}
}
