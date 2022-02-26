package es.udc.fi.ri.mri_searcher;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import es.udc.fi.ri.mri_searcher.util.FieldTypeCreator;
import es.udc.fi.ri.mri_searcher.util.IndexWriterCreator;

/**
 * Index all text files under a directory.
 * <p>
 * This is a command-line application demonstrating simple Lucene indexing. Run
 * it with no command-line arguments for usage information.
 */
public class IndexNPL {

	private IndexNPL() {
	}

	public static final FieldType TYPE_STORED = new FieldTypeCreator(
			IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, true, true, true, true).create();

	public static void main(String[] args) {
		String usage = "index -index PATH [-indexingmodel dir x|jm x|tfidf] -coll DOCSPATH [-openmode create|append|create_or_append]";
		String docsPath = null;
		String indexPath = "index";
		String openMode = "create";
		String indexingModel = "tfidf";
		Float x = null;
		/** Parse the args vector */
		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} else if ("-indexingmodel".equals(args[i])) {
				indexingModel = args[i + 1];
				i++;
				switch (indexingModel) {
				case "jm":
					x = Float.parseFloat(args[i + 1]);
					System.out.println("JMX");
					i++;
					break;
				case "dir":
					x = Float.parseFloat(args[i + 1]);
					System.out.println("DIRX");
					i++;
					break;
				}
			} else if ("-openmode".equals(args[i])) {
				openMode = args[i + 1];
				i++;
			} else if ("-coll".equals(args[i])) {
				docsPath = args[i + 1];
				i++;
			}
		}

		if (docsPath == null) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		}

		final Path docDir = Paths.get(docsPath);
		if (!Files.isReadable(docDir)) {
			System.out.println("Document directory '" + docDir.toAbsolutePath()
					+ "' does not exist or is not readable, please check the path");
			System.exit(1);
		}

		Date start = new Date();
		System.out.println("Indexing to directory '" + indexPath + "'...");
		try (Directory dir = FSDirectory.open(Paths.get(indexPath));
				IndexWriter writer = new IndexWriterCreator(openMode, dir, indexingModel, x).getIndexWriter();) {
			indexDocs(writer, docDir);
		} catch (IOException e) {
			System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
		}
		Date end = new Date();
		System.out.println(end.getTime() - start.getTime() + " total milliseconds");
	}

	static void indexDocs(final IndexWriter writer, Path path) throws IOException {
		if (Files.isDirectory(path)) {
			path = path.resolve("doc-text");
			if (Files.isReadable(path)) {
				try (InputStream stream = Files.newInputStream(path);
					BufferedReader reader = new BufferedReader(
					new InputStreamReader(stream, StandardCharsets.UTF_8));) {
						String line = "";
						while ((line = reader.readLine()) != null) {
							Document doc = new Document();
							String content = "";
							String docId = line;
							Field addressField = new StringField("DocIDNPL", docId, Field.Store.YES);
							doc.add(addressField);

							while ((line = reader.readLine()) != null) {
								if (line.equals("   /")) {
									Field contentField = new Field("Contents", content, TYPE_STORED);
									doc.add(contentField);
									if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
										/* New index, so we just add the document (no old document can be there): */
										System.out.println("adding " + docId);
										writer.addDocument(doc);
									} else {
										/*
										 * Existing index (an old copy of this document may have been indexed) so we use
										 * updateDocument instead to replace the old one matching the exact path, if
										 * present:
										 */
										System.out.println("updating " + docId);
										writer.updateDocument(new Term("DocIDNPL", docId), doc);
									}
									break;
								}
								content = content + line + " ";
							}
						}
					}
				}
			}
		}
	}
