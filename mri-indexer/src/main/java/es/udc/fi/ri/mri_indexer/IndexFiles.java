package es.udc.fi.ri.mri_indexer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import es.udc.fi.ri.mri_indexer.util.IndexWriterCreator;
import es.udc.fi.ri.mri_indexer.util.ConfigurationParametersManager;
import es.udc.fi.ri.mri_indexer.util.FieldTypeCreator;

public class IndexFiles {
	/* A class for the indexer threads */
	private static class DirWorkerThread implements Runnable {

		private final Path folder;
		IndexWriter writer;
		boolean partial;
		boolean onlyFiles;
		List<String> extensionsL;

		/*
		 * Each thread needs to have the parameters of the indexDocs() method as
		 * attributes
		 */
		private DirWorkerThread(final IndexWriter writer, final Path folder, boolean partial, boolean onlyFiles,
				List<String> extensionsL) {
			this.writer = writer;
			this.folder = folder;
			this.partial = partial;
			this.onlyFiles = onlyFiles;
			this.extensionsL = extensionsL;
		}

		@Override
		public void run() {
			try {
				indexDocs(writer, folder, onlyFiles, extensionsL);
				/*
				 * If the thread is not performing partial indexing, it shares its writer with
				 * the rest of the threads so it should not be closed.
				 */
				if (partial)
					writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public static final FieldType TYPE_STORED = new FieldTypeCreator(
			IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, true, true, true, true).create();

	static Directory[] openPartialDirectories() {
		/*
		 * Obtain the locations of the partial indexes directories from the
		 * configuration file, open them and store them in a list
		 */
		String[] partialsStr = ConfigurationParametersManager.getParameter("PARTIAL_INDEXES").split(" ");
		Directory[] partialIndexes = new Directory[partialsStr.length];
		int i = 0;
		for (String partialPath : partialsStr) {
			final Path indexDir = Paths.get(partialPath);
			if (!Files.isReadable(indexDir)) {
				System.out.println("Document directory '" + indexDir.toAbsolutePath()
						+ "' does not exist or is not readable, please check the path");
				System.exit(1);
			}
			try {
				partialIndexes[i] = FSDirectory.open(Paths.get(partialPath));
				i++;
			} catch (IOException e) {
				System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
			}
		}
		if (partialsStr.length != i) {
			System.out.println("Docs and partialIndexes mismatch");
			System.exit(1);
		}
		return partialIndexes;

	}

	static ArrayList<Path> getDocumentPaths() {
		/*
		 * Get the location of the folders to index from the configuration file and
		 * store their paths in a list
		 */
		String[] docs = ConfigurationParametersManager.getParameter("DOCS").split(" ");
		ArrayList<Path> docDirs = new ArrayList<Path>();
		for (String docsPath : docs) {
			final Path docDir = Paths.get(docsPath);
			if (!Files.isReadable(docDir)) {
				System.out.println("Document directory '" + docDir.toAbsolutePath()
						+ "' does not exist or is not readable, please check the path");
				System.exit(1);
			}
			docDirs.add(docDir);
		}
		return docDirs;
	}

	static void closePartialDirectories(Directory[] partialIndexes) throws IOException {
		for (Directory dir : partialIndexes) {
			dir.close();
		}
	}

	static List<String> getExtensionsList() {
		/*
		 * Get the valid file extensions from the configuration file and store them in a
		 * list
		 */
		String[] extensions = ConfigurationParametersManager.getParameter("ONLY_FILES").split(" ");
		return Arrays.asList(extensions);
	}

	/* Index all text files under a directory. */
	public static void main(String[] args) {
		/* Default configuration */
		String indexPath = "index";
		String openMode = "create";
		Boolean partial = false;
		Boolean onlyFiles = false;
		int numCores = Runtime.getRuntime().availableProcessors();
		/* Parse the args vector */
		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} else if ("-numThreads".equals(args[i])) {
				numCores = Integer.parseInt(args[i + 1]);
				i++;
			} else if ("-openMode".equals(args[i])) {
				openMode = args[i + 1];
				i++;
			} else if ("-partialIndexes".equals(args[i])) {
				partial = true;
			} else if ("-onlyFiles".equals(args[i])) {
				onlyFiles = true;
			}
		}

		Date start = new Date();

		ArrayList<Path> docDirs = getDocumentPaths();

		List<String> extensionsL = null;
		if (onlyFiles) {
			extensionsL = getExtensionsList();
			;
		}

		Directory[] partialIndexes = null;
		if (partial) {
			partialIndexes = openPartialDirectories();
		}

		System.out.println("Indexing to directory '" + indexPath + "'...");

		/*
		 * The try with resources grants automatic closing for the directory of the
		 * index and its writer
		 */
		String generalWriterOpenMode = partial ? "create" : openMode;
		try (Directory dir = FSDirectory.open(Paths.get(indexPath));
				IndexWriter writer = new IndexWriterCreator(generalWriterOpenMode, dir).getIndexWriter()) {

			final ExecutorService executor = Executors.newFixedThreadPool(numCores);

			int i = 0;
			for (Path docDir : docDirs) {
				Runnable worker;
				if (partial) {
					/*
					 * A thread is set to perform indexDocs on a single folder and store its results
					 * in the folder partialIndexes[i]
					 */

					worker = new DirWorkerThread(new IndexWriterCreator(openMode, partialIndexes[i]).getIndexWriter(),
							docDir, partial, onlyFiles, extensionsL);
					i++;
				} else {
					/*
					 * A thread is set to perform indexDocs on a single folder and store its results
					 * in the folder docDir
					 */
					worker = new DirWorkerThread(writer, docDir, partial, onlyFiles, extensionsL);
				}
				/*
				 * Send the thread to the ThreadPool. It will be processed eventually.
				 */
				executor.execute(worker);
			}
			/*
			 * Close the ThreadPool; no more jobs will be accepted, but all the previously
			 * submitted jobs will be processed.
			 */
			executor.shutdown();
			/* Wait up to 1 hour to finish all the previously submitted jobs */
			try {
				executor.awaitTermination(1, TimeUnit.HOURS);
			} catch (final InterruptedException e) {
				e.printStackTrace();
				System.exit(-2);
			}

			System.out.println("Finished all threads");
			// NOTE: if you want to maximize search performance,
			// you can optionally call forceMerge here. This can be
			// a terribly costly operation, so generally it's only
			// worth it when your index is relatively static (ie
			// you're done adding documents to it):
			//
			// writer.forceMerge(1);

			if (partial) {
				writer.addIndexes(partialIndexes);
				closePartialDirectories(partialIndexes);
			}
		} catch (IOException e) {
			System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
		}

		Date end = new Date();
		System.out.println(end.getTime() - start.getTime() + " total milliseconds");
	}

	static void indexDocs(final IndexWriter writer, Path path, boolean onlyFiles, List<String> extensionsL)
			throws IOException {
		if (Files.isDirectory(path)) {
			/* Traverse the file tree and in each node execute visitFile */
			Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					if (onlyFiles) {
						String fileName = file.toString();
						int dotP = fileName.lastIndexOf(".");
						if (dotP != -1) {
							/*
							 * If the extension does not match any of the elements in the list, the file is
							 * ignored
							 */
							if (!extensionsL.contains(fileName.substring(dotP))) {
								return FileVisitResult.CONTINUE;
							}
						}
					}
					try {
						indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
					} catch (IOException ignore) {
						// don't index files that can't be read.
					}
					return FileVisitResult.CONTINUE;
				}
			});

		} else {
			/* -onlyFiles does not apply for a single file. */
			indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
		}
	}

	/* Indexes a single document */
	static void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
		try (InputStream stream = Files.newInputStream(file)) {
			// make a new, empty document
			Document doc = new Document();

			// Add the path of the file as a field named "path". Use a
			// field that is indexed (i.e. searchable), but don't tokenize
			// the field into separate words and don't index term frequency
			// or positional information:
			Field pathField = new StringField("path", file.toString(), Field.Store.YES);
			doc.add(pathField);

			// Add the last modified date of the file a field named "modified".
			// Use a LongPoint that is indexed (i.e. efficiently filterable with
			// PointRangeQuery). This indexes to milli-second resolution.
			doc.add(new LongPoint("modified", lastModified));
			doc.add(new StoredField("modified", lastModified));
			// Add the contents of the file to a field named "contents". Specify a Reader,
			// so that the text of the file is tokenized and indexed, but not stored.
			// Note that FileReader expects the file to be in UTF-8 encoding.
			// If that's not the case searching for special characters will fail.
			BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));

			Field addressField = new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES);
			doc.add(addressField);

			Field threadField = new StringField("thread", Thread.currentThread().getName(), Field.Store.YES);
			doc.add(threadField);

			doc.add(new DoublePoint("sizeKb", (double) stream.available() / 1000));
			doc.add(new StoredField("sizeKb", (double) stream.available() / 1000));

			List<String> lines = new LinkedList<String>();
			StringBuffer top5 = new StringBuffer();
			StringBuffer bot5 = new StringBuffer();
			StringBuffer content = new StringBuffer();

			String line = "";
			int i = 0;
			while ((line = reader.readLine()) != null) {
				/* The first five lines go to top5 */
				if (i < 5) {
					top5.append(line).append('\n');
					i++;
				}
				/* Every line goes to content */
				content.append(line).append('\n');
				/* The last five lines are kept */
				if (lines.add(line) && lines.size() > 5)
					lines.remove(0);
			}

			for (String botline : lines) {
				bot5.append(botline).append('\n');
			}

			Field topField = new Field("top5Lines", top5.toString(), TYPE_STORED);
			doc.add(topField);
			Field botField = new Field("bottom5Lines", bot5.toString(), TYPE_STORED);
			doc.add(botField);
			Field contentField = new Field("contents", content.toString(), TYPE_STORED);
			doc.add(contentField);

			if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
				/* New index, so we just add the document (no old document can be there): */
				System.out.println("adding " + file);
				writer.addDocument(doc);
			} else {
				/*
				 * Existing index (an old copy of this document may have been indexed) so we use
				 * updateDocument instead to replace the old one matching the exact path, if
				 * present:
				 */
				System.out.println(writer.getConfig().getOpenMode().toString());
				System.out.println("updating " + file);
				writer.updateDocument(new Term("path", file.toString()), doc);
			}
		}
	}
}
