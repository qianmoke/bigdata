package com.yi.java.hadoop.word;
import java.io.IOException;
import java.util.Formatter;
import java.util.Random;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
public class MapReduceIntroConfig {
	protected static Logger logger = Logger.getLogger(MapReduceIntroConfig.class);
	protected static Path inputDirectory = new Path("file:///home/gurunath/sorting1");
	protected static Path outputDirectory = new Path("file:///home/gurunath/out250");
	protected static void exampleHouseKeeping(final JobConf conf, final Path inputDirectory, final Path outputDirectory)
			throws IOException {
		conf.set("mapred.job.tracker", "local");
		conf.set("fs.default.name", "file:///");
		conf.setInt("io.sort.mb", 1);
		generateSampleInputIf(conf, inputDirectory);
		if (!removeIf(conf, outputDirectory)) {
			logger.error("Unable to remove " + outputDirectory + "jobaborted");
			System.exit(1);
		}
	}
	protected static void generateRandomFiles(final FileSystem fs, final Path inputDirectory, final int fileCount, final int maxLines)
			throws IOException {
		final Random random = new Random();
		logger .info( "Generating 3 input files of random data," + "each record is a random number TAB the input file name");
		for (int file = 0; file < fileCount; file++) {
			final Path outputFile = new Path("file-");
			final String qualifiedOutputFile = outputFile.makeQualified(fs).toUri().toASCIIString();
			FSDataOutputStream out = null;
			try {
				out = fs.create(outputFile);
				final Formatter fmt = new Formatter(out);
				final int lineCount = (int) (Math.abs(random.nextFloat())* maxLines + 1);
				for (int line = 0; line < lineCount; line++) {
					fmt.format("%d\t%s%n", Math.abs(random.nextInt()),qualifiedOutputFile);
				}
				fmt.flush();
			} finally {
				out.close();
			}
		}
	}
	protected static void generateSampleInputIf(final JobConf conf, final Path inputDirectory) throws IOException {
		boolean inputDirectoryExists;
		final FileSystem fs = inputDirectory.getFileSystem(conf);
		if ((inputDirectoryExists = fs.exists(inputDirectory)) && !isEmptyDirectory(fs, inputDirectory)) {
			if (logger.isDebugEnabled()) {
				logger.debug("The inputDirectory " + inputDirectory + " exists and is either a" + " file or a non empty directory");
			}
			return;
		}
		if (!inputDirectoryExists) {
			if (!fs.mkdirs(inputDirectory)) {
				logger.error("Unable to make the inputDirectory " + inputDirectory.makeQualified(fs) + " aborting job");
				System.exit(1);
			}
		}
		final int fileCount = 3;
		final int maxLines = 100;
		generateRandomFiles(fs, inputDirectory, fileCount, maxLines);
	}
	public static Path getInputDirectory() {
		return inputDirectory;
	}
	public static Path getOutputDirectory() {
		return outputDirectory;
	}
	private static boolean isEmptyDirectory(final FileSystem fs, final Path inputDirectory) throws IOException {
		final FileStatus[] statai = fs.listStatus(inputDirectory);
		if ((statai == null) || (statai.length == 0)) {
			if (logger.isDebugEnabled()) {
				logger.debug(inputDirectory.makeQualified(fs).toUri() + " is empty or missing");
			}
			return true;
		}
		if (logger.isDebugEnabled()) {
			logger.debug(inputDirectory.makeQualified(fs).toUri() + " is not empty");
		}
		for (final FileStatus status : statai) {
			if (!status.isDir() && (status.getLen() != 0)) {
				if (logger.isDebugEnabled()) {
					logger.debug("A non empty file " + status.getPath().makeQualified(fs).toUri() + " was found");
					return false;
				}
			}
		}
		
		for (final FileStatus status : statai) {
			if (status.isDir() && isEmptyDirectory(fs, status.getPath())) {
				continue;
			}
			if (status.isDir()) {
				return false;
			}
		}
			return true;
	}
	protected static boolean removeIf(final JobConf conf, final Path outputDirectory) throws IOException {
		final FileSystem fs = outputDirectory.getFileSystem(conf);
		if (!fs.exists(outputDirectory)) {
			if (logger.isDebugEnabled()) {
				logger .debug("The output directory does not exist," + " no removal needed.");
			}
			return true;
		}
		final FileStatus status = fs.getFileStatus(outputDirectory);
		logger.info("The job output directory " + outputDirectory.makeQualified(fs) + " exists" + (status.isDir() ? " and is not a directory" : "") + " and will be removed");
		if (!fs.delete(outputDirectory, true)) {
			logger.error("Unable to delete the configured output directory " + outputDirectory);
			return false;
		}
		return true;
	}
	public static void setInputDirectory(final Path inputDirectory) {
		MapReduceIntroConfig.inputDirectory = inputDirectory;
	}
	public static void setOutputDirectory(final Path outputDirectory) {
		MapReduceIntroConfig.outputDirectory = outputDirectory;
	}
}