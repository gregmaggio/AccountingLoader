/**
 * 
 */
package ca.datamagic.accounting.avro;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

/**
 * @author gregm
 *
 */
public class Converter {
	private static final Logger logger = Logger.getLogger(Converter.class.getName());
	private static final Pattern timeStampPattern = Pattern.compile("(?<year>\\d+)-(?<month>\\d+)-(?<day>\\d+)\\s(?<hour>\\d+):(?<minute>\\d+):(?<second>\\d+)\\s(?<timeZone>\\w+)", Pattern.CASE_INSENSITIVE);
	private static final TimeZone easternTimeZone = TimeZone.getTimeZone("US/Eastern");
	private static final TimeZone centralTimeZone = TimeZone.getTimeZone("US/Central");
	private static final TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");
	private static SimpleDateFormat utcDateFormat = null;
	private static SimpleDateFormat utcTimeFormat = null;
	private String csvFileName = null;
	private String avroFileName = null;
	private char delimiter = ',';
	private char quote = '\"';
	private String lineSeparator = "\n";
	
	static {
		TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");
		utcDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		utcDateFormat.setTimeZone(utcTimeZone);
		utcTimeFormat = new SimpleDateFormat("HH:mm:ss");
		utcTimeFormat.setTimeZone(utcTimeZone);
	}
	
	private static UTCTimeStamp localToGMT(int year, int month, int day, int hour, int minute, int second, String localTimeZone) throws Exception {
		//CDT => Central Daylight (plus 5 hours for UTC)
		//EDT => Eastern Daylight (plus 4 hours for UTC)
		//EST => Eastern Standard (plus 5 hours for UTC)
		//CST => Central Standard (plus 6 hours for UTC)
		TimeZone timeZone = null;
		boolean isDST = false;
		if (localTimeZone.compareToIgnoreCase("CDT") == 0) {
			timeZone = centralTimeZone;
			isDST = true;
		} else if (localTimeZone.compareToIgnoreCase("CST") == 0) {
			timeZone = centralTimeZone;
		} else if (localTimeZone.compareToIgnoreCase("EDT") == 0) {
			timeZone = easternTimeZone;
			isDST = true;
		} else if (localTimeZone.compareToIgnoreCase("EST") == 0) {
			timeZone = easternTimeZone;
		} else if (localTimeZone.compareToIgnoreCase("UTC") == 0) {
			timeZone = utcTimeZone;
		} else {
			throw new Exception(MessageFormat.format("Cannot handle timezone {0}.", localTimeZone));
		}
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.DST_OFFSET, (isDST ? 3600000 : 0));
		calendar.setTimeZone(timeZone);
		calendar.set(Calendar.YEAR, year);
		calendar.set(Calendar.MONTH, month - 1);
		calendar.set(Calendar.DAY_OF_MONTH, day);
		calendar.set(Calendar.HOUR_OF_DAY, hour);
		calendar.set(Calendar.MINUTE, minute);
		calendar.set(Calendar.SECOND, second);
	    String datePart = utcDateFormat.format(calendar.getTime());
	    String timePart = utcTimeFormat.format(calendar.getTime());
	    int utcYear = Integer.parseInt(datePart.substring(0, 4));
	    int utcMonth = Integer.parseInt(datePart.substring(5, 7));
	    int utcDay = Integer.parseInt(datePart.substring(8));
	    String utcTimeStamp = MessageFormat.format("{0}T{1}Z", datePart, timePart);
	    return new UTCTimeStamp(utcYear, utcMonth, utcDay, utcTimeStamp);
	}
	
	public String getCsvFileName() {
		return this.csvFileName;
	}

	public void setCsvFileName(String newVal) {
		this.csvFileName = newVal;
	}

	public String getAvroFileName() {
		return this.avroFileName;
	}

	public void setAvroFileName(String newVal) {
		this.avroFileName = newVal;
	}
	
	public char getDelimiter() {
		return this.delimiter;
	}

	public void setDelimiter(char newVal) {
		this.delimiter = newVal;
	}

	public char getQuote() {
		return this.quote;
	}

	public void setQuote(char newVal) {
		this.quote = newVal;
	}

	public String getLineSeparator() {
		return this.lineSeparator;
	}

	public void setLineSeparator(String newVal) {
		this.lineSeparator = newVal;
	}

	public void convert() throws IOException {
		logger.info("csvFileName: " + this.csvFileName);
		logger.info("avroFileName: " + this.avroFileName);
		
		File csvFile = new File(this.csvFileName);
		final File avroFile = new File(this.avroFileName);
		
		if (avroFile.exists()) {
			Path csvPath = Paths.get(this.csvFileName);
            BasicFileAttributes csvAttr = Files.readAttributes(csvPath, BasicFileAttributes.class);
            logger.info("csvLastModifiedTime: " + csvAttr.lastModifiedTime());
            Path avroPath = Paths.get(this.avroFileName);
            BasicFileAttributes avroAttr = Files.readAttributes(avroPath, BasicFileAttributes.class);
            logger.info("avroLastModifiedTime: " + avroAttr.lastModifiedTime());
            if (csvAttr.lastModifiedTime().toMillis() < avroAttr.lastModifiedTime().toMillis()) {
            	logger.info("Already written. Won't do it again.");
            	return;
            }
		}
		
		RecordBuilder<Schema> recordBuilder = SchemaBuilder.record("AccountingDTO");
		recordBuilder.namespace("ca.datamagic.accounting.dto");
		FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();
		fieldAssembler.requiredString("avroFile");
		fieldAssembler.requiredString("utcTimeStamp");
		fieldAssembler.requiredInt("utcYear");
		fieldAssembler.requiredInt("utcMonth");
		fieldAssembler.requiredInt("utcDay");
		fieldAssembler.optionalDouble("deviceLatitude");
		fieldAssembler.optionalDouble("deviceLongitude");
		fieldAssembler.requiredString("eventName");
		fieldAssembler.requiredString("eventMessage");
		final Schema schema = fieldAssembler.endRecord();
		
		final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
		final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
		dataFileWriter.create(schema, avroFile);
				
		ObjectRowProcessor processor = new ObjectRowProcessor() {				
			@Override
			public void rowProcessed(Object[] row, ParsingContext context) {
				try {
					String timeStamp = null;
					Double deviceLatitude = null;
					Double deviceLongitude = null;
					String eventName = null;
					String eventMessage = null;				
					if ((row.length > 0) && (row[0] != null)) {
						timeStamp = row[0].toString();
					}
					if ((row.length > 1) && (row[1] != null)) {
						deviceLatitude = Double.parseDouble(row[1].toString());
					}
					if ((row.length > 2) && (row[2] != null)) {
						deviceLongitude = Double.parseDouble(row[2].toString());
					}
					if ((row.length > 3) && (row[3] != null)) {
						eventName = row[3].toString();
					}
					if ((row.length > 4) && (row[4] != null)) {
						eventMessage = row[4].toString();
					}
					if ((timeStamp != null) && (eventName != null) && (eventMessage != null)) {
						Matcher timeStampMatcher = timeStampPattern.matcher(timeStamp);
						if (timeStampMatcher.matches()) {
							int year = Integer.parseInt(timeStampMatcher.group("year"));
							int month = Integer.parseInt(timeStampMatcher.group("month"));
							int day = Integer.parseInt(timeStampMatcher.group("day"));
							int hour = Integer.parseInt(timeStampMatcher.group("hour"));
							int minute = Integer.parseInt(timeStampMatcher.group("minute"));
							int second = Integer.parseInt(timeStampMatcher.group("second"));
							String timeZone = timeStampMatcher.group("timeZone");
							
							UTCTimeStamp timeStampUTC = localToGMT(year, month, day, hour, minute, second, timeZone);
							logger.info("timeStampUTC: " + timeStampUTC);
							
							GenericRecord record = new GenericData.Record(schema);
							record.put("avroFile", avroFile.getName());
							record.put("utcTimeStamp", timeStampUTC.getUTCTimeStamp());
							record.put("utcYear", timeStampUTC.getUTCYear());
							record.put("utcMonth", timeStampUTC.getUTCMonth());
							record.put("utcDay", timeStampUTC.getUTCDay());
							record.put("deviceLatitude", deviceLatitude);
							record.put("deviceLongitude", deviceLongitude);
							record.put("eventName", eventName);
							record.put("eventMessage", eventMessage);
							dataFileWriter.append(record);
						}
					}
				} catch (Exception ex) {
					logger.warning("Error converting record to AVRO at line #" + Long.toString(context.currentLine()));
				}					
			}
		};
		
		CsvFormat csvFormat = new CsvFormat();
		csvFormat.setDelimiter(',');
		csvFormat.setQuote('\"');
		csvFormat.setLineSeparator("\n");
		CsvParserSettings csvSettings = new CsvParserSettings();
		csvSettings.setFormat(csvFormat);
		csvSettings.setAutoClosingEnabled(true);
		csvSettings.setNullValue(null);
		csvSettings.setProcessor(processor);
		csvSettings.setHeaderExtractionEnabled(false);
		CsvParser csvParser = new CsvParser(csvSettings);
		csvParser.parse(csvFile);
		
		dataFileWriter.flush();
		dataFileWriter.close();
	}
	
	private static class UTCTimeStamp {
		private Integer utcYear = null;
		private Integer utcMonth = null;
		private Integer utcDay = null;
		private String utcTimeStamp = null;
	    
		public UTCTimeStamp(Integer utcYear, Integer utcMonth, Integer utcDay, String utcTimeStamp) {
			this.utcYear = utcYear;
			this.utcMonth = utcMonth;
			this.utcDay = utcDay;
			this.utcTimeStamp = utcTimeStamp;
		}
		
		public Integer getUTCYear() {
			return this.utcYear;
		}
		
		public Integer getUTCMonth() {
			return this.utcMonth;
		}
		
		public Integer getUTCDay() {
			return this.utcDay;
		}
		
		public String getUTCTimeStamp() {
			return this.utcTimeStamp;
		}
		
		@Override
		public String toString() {
			return this.utcTimeStamp;
		}
	}
}
