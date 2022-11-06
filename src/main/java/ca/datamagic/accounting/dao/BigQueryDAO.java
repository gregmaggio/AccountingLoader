/**
 * 
 */
package ca.datamagic.accounting.dao;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;

/**
 * @author gregm
 *
 */
public class BigQueryDAO extends BaseDAO {
	private static final Logger logger = LogManager.getLogger(BigQueryDAO.class);
	private BigQuery bigQuery = null;
	
	public String getProjectId() throws IOException {
		return this.getProperties().getProperty("projectId");
	}
	
	public String getDatasetName() throws IOException {
		return this.getProperties().getProperty("datasetName");
	}

	public String getTableName() throws IOException {
		return this.getProperties().getProperty("tableName");
	}
	
	public BigQuery getBigQuery() throws IOException {
		if (this.bigQuery == null) {
			this.bigQuery = BigQueryOptions.newBuilder().setCredentials(getCredentials()).build().getService();
		}
		return this.bigQuery;
	}
	
	public long getEvents(int year, int month, int day) throws IOException, InterruptedException {
		String query = MessageFormat.format("select count(eventName) as events from {0}.{1}.{2} where utcYear = {3} and utcMonth = {4} and utcDay = {5}", 
				getProjectId(), 
				getDatasetName(), 
				getTableName(),
				Integer.toString(year),
				month,
				day);
		logger.debug("query: " + query);
		TableResult result = runQuery(query);
		Iterator<FieldValueList> iterator = result.iterateAll().iterator();
		if (iterator.hasNext()) {
			FieldValueList row = iterator.next();
			return row.get("events").getLongValue();
		}
		return 0L;
	}

	public void deleteEvents(int year, int month, int day) throws IOException, InterruptedException {
		String query = MessageFormat.format("delete from {0}.{1}.{2} where utcYear = {3} and utcMonth = {4} and utcDay = {5}", 
				getProjectId(), 
				getDatasetName(), 
				getTableName(),
				Integer.toString(year),
				month,
				day);
		logger.debug("query: " + query);
		runQuery(query);
	}
	
	public void loadAvro(String sourceUri) throws IOException, InterruptedException {		
		TableId tableId = TableId.of(getDatasetName(), getTableName());
		LoadJobConfiguration loadJobConfiguration = LoadJobConfiguration.newBuilder(tableId, sourceUri)
				.setFormatOptions(FormatOptions.avro())
				.setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
				.build();
		Job job = this.getBigQuery().create(JobInfo.of(loadJobConfiguration));
		job = job.waitFor();
		if (!job.isDone()) {
			throw new IOException("Error loading AVRO: " + sourceUri);
		}
	}
	
	public TableResult runQuery(String query) throws IOException, InterruptedException {
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query)
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(false)
                        .build();
        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString().toUpperCase());
        Job queryJob = this.getBigQuery().create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        // Get the results
        return queryJob.getQueryResults();
    }
}
