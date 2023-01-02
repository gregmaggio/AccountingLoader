/**
 * 
 */
package ca.datamagic.accounting.dao;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.Storage.BlobListOption;

/**
 * @author gregm
 *
 */
public class StorageDAO extends BaseDAO {
	private static final Logger logger = Logger.getLogger(StorageDAO.class.getName());
	private Storage storage = null;
	
	public String getBucketName() throws IOException {
		return this.getProperties().getProperty("bucketName");
	}
	
	public Storage getStorage() throws IOException {
		if (this.storage == null) {
			this.storage = StorageOptions.newBuilder().setCredentials(getCredentials()).build().getService();
		}
		return this.storage;
	}
	
	public String upload(String fileName) throws IOException {
		FileInputStream fileInputStream = null;
		try {
			fileInputStream = new FileInputStream(fileName);
			File file = new File(fileName);
			String bucketName = getBucketName();
			String objectName = file.getName();
			BlobId blobId = BlobId.of(bucketName, objectName);
			Blob blob = this.getStorage().get(blobId);
			if (blob != null) {
				if (blob.exists()) {
					Path path = Paths.get(fileName);
		            BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
		            logger.info("lastModifiedTime: " + attr.lastModifiedTime());
					if (blob.getCreateTimeOffsetDateTime().toEpochSecond() > attr.lastModifiedTime().toMillis()) {
						logger.info("Blob already written. Not doing it again.");
						return MessageFormat.format("gs://{0}/{1}", bucketName, objectName);
					}
				}
			}
			BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
			this.getStorage().createFrom(blobInfo, fileInputStream);
			return MessageFormat.format("gs://{0}/{1}", bucketName, objectName);
		} finally {
			if (fileInputStream != null) {
				fileInputStream.close();
			}
		}
	}
	
	public String[] list() throws IOException {
		List<String> list = new ArrayList<>();
		Page<Blob> blobs = this.getStorage().list(getBucketName(), BlobListOption.pageSize(100));
		Iterator<Blob> blobIterator = blobs.iterateAll().iterator();
		while (blobIterator.hasNext()) {
			Blob blob = blobIterator.next();
			list.add(blob.getName());
		}
		String[] array = new String[list.size()];
		list.toArray(array);
		return array;
	}
	
	public String read(String date) throws IOException {
		String bucketName = getBucketName();
		String objectName1 = MessageFormat.format("accounting.avro.{0}", date);
		BlobId blobId1 = BlobId.of(bucketName, objectName1);
		Blob blob1 = this.getStorage().get(blobId1);
		if (blob1 != null) {
			if (blob1.exists()) {
				return MessageFormat.format("gs://{0}/{1}", bucketName, objectName1);
			}
		}
		String objectName2 = MessageFormat.format("accounting.avro.{0}_", date);
		BlobId blobId2 = BlobId.of(bucketName, objectName2);
		Blob blob2 = this.getStorage().get(blobId2);
		if (blob2 != null) {
			if (blob2.exists()) {
				return MessageFormat.format("gs://{0}/{1}", bucketName, objectName2);
			}
		}
		return null;
	}
	
	public void delete(String objectName) throws IOException {
		this.getStorage().delete(getBucketName(), objectName);
	}
}
