import org.apache.avro.Schema;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.LinkedHashMap;

import javax.crypto.SealedObject;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;

import org.apache.avro.generic.GenericRecord;

import org.apache.avro.io.DatumReader;



public class TestAvro {

    

	public static void main (String[] args) throws IOException {
		// create a schema
		// Schema schema = new Schema.Parser().parse(new File("/home/sekhrivijay/src/dataflow/gcs-to-spanner/src/main/resources/schema/avro/rcs.avsc"));
		// create a record using schema
		// GenericRecord AvroRec = new GenericData.Record();
		File AvroFile = new File("/tmp/output");
        // String initialString = "text";
        // InputStream targetStream = new ByteArrayInputStream(initialString.getBytes());
        InputStream targetStream = new ByteArrayInputStream(Files.readAllBytes(AvroFile.toPath()));
        
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
        DataFileStream<GenericRecord> dataFileReader = new DataFileStream<GenericRecord>(targetStream, datumReader);
		// DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(AvroFile, datumReader);

		System.out.println("Deserialized data is :");
		while (dataFileReader.hasNext()) {
			GenericRecord AvroRec = dataFileReader.next();
			System.out.println(AvroRec);
		}
	}

}
