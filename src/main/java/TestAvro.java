import org.apache.avro.Schema;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;


import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;

import org.apache.avro.generic.GenericRecord;

import org.apache.avro.io.DatumReader;



public class TestAvro {

    

	public static void main (String[] args) throws IOException {
		// create a schema
		Schema schema = new Schema.Parser().parse(new File("/home/sekhrivijay/src/dataflow/gcs-to-spanner/src/main/resources/schema/avro/rcs.avsc"));
		// create a record using schema
		GenericRecord AvroRec = new GenericData.Record(schema);
		File AvroFile = new File("/tmp/output");
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(AvroFile, datumReader);
		System.out.println("Deserialized data is :");
		while (dataFileReader.hasNext()) {
			AvroRec = dataFileReader.next(AvroRec);
			System.out.println(AvroRec);
		}
	}

}
