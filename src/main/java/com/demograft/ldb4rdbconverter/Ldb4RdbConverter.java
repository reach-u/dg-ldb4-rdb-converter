package com.demograft.ldb4rdbconverter;

import com.demograft.ldb4rdbconverter.generated.LocationRecord;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroParquetWriter;

import javax.tools.DocumentationTool;

@Slf4j
public class Ldb4RdbConverter {


    @Option(name = "--input-file", required = true, usage = "Input file, accepts only CSV")
    private File inputFile;


    /*
    Väljastab ühe parkett faili
     */
    @Option(name = "--output-file", required = true, usage = "Väljund fail, parkett ")
    private String outputFile;

    @Option(name = "--header", required = false, usage = "Määrab, kas on olemas header row")
    private boolean headerrow = true;



    public static void main(String[] args) {
        Ldb4RdbConverter converter = new Ldb4RdbConverter();
        CmdLineParser parser = new CmdLineParser(converter);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            parser.printUsage(System.err);
            System.exit(1);
        }
        converter.run();
    }

    static long timeToMillisecondsConverter(String time){
        try {
            DateTimeFormatter formatter
                    = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS xx");

            ZonedDateTime date = ZonedDateTime.parse(time, formatter);
            return date.toInstant().toEpochMilli();
        }
        catch(DateTimeParseException e){
            try {
                DateTimeFormatter formatter
                        = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

                LocalDateTime localdate = LocalDateTime.parse(time, formatter);
                ZonedDateTime date = ZonedDateTime.of(localdate, ZoneId.ofOffset("", ZoneOffset.of("+0000")));
                return date.toInstant().toEpochMilli();
            }
            catch(DateTimeParseException e1){
                try {
                    DateTimeFormatter formatter
                            = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

                    ZonedDateTime date = ZonedDateTime.parse(time, formatter);
                    return date.toInstant().toEpochMilli();
                }
                catch(DateTimeParseException e2){
                    try{
                        DateTimeFormatter formatter
                                = DateTimeFormatter.ISO_ZONED_DATE_TIME;

                        ZonedDateTime date = ZonedDateTime.parse(time, formatter);
                        return date.toInstant().toEpochMilli();
                    }
                    catch(DateTimeParseException e3){
                        throw new RuntimeException("Invalid date format");
                    }
                }
            }
        }
    }

    private void run(){
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        CsvParser parser = new CsvParser(settings);
        List<Record> allRecords = new ArrayList<>();
        int files = 0;
        if(inputFile.getName().endsWith(".csv")){
            allRecords = parser.parseAllRecords(inputFile);
            if(headerrow){
                allRecords.remove(0);
            }
        }
        else if(inputFile.isDirectory()) {
            File[] listOfFiles = inputFile.listFiles();
            for(File file: listOfFiles){
                if(file.getName().endsWith(".csv")){
                    files += 1;
                    List<Record> newRecords = parser.parseAllRecords(file);
                    if(headerrow){
                        newRecords.remove(0);
                    }
                    allRecords.addAll(newRecords);
                }
            }
        }
        else {
            throw new RuntimeException("Error finding the file. Make sure the file name or directory name is correct.");
        }
        log.info("Found " + files + " files with a total of " + allRecords.size() + " records.");
        Schema avroSchema = LocationRecord.getClassSchema();
        List<GenericData.Record> parquetrecords = new ArrayList<>();


        //sdasda
        for(Record record: allRecords){
            GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(avroSchema);
            for (Schema.Field field: avroschema) {
                genericRecordBuilder = genericRecordBuilder.set(field, );
        }
            GenericData.Record towrite = genericRecordBuilder.build();
            parquetrecords.add(towrite);
        }
        // CSV -> JSON -> Schema -> Record -> Parquetfile


        int blockSize = 1024;
        int pageSize = 65535;


        Path output = new Path(outputFile);
        try(
                AvroParquetWriter parquetWriter = new AvroParquetWriter(
                        output,
                        avroSchema,
                        CompressionCodecName.SNAPPY,
                        blockSize,
                        pageSize)
        ){
            for(GenericData.Record obj : parquetrecords){
                parquetWriter.write(obj);
            }
        }catch(java.io.IOException e){
            System.out.println(String.format("Error writing parquet file %s", e.getMessage()));
            e.printStackTrace();
        }
    }

}