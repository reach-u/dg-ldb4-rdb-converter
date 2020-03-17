package com.demograft.ldb4rdbconverter;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.JsonProperties;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

    @Option(name = "--output-file", required = true, usage = "Output file, outputs one parquet file")
    private String outputFile;

    @Option(name = "--longitude", required = false, usage = "Redefine the longitude row")
    private String longitude = "longitude";

    @Option(name = "--latitude", required = false, usage = "Redefine the latitude row")
    private String latitude = "latitude";

    @Option(name = "--time", required = false, usage = "Redefine the time row")
    private String time = "time";



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

    static long timeToMillisecondsConverter(String time) throws DateTimeParseException {
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

                    DateTimeFormatter formatter
                            = DateTimeFormatter.ISO_ZONED_DATE_TIME;

                    ZonedDateTime date = ZonedDateTime.parse(time, formatter);
                    return date.toInstant().toEpochMilli();

                }
            }
        }
    }

    // Takes the CSV file/folder given by the command line and parses all of the records in it.
    private List<Record> parseCSV(){
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        CsvParser parser = new CsvParser(settings);
        List<Record> parsedRecords = new ArrayList<>();
        int files = 0;
        if (inputFile.getName().endsWith(".csv")) {
            parsedRecords = parser.parseAllRecords(inputFile);
        } else if (inputFile.isDirectory()) {
            File[] listOfFiles = inputFile.listFiles();
            for (File file : listOfFiles) {
                try {
                    if (file.getName().endsWith(".csv")) {
                        files += 1;
                        List<Record> newRecords = parser.parseAllRecords(file);
                        if(files > 1){
                            parsedRecords.remove(0);
                        }
                        parsedRecords.addAll(newRecords);
                    }
                } catch (NullPointerException e) {
                    throw new RuntimeException("Specified directory doesn't exist");
                }
            }
        } else {
            throw new RuntimeException("Error finding the file. Make sure the file name or directory name is correct and that all the files are .csv files.");
        }
        return parsedRecords;
    }

    private JSONObject createJSONFromCSVRecords(String[] headerrow, String[] examplerow){
        JSONObject mainjson = new JSONObject();
        mainjson.put("name", "locationrecord");
        mainjson.put("type", "record");
        mainjson.put("namespace", "com.demograft.ldb4rdbconverter.generated");
        JSONArray list = new JSONArray();
        for(int i = 0; i < headerrow.length; i++){
            String headername = headerrow[i].trim();
            String example = examplerow[i];
            JSONObject jo = new JSONObject();
            // The three most important rows, longitude, latitude and time.

            if(headername.equals(longitude)){
                jo.put("name", "lon");
                jo.put("type", "double");
                list.add(jo);
            }
            else if(headername.equals(latitude)){
                jo.put("name", "lat");
                jo.put("type", "double");
                list.add(jo);
            } else if(headername.equals(time)){
                jo.put("name", "time");
                jo.put("type", "long");
                list.add(jo);
            } else {

                try {
                    long data = timeToMillisecondsConverter(example);
                    jo.put("name", headername);
                    JSONArray typelist = new JSONArray();
                    typelist.add("null");
                    typelist.add("long");
                    jo.put("type", typelist);
                    list.add(jo);
                } catch (DateTimeParseException e) {
                    {
                        try {
                            long data = Long.parseLong(example);
                            jo.put("name", headername);
                            JSONArray typelist = new JSONArray();
                            typelist.add("null");
                            typelist.add("long");
                            jo.put("type", typelist);
                            list.add(jo);
                        } catch (NumberFormatException e2) {
                            try {
                                double data = Double.parseDouble(example);
                                JSONArray typelist = new JSONArray();
                                jo.put("name", headername);
                                typelist.add("null");
                                typelist.add("double");
                                jo.put("type", typelist);
                                list.add(jo);
                            } catch (NumberFormatException e3) {
                                JSONArray typelist = new JSONArray();
                                jo.put("name", headername);
                                typelist.add("null");
                                typelist.add("string");
                                jo.put("type", typelist);
                                list.add(jo);
                            }
                        }
                    }
                } catch (NullPointerException e) {
                    // If in the example data line the value indicated is null, that property is ignored and not used to construct the schema.
                }
            }
        }

        mainjson.put("fields",list);
        return mainjson;
    }

    private void writeAttributeFile(Schema schema, String fileName){
        JSONObject attributeTr = new JSONObject();
        JSONArray data = new JSONArray();
        for (Schema.Field field: schema.getFields()){
            JSONObject row = new JSONObject();
            row.put("hidden", false);
            if(field.name().equals("lat")){
                row.put("attributeName", "Latitude");
                row.put("attributeTooltip", "Refined location of the record, used by heatmapping and optionally by trajectory visualisation function" +
                        "\\nGeographic WGS84 coordinate in decimal degrees." +
                        "\\nIn the current version equals to " + latitude);
                row.put("group","Geographic Location");
                row.put("unit","degrees");
                row.put("attributeId", field.name());
                data.add(row);
            }
            else if(field.name().equals("lon")){
                row.put("attributeName", "Longitude");
                row.put("attributeTooltip", "Refined location of the record, used by heatmapping and optionally by trajectory visualisation function" +
                        "\\nGeographic WGS84 coordinate in decimal degrees." +
                        "\\nIn the current version equals to " + longitude);
                row.put("group","Geographic Location");
                row.put("unit","degrees");
                row.put("attributeId", field.name());
                data.add(row);
            }
            else{
                row.put("attributeName", field.name());
                row.put("attributeTooltip", field.name());
                row.put("group","Generic");
                row.put("attributeId", field.name());
                data.add(row);
            }

        }
        attributeTr.put("data", data);

        try(FileWriter fw = new FileWriter(fileName)){
            fw.write(attributeTr.toJSONString());
        }
        catch(IOException e){
            System.out.println("Error writing the attributeTranslation file. Error message: " + e.getMessage());
        }
    }

    private List<GenericData.Record> convertToParquetRecords(Schema schema, List<Record> csvRecords){
        List<GenericData.Record> toWriteList = new ArrayList<>();

        for(Record record: csvRecords){
            GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
            for (Schema.Field field: schema.getFields()) {
                Schema subschema = field.schema();

                //Union data types always have 2 types, first is null and second is the needed type.

                if(subschema.getType() == Schema.Type.UNION){
                    if(record.getString(field.name()) == null){
                        subschema = subschema.getTypes().get(0);
                    }
                    else{
                        subschema = subschema.getTypes().get(1);
                    }
                }
                switch(subschema.getType()){
                    case LONG:
                        if (field.name().equals("time")){
                            genericRecordBuilder = genericRecordBuilder.set("time", timeToMillisecondsConverter(record.getString(time)));
                        }
                        else {
                            try {
                                genericRecordBuilder = genericRecordBuilder.set(field, record.getLong(field.name()));
                            } catch (NumberFormatException e) {
                                genericRecordBuilder = genericRecordBuilder.set(field,
                                        timeToMillisecondsConverter(record.getString(field.name())));
                            }
                        }
                        break;
                    case DOUBLE:
                        if (field.name().equals("lon")){
                            genericRecordBuilder = genericRecordBuilder.set("lon", record.getDouble(longitude));
                        }
                        else if(field.name().equals("lat")){
                            genericRecordBuilder = genericRecordBuilder.set("lat", record.getDouble(latitude));
                        }
                        else {
                            genericRecordBuilder = genericRecordBuilder.set(field, record.getDouble(field.name()));
                        }
                        break;
                    case STRING:
                        genericRecordBuilder = genericRecordBuilder.set(field, record.getString(field.name()));
                        break;
                    case NULL:
                        genericRecordBuilder = genericRecordBuilder.set(field, null);

                        //Union data types always have 2 types, first is null and second is the needed type.
                }
            }
            GenericData.Record towrite = genericRecordBuilder.build();
            toWriteList.add(towrite);
        }
        return toWriteList;
    }

    private void run() {

        List<Record> allRecords = parseCSV();
        Record headers = allRecords.get(0);
        Record examplerow = allRecords.get(1);
        String[] headerarray = headers.getValues();
        String[] examplearray = examplerow.getValues();
        JSONObject mainjson = createJSONFromCSVRecords(headerarray, examplearray);

        // Take the first data sample and the header row from the data and create a JSON object based on those that is then converted into a schema.



        // Remove the headerrow, because it doesn't contain location data.
        allRecords.remove(0);

        // Create the shcema from json object.

        Schema avroSchema = new Schema.Parser().parse(mainjson.toString());

        // Create the attributeTranslations file for the GUI based on chosen attributes and types.

        writeAttributeFile(avroSchema, "attributeTranslation.json");

        List<GenericData.Record> toWrite = convertToParquetRecords(avroSchema, allRecords);


        /* CSV -> JSON -> Schema -> Record -> Parquetfile
                            |
                            |
                  attributeTranslation.json

        */

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
            for(GenericData.Record obj : toWrite){
                parquetWriter.write(obj);
            }
        }catch(java.io.IOException e){
            System.out.println(String.format("Error writing parquet file %s", e.getMessage()));
            e.printStackTrace();
        }

    }
}