package com.demograft.ldb4rdbconverter;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
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
import java.util.*;

import org.apache.avro.Schema;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.avro.AvroParquetWriter;

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

    private int files = 0;

    private DateTimeFormatter timeFormatter;

    private StringBuilder statistics = new StringBuilder();

    private HashMap<String, Integer> IdHash = new HashMap<>();

    private String formatName(String name){
        name = name.replaceAll("_", " ");
        StringBuilder newName = new StringBuilder();
        for(int i = 0; i < name.length() - 2; i++){
            if(Character.isLowerCase(name.charAt(i)) && Character.isUpperCase(name.charAt(i + 1)) && Character.isLowerCase(name.charAt(i + 2))){
                newName.append(name.charAt(i));
                newName.append(" ");
            }
            else{
                newName.append(name.charAt(i));
            }
        }
        newName.append(name.charAt(name.length()-2));
        newName.append(name.charAt(name.length()-1));
        return newName.toString();
    }

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

    static DateTimeFormatter identifyTimeFormat(String time){
        try {
            DateTimeFormatter formatter
                    = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS xx");
            ZonedDateTime date = ZonedDateTime.parse(time, formatter);
            return formatter;
        }
        catch(DateTimeParseException e) {
        }
        try {
            DateTimeFormatter formatter
                    = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

            LocalDateTime localdate = LocalDateTime.parse(time, formatter);
            ZonedDateTime date = ZonedDateTime.of(localdate, ZoneId.ofOffset("", ZoneOffset.of("+0000")));
            return formatter;
        }
        catch(DateTimeParseException e1) {
        }
        try {
            DateTimeFormatter formatter
                    = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

            ZonedDateTime date = ZonedDateTime.parse(time, formatter);
            return formatter;
        }
        catch(DateTimeParseException e2) {
        }
        try{
            DateTimeFormatter formatter
                    = DateTimeFormatter.ISO_ZONED_DATE_TIME;

            ZonedDateTime date = ZonedDateTime.parse(time, formatter);
            return formatter;
        }
        catch(DateTimeParseException e3){
            throw new RuntimeException("Error. Couldn't get DateTime parser from first example row. Does not match any common date time formats.");
        }
    }

    static long timeToMillisecondsConverter(String time, DateTimeFormatter format) throws DateTimeParseException {
        ZonedDateTime date = ZonedDateTime.parse(time, format);
        return date.toInstant().toEpochMilli();
    }

    // Takes the CSV file/folder given by the command line and parses all of the records in it.
    private List<Record> parseCSV(){
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        CsvParser parser = new CsvParser(settings);
        List<Record> parsedRecords = new ArrayList<>();
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
                            newRecords.remove(0);
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

    private Schema.Type getSchemaType(Schema subschema, String field){
        if(subschema.getType() == Schema.Type.UNION){
            if(field == null){
                return subschema.getTypes().get(0).getType();
            }
            else{
                return subschema.getTypes().get(1).getType();
            }
        }
        else{
            return subschema.getType();
        }
    }

    private Schema.Type getSchemaType(Schema subschema){
        if(subschema.getType() == Schema.Type.UNION){
            return subschema.getTypes().get(1).getType();
        }
        else{
            return subschema.getType();
        }
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
            }
            else if(headername.equals(time)){
                jo.put("name", "time");
                jo.put("type", "long");
                list.add(jo);
            }
            else {
                try {
                    long time = timeToMillisecondsConverter(example, timeFormatter);
                    jo.put("name", headername);
                    JSONArray typelist = new JSONArray();
                    typelist.add("null");
                    typelist.add("long");
                    jo.put("type", typelist);
                    list.add(jo);
                } catch (DateTimeParseException e1) {
                    try {
                        float data = Float.parseFloat(example);
                        jo.put("name", headername);
                        JSONArray typelist = new JSONArray();
                        typelist.add("null");
                        typelist.add("float");
                        jo.put("type", typelist);
                        list.add(jo);
                    } catch (NumberFormatException e2) {
                        JSONArray typelist = new JSONArray();
                        jo.put("name", headername);
                        typelist.add("null");
                        typelist.add("string");
                        jo.put("type", typelist);
                        list.add(jo);
                    }
                } catch (NullPointerException e3) {
                    // If the example in the examplerow is null, ignore that attribute
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
            String formattedName = formatName(field.name());
            if(field.name().equals("lat")){
                row.put("attributeName", "Latitude");
                row.put("attributeTooltip", "Refined location of the record, used by heatmapping and optionally by trajectory visualisation function" +
                        "\nGeographic WGS84 coordinate in decimal degrees." +
                        "\nIn the current version equals to " + latitude);
                row.put("group","Geographic Location");
                row.put("unit","degrees");
                row.put("attributeId", formattedName);
                data.add(row);
            }
            else if(field.name().equals("lon")){
                row.put("attributeName", "Longitude");
                row.put("attributeTooltip", "Refined location of the record, used by heatmapping and optionally by trajectory visualisation function" +
                        "\\nGeographic WGS84 coordinate in decimal degrees." +
                        "\\nIn the current version equals to " + longitude);
                row.put("group","Geographic Location");
                row.put("unit","degrees");
                row.put("attributeId", formattedName);
                data.add(row);
            } else if(getSchemaType(field.schema()) == Schema.Type.LONG && field.name().equals("time")){

                // guiType:dateTime

                row.put("attributeName", "Time");
                row.put("attributeTooltip", formattedName);
                row.put("group","Generic");
                row.put("attributeId", formattedName);
                row.put("guiType", "dateTime");
                data.add(row);
            }
            else if(getSchemaType(field.schema()) == Schema.Type.LONG){
                row.put("attributeName", formattedName);
                row.put("attributeTooltip", formattedName);
                row.put("group","Generic");
                row.put("attributeId", formattedName);
                row.put("guiType", "dateTime");
                data.add(row);
            }
            else{
                row.put("attributeName", formattedName);
                row.put("attributeTooltip", formattedName);
                row.put("group","Generic");
                row.put("attributeId", formattedName);
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
        HashMap<String, Integer[]> statsTable = new HashMap<>();
        HashMap<String, Float[]> minMaxTable = new HashMap<>();


        /* Statistics look as follows:  3 numbers for each row, they indicate:
        1. Number of non-null values
        2. Number of malformed values
        3. Number of NULL values

        Minmax values table as follows:

        1. Min value of this column
        2. Max value of this column

       */
        for (Schema.Field field: schema.getFields()){
            statsTable.put(field.name(), new Integer[]{0,0,0});
            if (getSchemaType(field.schema()) == Schema.Type.FLOAT){
                minMaxTable.put(field.name(), new Float[]{Float.MAX_VALUE, Float.MIN_VALUE});
            }
        }
        statistics.append("Parquet file generation successful. \n \n \nGeneral statistics: \nFound a total of " + csvRecords.size() + " records. ");
        for(Record record: csvRecords){
            boolean malformed = false;
            GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
            for (Schema.Field field: schema.getFields()) {
                Schema subschema = field.schema();

                String targetName;

                //Union data types always have 2 types, first is null and second is the needed type.
                if(field.name().equals("lat")){
                    targetName = latitude;
                }
                else if(field.name().equals("lon")){
                    targetName = longitude;
                }
                else if(field.name().equals("time")){
                    targetName = time;
                }
                else{
                    targetName = field.name();
                }
                Schema.Type subSchemaType = getSchemaType(subschema,record.getString(targetName));


                switch(subSchemaType){

                    // Long currently only used for dateTimes

                    case LONG:
                        if (field.name().equals("time")){
                            try {
                                if(record.getString(time).equals("")){
                                    throw new NullPointerException();
                                }
                                Long timeval = timeToMillisecondsConverter(record.getString(time), timeFormatter);
                                genericRecordBuilder = genericRecordBuilder.set("time", timeval);
                                Integer[] stat2 = statsTable.get("time");
                                stat2[0] += 1;
                                statsTable.put("time", stat2);
                            } catch (DateTimeParseException e) {
                                Integer[] stat2 = statsTable.get("time");
                                stat2[0] += 1;
                                stat2[1] += 1;
                                statsTable.put("time", stat2);
                                malformed = true;
                                break;
                            }
                            catch(NullPointerException e1){
                                Integer[] stat = statsTable.get("time");
                                stat[2] += 1;
                                statsTable.put("time", stat);
                                malformed = true;
                                break;
                            }
                        }
                        else {
                             // Could be null or long
                                try {
                                    genericRecordBuilder = genericRecordBuilder.set(field, timeToMillisecondsConverter(record.getString(field.name()), timeFormatter));
                                    Integer[] stat = statsTable.get(field.name());
                                    stat[0] += 1;
                                    statsTable.put(field.name(), stat);
                                } catch (DateTimeParseException e) {
                                    Integer[] stat = statsTable.get(field.name());
                                    stat[1] += 1;
                                    statsTable.put(field.name(), stat);
                                    genericRecordBuilder = genericRecordBuilder.set(field, null);
                                }
                            }
                        break;
                    case DOUBLE:
                        if (field.name().equals("lon")){
                            try{
                                if(record.getString(longitude).equals("")){
                                    throw new NullPointerException();
                                }
                                genericRecordBuilder = genericRecordBuilder.set("lon", record.getDouble(longitude));
                                Integer[] stat2 = statsTable.get("lon");
                                stat2[0] += 1;
                                statsTable.put("lon", stat2);
                            }
                            catch(NumberFormatException e){
                                Integer[] stat2 = statsTable.get("lon");
                                stat2[0] += 1;
                                stat2[1] += 1;
                                statsTable.put("lon", stat2);
                                malformed = true;
                                break;
                            }
                            catch(NullPointerException e1){
                                Integer[] stat = statsTable.get("lon");
                                stat[2] += 1;
                                statsTable.put("lon", stat);
                                malformed = true;
                                break;
                            }
                        }
                        else if(field.name().equals("lat")){
                                try{
                                    if(record.getString(latitude).equals("")){
                                        throw new NullPointerException();
                                    }
                                    genericRecordBuilder = genericRecordBuilder.set("lat", record.getDouble(latitude));
                                    Integer[] stat2 = statsTable.get("lat");
                                    stat2[0] += 1;
                                    statsTable.put("lat", stat2);
                                }
                                catch(NumberFormatException e){
                                    Integer[] stat2 = statsTable.get("lat");
                                    stat2[0] += 1;
                                    stat2[1] += 1;
                                    statsTable.put("lat", stat2);
                                    malformed = true;
                                    break;
                                }
                                catch(NullPointerException e1){
                                    Integer[] stat = statsTable.get("lat");
                                    stat[2] += 1;
                                    statsTable.put("lat", stat);
                                    malformed = true;
                                    break;
                                }
                        }
                        else {
                            try {
                                genericRecordBuilder = genericRecordBuilder.set(field, record.getDouble(field.name()));
                                Integer[] stat = statsTable.get(field.name());
                                stat[0] += 1;
                                statsTable.put(field.name(), stat);
                            }
                            catch(NumberFormatException e){
                                Integer[] stat2 = statsTable.get(field.name());
                                stat2[1] += 1;
                                statsTable.put(field.name(), stat2);
                                genericRecordBuilder = genericRecordBuilder.set(field, null);
                            }
                        }
                        break;

                    case FLOAT:
                        try{
                            // Max values are sometimes used to indicate missing data.
                            if(record.getInt(field.name()) == Integer.MAX_VALUE) {
                                throw new NumberFormatException();
                            }
                            Float foundValue = record.getFloat(field.name());
                            genericRecordBuilder = genericRecordBuilder.set(field, foundValue);
                            Integer[] stat = statsTable.get(field.name());
                            Float[] minmax = minMaxTable.get(field.name());
                            stat[0] += 1;
                            statsTable.put(field.name(), stat);
                            if(minmax[0] > foundValue){
                                minmax[0] = foundValue;
                            }
                            if(minmax[1] < foundValue){
                                minmax[1] = foundValue;
                            }
                            minMaxTable.put(field.name(), minmax);
                        }
                        catch(NumberFormatException e1){
                            Integer[] stat2 = statsTable.get(field.name());
                            stat2[0] += 1;
                            stat2[1] += 1;
                            statsTable.put(field.name(), stat2);
                            genericRecordBuilder = genericRecordBuilder.set(field, null);
                        }
                        catch(NullPointerException e){
                            Integer[] stat = statsTable.get(field.name());
                            stat[2] += 1;
                            statsTable.put(field.name(), stat);
                            genericRecordBuilder = genericRecordBuilder.set(field, null);
                        }
                        break;
                    case STRING:
                        genericRecordBuilder = genericRecordBuilder.set(field, record.getString(field.name()));
                        Integer[] stat = statsTable.get(field.name());
                        stat[0] += 1;
                        statsTable.put(field.name(), stat);
                        break;
                    case NULL:
                        Integer[] stat2 = statsTable.get(field.name());
                        stat2[2] += 1;
                        statsTable.put(field.name(), stat2);
                        genericRecordBuilder = genericRecordBuilder.set(field, null);
                        break;

                        //Union data types always have 2 types, first is null and second is the needed type.
                }
            }
            if(!malformed) {
                GenericData.Record towrite = genericRecordBuilder.build();
                toWriteList.add(towrite);
            }
        }
        statistics.append(toWriteList.size() + " records from " + files + " files parsed into the parquet file. " + (csvRecords.size() - toWriteList.size()) + " records contained malformed lat, lon or time fields.\n");
        statistics.append("Field statistics in the form of: field name, non-null values, null values :  \n\n");
        for(String key: statsTable.keySet()){
            statistics.append(key + " , non-null: " + statsTable.get(key)[0] + " , invalid: " + statsTable.get(key)[1] + " , null: " + statsTable.get(key)[2] + "\n");
            if(minMaxTable.containsKey(key)){
                statistics.append("     Min: " + minMaxTable.get(key)[0] + "       Max: " + minMaxTable.get(key)[1] + "\n");
            }
        }
        return toWriteList;
    }

    private void run() {

        List<Record> allRecords = parseCSV();
        Record headers = allRecords.get(0);
        Record examplerow = allRecords.get(1);
        String[] headerarray = headers.getValues();
        String[] examplearray = examplerow.getValues();
        timeFormatter = identifyTimeFormat(examplerow.getString(time));
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

        try(FileWriter fw = new FileWriter("stats.txt")){
            fw.write(statistics.toString());
        }
        catch(IOException e){
            System.out.println("Error writing the statistics file. Error message: " + e.getMessage());
        }

    }
}