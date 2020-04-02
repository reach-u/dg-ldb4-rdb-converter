package com.demograft.ldb4rdbconverter;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.JsonProperties;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.*;
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

    private File inputFile;
    private String outputFile;

    @Option(name = "--config-file", required = true, usage = "Configuration file to configure generator")
    private String configFile = "";

    private String longitude = "";
    private String latitude = "";
    private String time = "";

    private int files = 0;

    private List<String> columnsToRemove = new ArrayList<>();

    private DateTimeFormatter timeFormatter;

    private StringBuilder statistics = new StringBuilder();

    private List<String> hashColumns = new ArrayList<>();

    private List<Float> float_null_values = new ArrayList<>();

    private List<Double> double_null_values = new ArrayList<>();

    private List<Long> long_null_values = new ArrayList<>();

    private List<String> retain_long_columns = new ArrayList<>();

    private Long[] hashMapCounters;

    private List<String> long_columns = new ArrayList<>();

    private List<String> float_columns = new ArrayList<>();

    private List<String> double_columns = new ArrayList<>();

    private List<String> string_columns = new ArrayList<>();

    private List<HashMap<String, Long>> hashTables = new ArrayList<>();

    private HashMap<String, Integer[]> statsTable = new HashMap<>();

    private HashMap<String, Float[]> minMaxTable = new HashMap<>();

    private HashMap<String, Schema.Type> typeTable = new HashMap<>();

    private Integer totalRecords = 0;

    private Integer writtenRecords = 0;

    /* Statistics look as follows: 4 numbers for each row, they indicate:
        1. Number of non-null values
        2. Number of malformed values
        3. Number of NULL values
        4. Number of zero-values (0.0 for float, double; 0 for long)

        Minmax values table as follows:

        1. Min value of this column
        2. Max value of this column

       */

    private final int COL_NON_NULL_VALUES = 0;

    private final int COL_MALFORMED_VALUES = 1;

    private final int COL_NULL_VALUES = 2;

    private final int COL_ZERO_VALUES = 3;

    private final int COL_MIN_VALUE = 0;

    private final int COL_MAX_VALUE = 1;

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
            boolean missing = false;
            String headername = headerrow[i].trim();
            String example = examplerow[i];
            JSONObject jo = new JSONObject();
            // The three most important rows, longitude, latitude and time.
            if(headername.equals(longitude)){
                jo.put("name", "lon");
                jo.put("type", "double");
            }
            else if(headername.equals(latitude)){
                jo.put("name", "lat");
                jo.put("type", "double");
            }
            else if(headername.equals(time)){
                jo.put("name", "time");
                jo.put("type", "long");
            }

            // Predefined rows

            else if(long_columns.contains(headername)){
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add("long");
                jo.put("type", typelist);
            }
            else if(float_columns.contains(headername)){
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add("float");
                jo.put("type", typelist);
            }
            else if(double_columns.contains(headername)){
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add("double");
                jo.put("type", typelist);
            }
            else if(string_columns.contains(headername)){
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add("string");
                jo.put("type", typelist);
            }

            // Try to determine column type

            else{
                try {
                    long time = timeToMillisecondsConverter(example, timeFormatter);
                    jo.put("name", headername);
                    JSONArray typelist = new JSONArray();
                    typelist.add("null");
                    typelist.add("long");
                    jo.put("type", typelist);
                } catch (DateTimeParseException e1) {
                    try {
                        float data = Float.parseFloat(example);
                        jo.put("name", headername);
                        JSONArray typelist = new JSONArray();
                        typelist.add("null");
                        if(retain_long_columns.contains(headername)){
                            typelist.add("long");
                        }
                        else{
                            typelist.add("float");
                        }
                        jo.put("type", typelist);
                    } catch (NumberFormatException e2) {
                        if(example.equals("")){
                            throw new NullPointerException();
                        }
                        JSONArray typelist = new JSONArray();
                        jo.put("name", headername);
                        typelist.add("null");
                        if(hashColumns.contains(headername)){
                            typelist.add("long");
                        }
                        else{
                            typelist.add("string");
                        }
                        jo.put("type", typelist);
                    }
                } catch (NullPointerException e3) {
                   missing = true;
                }
            }
            if(!columnsToRemove.contains(headername) && !missing){
                list.add(jo);
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
            } else if(getSchemaType(field.schema()) == Schema.Type.LONG && field.name().equals("time")){

                // guiType:dateTime

                row.put("attributeName", "Time");
                row.put("attributeTooltip", formattedName);
                row.put("group","Generic");
                row.put("attributeId", field.name());
                row.put("guiType", "dateTime");
                data.add(row);
            }
            else if(getSchemaType(field.schema()) == Schema.Type.LONG){
                row.put("attributeName", formattedName);
                row.put("attributeTooltip", formattedName);
                row.put("group","Generic");
                row.put("attributeId", field.name());
                row.put("guiType", "dateTime");
                data.add(row);
            }
            else{
                row.put("attributeName", formattedName);
                row.put("attributeTooltip", formattedName);
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

    private String typeToString(Schema.Type type){
        if(type == Schema.Type.LONG){
            return "long";
        }
        if(type == Schema.Type.FLOAT){
            return "float";
        }
        if(type == Schema.Type.DOUBLE){
            return "double";
        }
        else{
            return "string";
        }
    }

    private void readConfigFile(){
        Properties defaultProp = new Properties();

        try(FileReader fileReader = new FileReader(configFile)){
            defaultProp.load(fileReader);
        } catch (IOException e) {
            System.out.println("Error reading the configuration file, make sure your configuration file is named correctly.");
        }
        if(!defaultProp.containsKey("input-file")){
            throw new RuntimeException("Missing input-file property in configuration file");
        }
        inputFile = new File(defaultProp.getProperty("input-file"));
        if(!defaultProp.containsKey("output-file")){
            throw new RuntimeException("Missing output-file property in configuration file");
        }
        outputFile = defaultProp.getProperty("output-file");
        if(!defaultProp.containsKey("longitude")){
            throw new RuntimeException("Missing longitude property in configuration file");
        }
        longitude = defaultProp.getProperty("longitude");
        if(!defaultProp.containsKey("latitude")){
            throw new RuntimeException("Missing latitude property in configuration file");
        }
        latitude = defaultProp.getProperty("latitude");
        if(!defaultProp.containsKey("time")){
            throw new RuntimeException("Missing time property in configuration file");
        }
        time = defaultProp.getProperty("time");
        if(defaultProp.containsKey("columns_to_remove")) {
            String propInfo = defaultProp.getProperty("columns_to_remove");
            for (String column: propInfo.split(",")) {
                columnsToRemove.add(column.trim());
            }
        }
        if(defaultProp.containsKey("columns_to_map_long")) {
            String propInfo = defaultProp.getProperty("columns_to_map_long");
            for (String column: propInfo.split(",")) {
                hashColumns.add(column.trim());
            }
        }
        if(defaultProp.containsKey("long_null_values")) {
            String propInfo = defaultProp.getProperty("long_null_values");
            for (String column: propInfo.split(",")) {
                long_null_values.add(Long.parseLong(column.trim()));
            }
        }
        if(defaultProp.containsKey("double_null_values")) {
            String propInfo = defaultProp.getProperty("double_null_values");
            for (String column: propInfo.split(",")) {
                double_null_values.add(Double.parseDouble(column.trim()));
            }
        }
        if(defaultProp.containsKey("float_null_values")) {
            String propInfo = defaultProp.getProperty("float_null_values");
            for (String column: propInfo.split(",")) {
                float_null_values.add(Float.parseFloat(column.trim()));
            }
        }
        if(defaultProp.containsKey("retain_long")) {
            String propInfo = defaultProp.getProperty("retain_long");
            for (String column: propInfo.split(",")) {
                retain_long_columns.add(column.trim());
            }
        }
        if(defaultProp.containsKey("long_columns")) {
            String propInfo = defaultProp.getProperty("long_columns");
            for (String column: propInfo.split(",")) {
                retain_long_columns.add(column.trim());
            }
        }
        if(defaultProp.containsKey("float_columns")) {
            String propInfo = defaultProp.getProperty("float_columns");
            for (String column: propInfo.split(",")) {
                retain_long_columns.add(column.trim());
            }
        }
        if(defaultProp.containsKey("double_columns")) {
            String propInfo = defaultProp.getProperty("double_columns");
            for (String column: propInfo.split(",")) {
                retain_long_columns.add(column.trim());
            }
        }
        if(defaultProp.containsKey("string_columns")) {
            String propInfo = defaultProp.getProperty("string_columns");
            for (String column: propInfo.split(",")) {
                retain_long_columns.add(column.trim());
            }
        }
    }

    private Long generateNewHash(String hash, Integer index){
        if(hashTables.get(index).containsKey(hash)){
            return hashTables.get(index).get(hash);
        }
        Long hashNumber = hashMapCounters[index] + 1;
        hashTables.get(index).put(hash, hashNumber);
        hashMapCounters[index] = hashNumber;
        return hashNumber;
    }

    private GenericData.Record convertToParquetRecord(Schema schema, Record record){
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

                    // Long currently only used for dateTimes and hashes

                    case LONG:
                        if (field.name().equals("time")){
                            try {
                                if(record.getString(time).equals("")){
                                    throw new NullPointerException();
                                }
                                Long timeVal = timeToMillisecondsConverter(record.getString(time), timeFormatter);
                                if(long_null_values.contains(timeVal)){
                                    throw new NullPointerException();
                                }
                                genericRecordBuilder = genericRecordBuilder.set("time", timeVal);
                                Integer[] stat2 = statsTable.get("time");
                                stat2[COL_NON_NULL_VALUES] += 1;
                                statsTable.put("time", stat2);
                            } catch (DateTimeParseException e) {
                                Integer[] stat2 = statsTable.get("time");
                                stat2[COL_NON_NULL_VALUES] += 1;
                                stat2[COL_MALFORMED_VALUES] += 1;
                                statsTable.put("time", stat2);
                                malformed = true;
                                break;
                            }
                            catch(NullPointerException e1){
                                Integer[] stat = statsTable.get("time");
                                stat[COL_NULL_VALUES] += 1;
                                statsTable.put("time", stat);
                                malformed = true;
                                break;
                            }
                        }
                        else if(hashColumns.contains(field.name())){
                            String fieldName = field.name();
                            Integer index = hashColumns.indexOf(fieldName);
                            try {
                                Long hashValue = generateNewHash(record.getString(fieldName), index);
                                genericRecordBuilder = genericRecordBuilder.set(field, hashValue);
                                Integer[] stat = statsTable.get(field.name());
                                stat[COL_NON_NULL_VALUES] += 1;
                                statsTable.put(field.name(), stat);
                                break;
                            }
                            catch (NullPointerException e){
                                Integer[] stat = statsTable.get(field.name());
                                stat[COL_NULL_VALUES] += 1;
                                statsTable.put(field.name(), stat);
                                break;
                            }
                        }
                        else {
                             // Could be null or long
                                try {
                                    Long date = timeToMillisecondsConverter(record.getString(field.name()), timeFormatter);
                                    if(long_null_values.contains(date)){
                                        throw new NullPointerException();
                                    }
                                    genericRecordBuilder = genericRecordBuilder.set(field, date);
                                    Integer[] stat = statsTable.get(field.name());
                                    stat[COL_NON_NULL_VALUES] += 1;
                                    statsTable.put(field.name(), stat);
                                } catch (DateTimeParseException e) {
                                    try {
                                        Long number = record.getLong(field.name());
                                        Integer[] stat = statsTable.get(field.name());
                                        stat[COL_NON_NULL_VALUES] += 1;
                                        if(number == 0L){
                                            stat[COL_ZERO_VALUES] += 1;
                                        }
                                        statsTable.put(field.name(), stat);
                                        genericRecordBuilder = genericRecordBuilder.set(field, number);
                                    }
                                    catch(NumberFormatException e1){
                                        Integer[] stat2 = statsTable.get(field.name());
                                        stat2[COL_NON_NULL_VALUES] += 1;
                                        stat2[COL_MALFORMED_VALUES] += 1;
                                        statsTable.put("lon", stat2);
                                        break;
                                    }
                                }
                                catch(NullPointerException e2){
                                    Integer[] stat = statsTable.get(field.name());
                                    stat[COL_NULL_VALUES] += 1;
                                    statsTable.put(field.name(), stat);
                                    break;
                                }
                            }
                        break;
                    case DOUBLE:
                        if (field.name().equals("lon")){
                            try{
                                if(record.getString(longitude).equals("")){
                                    throw new NullPointerException();
                                }
                                Double number = record.getDouble(longitude);
                                if(double_null_values.contains(number)){
                                    throw new NullPointerException();
                                }
                                genericRecordBuilder = genericRecordBuilder.set("lon", number);
                                Integer[] stat2 = statsTable.get("lon");
                                stat2[COL_NON_NULL_VALUES] += 1;
                                if(number == 0.0D){
                                    stat2[COL_ZERO_VALUES] += 1;
                                }
                                statsTable.put("lon", stat2);
                            }
                            catch(NumberFormatException e){
                                Integer[] stat2 = statsTable.get("lon");
                                stat2[COL_NON_NULL_VALUES] += 1;
                                stat2[COL_MALFORMED_VALUES] += 1;
                                statsTable.put("lon", stat2);
                                malformed = true;
                                break;
                            }
                            catch(NullPointerException e1){
                                Integer[] stat = statsTable.get("lon");
                                stat[COL_NULL_VALUES] += 1;
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
                                    Double number = record.getDouble(latitude);
                                    if(double_null_values.contains(number)){
                                        throw new NullPointerException();
                                    }
                                    genericRecordBuilder = genericRecordBuilder.set("lat", number);
                                    Integer[] stat2 = statsTable.get("lat");
                                    stat2[COL_NON_NULL_VALUES] += 1;
                                    if(number == 0.0D){
                                        stat2[COL_ZERO_VALUES] += 1;
                                    }
                                    statsTable.put("lat", stat2);
                                }
                                catch(NumberFormatException e){
                                    Integer[] stat2 = statsTable.get("lat");
                                    stat2[COL_NON_NULL_VALUES] += 1;
                                    stat2[COL_MALFORMED_VALUES] += 1;
                                    statsTable.put("lat", stat2);
                                    malformed = true;
                                    break;
                                }
                                catch(NullPointerException e1){
                                    Integer[] stat = statsTable.get("lat");
                                    stat[COL_NULL_VALUES] += 1;
                                    statsTable.put("lat", stat);
                                    malformed = true;
                                    break;
                                }
                        }
                        else {
                            try {
                                Double number = record.getDouble(field.name());
                                if(double_null_values.contains(number)){

                                }
                                genericRecordBuilder = genericRecordBuilder.set(field, number);
                                Integer[] stat = statsTable.get(field.name());
                                stat[COL_NON_NULL_VALUES] += 1;
                                if(number == 0.0D){
                                    stat[COL_ZERO_VALUES] += 1;
                                }
                                statsTable.put(field.name(), stat);
                            }
                            catch(NumberFormatException e){
                                Integer[] stat2 = statsTable.get(field.name());
                                stat2[COL_NULL_VALUES] += 1;
                                statsTable.put(field.name(), stat2);
                                genericRecordBuilder = genericRecordBuilder.set(field, null);
                            }
                        }
                        break;

                    case FLOAT:
                        try{
                            Float foundValue = record.getFloat(field.name());
                            // Max values are sometimes used to indicate missing data.
                            if(float_null_values.contains(foundValue)){
                                throw new NullPointerException();
                            }
                            genericRecordBuilder = genericRecordBuilder.set(field, foundValue);
                            Integer[] stat = statsTable.get(field.name());
                            Float[] minmax = minMaxTable.get(field.name());
                            stat[COL_NON_NULL_VALUES] += 1;
                            if(foundValue == 0.0F){
                                stat[COL_ZERO_VALUES] += 1;
                            }
                            statsTable.put(field.name(), stat);
                            if(minmax[COL_MIN_VALUE] > foundValue){
                                minmax[COL_MIN_VALUE] = foundValue;
                            }
                            if(minmax[COL_MAX_VALUE] < foundValue){
                                minmax[COL_MAX_VALUE] = foundValue;
                            }
                            minMaxTable.put(field.name(), minmax);
                        }
                        catch(NumberFormatException e1){
                            Integer[] stat2 = statsTable.get(field.name());
                            stat2[COL_NON_NULL_VALUES] += 1;
                            stat2[COL_MALFORMED_VALUES] += 1;
                            statsTable.put(field.name(), stat2);
                            genericRecordBuilder = genericRecordBuilder.set(field, null);
                        }
                        catch(NullPointerException e){
                            Integer[] stat = statsTable.get(field.name());
                            stat[COL_NULL_VALUES] += 1;
                            statsTable.put(field.name(), stat);
                            genericRecordBuilder = genericRecordBuilder.set(field, null);
                        }
                        break;
                    case STRING:
                        if(record.getString(field.name()).equals("")){
                            Integer[] stat = statsTable.get(field.name());
                            stat[COL_NULL_VALUES] += 1;
                            statsTable.put(field.name(), stat);
                            genericRecordBuilder = genericRecordBuilder.set(field, null);
                            break;
                        }
                        else {
                            genericRecordBuilder = genericRecordBuilder.set(field, record.getString(field.name()));
                            Integer[] stat = statsTable.get(field.name());
                            stat[COL_NON_NULL_VALUES] += 1;
                            statsTable.put(field.name(), stat);
                            break;
                        }
                    case NULL:
                        Integer[] stat2 = statsTable.get(field.name());
                        stat2[COL_NULL_VALUES] += 1;
                        statsTable.put(field.name(), stat2);
                        genericRecordBuilder = genericRecordBuilder.set(field, null);
                        break;

                        //Union data types always have 2 types, first is null and the second is the needed type.
                }
            }
            if(!malformed) {
                totalRecords += 1;
                writtenRecords += 1;
                return genericRecordBuilder.build();
            }
            else{
                totalRecords += 1;
                return null;
            }
    }

    private void writeStatsFile(){
        statistics.append("Parquet file generation successful. \n \n \nGeneral statistics: \nFound a total of " + totalRecords + " records. ");
        statistics.append(writtenRecords + " records from " + files + " files parsed into the parquet file. " + (totalRecords - writtenRecords) + " records contained malformed lat, lon or time fields.\n");
        statistics.append("Field statistics in the form of: field name, non-null values, null values :  \n\n");
        for(String key: statsTable.keySet()){
            statistics.append(key + " , non-null: " + statsTable.get(key)[0] + " , invalid: " + statsTable.get(key)[1] + " , null: " + statsTable.get(key)[2] + " , zero-values: " + statsTable.get(key)[3] + " , type: " + typeToString(typeTable.get(key)) +  "\n");
            if(minMaxTable.containsKey(key)){
                statistics.append("     Min: " + minMaxTable.get(key)[0] + "       Max: " + minMaxTable.get(key)[1] + "\n");
            }
        }
        try(FileWriter fw = new FileWriter("stats.txt")){
            fw.write(statistics.toString());
        }
        catch(IOException e){
            System.out.println("Error writing the statistics file. Error message: " + e.getMessage());
        }
    }

    private void run() {

        if(configFile.equals("")){
            throw new RuntimeException("Configuration file not set, cannot proceed.");
        }
        readConfigFile();
        List<File> csvFiles = new ArrayList<>();

        if(!inputFile.isDirectory()){
            csvFiles.add(inputFile);
        }
        else {
            try {
                for (File file : inputFile.listFiles()) {
                    if (file.getName().endsWith(".csv")) {
                        csvFiles.add(file);
                    }
                }
            }
            catch(NullPointerException e) {
                System.out.println("Error finding the file/directory, please recheck the file name and make sure this file or directory exists.");
            }
        }
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        CsvParser parser = new CsvParser(settings);

        File exampleFile = csvFiles.get(0);
        parser.beginParsing(exampleFile);
        String[] headerArray = parser.parseNextRecord().getValues();
        Record exampleRow = parser.parseNextRecord();
        String[] exampleArray = exampleRow.getValues();
        parser.stopParsing();
        timeFormatter = identifyTimeFormat(exampleRow.getString(time));
        JSONObject mainjson = createJSONFromCSVRecords(headerArray, exampleArray);
        hashMapCounters = new Long[hashColumns.size()];
        for (int i = 0; i < hashColumns.size(); i++){
            HashMap<String, Long> toAdd = new HashMap<>();
            hashTables.add(toAdd);
        }


        Arrays.fill(hashMapCounters, 0L);

        Schema avroSchema = new Schema.Parser().parse(mainjson.toString());
        writeAttributeFile(avroSchema, "attributeTranslation.json");

        for (Schema.Field field: avroSchema.getFields()){
            statsTable.put(field.name(), new Integer[]{0,0,0,0});
            typeTable.put(field.name(), getSchemaType(field.schema()));
            if (getSchemaType(field.schema()) == Schema.Type.FLOAT){
                minMaxTable.put(field.name(), new Float[]{Float.MAX_VALUE, Float.MIN_VALUE});
            }
        }


        /* CSV -> JSON -> Schema -> Record -> Parquetfile
                            |
                            |
                  attributeTranslation.json

        */

        int blockSize = 1024;
        int pageSize = 65535;


        Path output = new Path(outputFile);
        try(
                ParquetWriter<GenericData.Record> parquetWriter = AvroParquetWriter
                        .<GenericData.Record>builder(output)
                        .withSchema(avroSchema)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withRowGroupSize(blockSize)
                        .withPageSize(pageSize)
                        .build()
        ){
            for(File file: csvFiles) {
                parser.beginParsing(file);
                // Remove the header row
                Record record = parser.parseNextRecord();
                try{
                    while((record = parser.parseNextRecord()) != null) {
                        GenericData.Record toWrite = convertToParquetRecord(avroSchema, record);
                        if (toWrite != null) {
                            parquetWriter.write(toWrite);
                        }
                    }
                }
                catch(NullPointerException e){
                    System.out.println("Error. Encountered an empty file or a file with only a header row");
                }
                finally{
                    files += 1;
                    parser.stopParsing();
                }
            }
        }catch(java.io.IOException e){
            System.out.println(String.format("Error writing parquet file %s", e.getMessage()));
            e.printStackTrace();
        }
        writeStatsFile();
    }
}