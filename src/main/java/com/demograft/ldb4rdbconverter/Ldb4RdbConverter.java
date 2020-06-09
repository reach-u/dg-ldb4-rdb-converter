package com.demograft.ldb4rdbconverter;

import com.demograft.ldb4rdbconverter.parser.CsvInputParser;
import com.demograft.ldb4rdbconverter.parser.InputParser;
import com.demograft.ldb4rdbconverter.parser.InputRecord;
import com.demograft.ldb4rdbconverter.parser.ParquetInputParser;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

@Slf4j
public class Ldb4RdbConverter {

    private File inputFile;
    private String outputFile;

    @Option(name = "--config-file", required = true, usage = "Configuration file to configure generator")
    private String configFile = "";

    private String longitude = "";
    private String latitude = "";
    private String time = "";
    private String statsFile = "stats.txt";

    private int files = 0;

    private String[] columnsToRemove = new String[0];

    private List<String> columnsToRemoveList = new ArrayList<>();

    private DateTimeFormatter timeFormatter;

    private boolean inputEpochInMilliseconds = false;

    private StringBuilder statistics = new StringBuilder();

    private final String[] propertyNames = new String[]{"input-file","output-file","stats-file","latitude","longitude","time","start-time","end-time","columns-to-map-long", "headers",
    "long-null-values", "double-null-values", "float-null-values", "long-columns", "float-columns", "double-columns", "string-columns", "time-columns", "parquet-size","excluded","unique-strings","timezone","headers"};

    private final Set<String> propertySet = new HashSet<>(Arrays.asList(propertyNames));

    private String[] headerArray;

    private List<String> examples;

    private List<String> headers = new ArrayList<>();

    private List<String> hashColumns = new ArrayList<>();

    private List<Float> float_null_values = new ArrayList<>();

    private List<Double> double_null_values = new ArrayList<>();

    private List<Long> long_null_values = new ArrayList<>();

    private List<String> string_null_values = new ArrayList<>();

    private long[] hashMapCounters;

    private List<String> long_columns = new ArrayList<>();

    private List<String> float_columns = new ArrayList<>();

    private List<String> double_columns = new ArrayList<>();

    private List<String> string_columns = new ArrayList<>();

    private List<String> time_columns = new ArrayList<>();

    private List<HashMap<String, Long>> hashTables = new ArrayList<>();

    private HashMap<String, Integer[]> statsTable = new HashMap<>();

    private HashMap<String, Float[]> minMaxTable = new HashMap<>();

    private HashMap<String, Set<String>> uniqueStrings = new HashMap<>();

    private int parquet_size = 0;

    private int uniqueMax = Integer.MAX_VALUE;

    private boolean predefinedHeaders = false;

    /* Statistics look as follows: 4 numbers for each row, they indicate:
        1. Number of non-null values
        2. Number of malformed values
        3. Number of NULL values
        4. Number of zero-values (0.0 for float, double; 0 for long)

        Minmax values table as follows:

        1. Min value of this column
        2. Max value of this column

       */

    private HashMap<String, Schema.Type> typeTable = new HashMap<>();

    private int totalRecords = 0;

    private int timeGated = 0;

    private int writtenRecords = 0;

    private String start_time = "";

    private String end_time = "";

    private long startTimeEpoch = 0L;

    private long endTimeEpoch = 0L;

    private Map<String, Integer> timeData = new TreeMap<>();

    private Map<String, List<String>> rowNulls = new HashMap<>();

    private final int COL_NON_NULL_VALUES = 0;

    private final int COL_MALFORMED_VALUES = 1;

    private final int COL_NULL_VALUES = 2;

    private final int COL_ZERO_VALUES = 3;

    private final int COL_MIN_VALUE = 0;

    private final int COL_MAX_VALUE = 1;

    private static String timeZone = "Z";

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

    /**
     * method tries to guess input date formatter.
     *
     * @param time     input time string
     * @param timeZone timezone for case where input time doesn't have an timeZone given.
     * @return DateTimeFormatter or null if the input time is an epoch.
     */
    static DateTimeFormatter identifyTimeFormat(String time, String timeZone) {
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
        } catch (DateTimeParseException e1) {

        }
        try {
            Long epoch = Long.parseLong(time);
            return null;
        } catch (NumberFormatException e1) {

        }
        try {
            DateTimeFormatter formatter
                    = DateTimeFormatter.ISO_LOCAL_DATE_TIME;


            LocalDateTime localdate = LocalDateTime.parse(time, formatter);
            ZonedDateTime date = ZonedDateTime.of(localdate, ZoneId.of(timeZone));
            return formatter;
        }
        catch(DateTimeParseException e3) {
            throw new RuntimeException("Error. Couldn't get DateTime parser from first example row. Does not match any common date time formats.");
        }
    }

    static long timeToMillisecondsConverter(String time, DateTimeFormatter format, boolean inputEpochInMilliseconds) throws DateTimeParseException {
        if (format != null) {
            try {
                ZonedDateTime date = ZonedDateTime.parse(time, format);
                return date.toInstant().toEpochMilli();
            } catch (DateTimeParseException e) {
                LocalDateTime localdate = LocalDateTime.parse(time, format);
                ZonedDateTime date = ZonedDateTime.of(localdate, ZoneId.of(timeZone));
                return date.toInstant().toEpochMilli();
            }
        } else {
            try {
                if (time.length() < 10) {
                    throw new DateTimeParseException("Couldn't parse input time to long", time, 0);
                }

                Long epoch = Long.parseLong(time);
                if (inputEpochInMilliseconds) {
                    return epoch;
                } else {
                    return epoch * 1000;
                }

            } catch (NumberFormatException e) {
                throw new DateTimeParseException("Couldn't parse input time to long", time, 0, e);
            }
        }
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

    private JSONObject determineType(String headername, String example){
        JSONObject jo = new JSONObject();
        try {
            long time = timeToMillisecondsConverter(example, timeFormatter,inputEpochInMilliseconds);
            jo.put("name", headername);
            JSONArray typelist = new JSONArray();
            typelist.add("null");
            typelist.add("long");
            jo.put("type", typelist);
            time_columns.add(headername);
        } catch (DateTimeParseException e1) {
            try {
                float data = Float.parseFloat(example);
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add("float");
                jo.put("type", typelist);
            } catch (NumberFormatException e2) {
                JSONArray typelist = new JSONArray();
                jo.put("name", headername);
                typelist.add("null");
                if (hashColumns.contains(headername)) {
                    typelist.add("long");
                } else {
                    typelist.add("string");
                }
                jo.put("type", typelist);
            }
        }

        // All undefinable data types are marked as strings.

        catch (NullPointerException e1){
            jo.put("name", headername);
            JSONArray typelist = new JSONArray();
            typelist.add("null");
            typelist.add("string");
            jo.put("type", typelist);
        }
        return jo;
    }

    private JSONObject createJSONFromCSVRecords(List<String> headerrow, List<String> examplerow) {
        JSONObject mainjson = new JSONObject();
        mainjson.put("name", "locationrecord");
        mainjson.put("type", "record");
        mainjson.put("namespace", "com.demograft.ldb4rdbconverter.generated");
        JSONArray list = new JSONArray();
        for (int i = 0; i < headerrow.size(); i++) {
            String headername = headerrow.get(i).trim();
            String example = examplerow.get(i);
            JSONObject jo = new JSONObject();

            // Check if it is one of the three important rows: longitude, latitude and time.

            if (headername.equals(longitude)) {
                jo.put("name", "lon");
                jo.put("type", "double");
            } else if (headername.equals(latitude)) {
                jo.put("name", "lat");
                jo.put("type", "double");
            } else if (headername.equals(time)) {
                jo.put("name", "time");
                jo.put("type", "long");
            }

            // Then check if it is from predefined columns

            else if (long_columns.contains(headername) || time_columns.contains(headername)) {
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add("long");
                jo.put("type", typelist);
            } else if (float_columns.contains(headername)) {
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add("float");
                jo.put("type", typelist);
            } else if (double_columns.contains(headername)) {
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add("double");
                jo.put("type", typelist);
            } else if (string_columns.contains(headername)) {
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add("string");
                jo.put("type", typelist);
            }

            // Then try to determine column type

            else {
                jo = determineType(headername, example);
            }

            if (!columnsToRemoveList.contains(headername)) {  // If not manually excluded
                    list.add(jo);
            }
        }
        mainjson.put("fields", list);
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
                if(time_columns.contains(field.name())){
                    row.put("guiType", "dateTime");
                }
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
            log.info("Error writing the attributeTranslation file. Error message: " + e.getMessage());
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

    private Float checkForNullFloat(Float value, String headerName){
        if(!rowNulls.containsKey(headerName)){
            return value;
        }
        else{
            List<String> comparables = rowNulls.get(headerName);
            for(String comparable:comparables){
                if(Float.parseFloat(comparable) == value){
                    return null;
                }
            }
        }
            return value;
    }

    private Double checkForNullDouble(Double value, String headerName){
        if(!rowNulls.containsKey(headerName)){
            return value;
        }
        else{
            List<String> comparables = rowNulls.get(headerName);
            for(String comparable:comparables){
                if(Double.parseDouble(comparable) == value){
                    return null;
                }
            }
        }
        return value;
    }

    private Long checkForNullLong(Long value, String headerName){
        if(!rowNulls.containsKey(headerName)){
            return value;
        }
        else{
            List<String> comparables = rowNulls.get(headerName);
            for(String comparable:comparables){
                if(Long.parseLong(comparable) == value){
                    return null;
                }
            }
        }
        return value;
    }

    private String checkForNullString(String value, String headerName){
        if(!rowNulls.containsKey(headerName)){
            return value;
        }
        else{
            List<String> comparables = rowNulls.get(headerName);
            for(String comparable:comparables){
                if(comparable.equals(value)){
                    return null;
                }
            }
        }
        return value;
    }

    private void checkValidity(List<String> headers){
        for(String configValue: rowNulls.keySet()){
            if(!headers.contains(configValue)){
                log.error("Configuration file contains a property (" + configValue + ") that isn't a configuration property or a header in the data.");
                throw new RuntimeException("Configuration file contains a property (" + configValue + ") that isn't a configuration property or a header in the data.");
            }
        }
    }

    private void readConfigFile(){
        Properties defaultProp = new Properties();

        try(FileReader fileReader = new FileReader(configFile)){
            defaultProp.load(fileReader);
        } catch (IOException e) {
            log.info("Error reading the configuration file, make sure your configuration file is named correctly.");
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
        if(defaultProp.containsKey("excluded")) {
            String propInfo = defaultProp.getProperty("excluded");
            columnsToRemove = propInfo.split(",");
            columnsToRemoveList = new ArrayList<>(Arrays.asList(columnsToRemove));
        }
        if(defaultProp.containsKey("parquet-size")){
            parquet_size = Integer.parseInt(defaultProp.getProperty("parquet-size"));
        }

        if(defaultProp.containsKey("columns-to-map-long")) {
            String propInfo = defaultProp.getProperty("columns-to-map-long");
            for (String column: propInfo.split(",")) {
                hashColumns.add(column.trim());
            }
        }
        if(defaultProp.containsKey("time-columns")) {
            String propInfo = defaultProp.getProperty("time-columns");
            for (String column: propInfo.split(",")) {
                time_columns.add(column.trim());
            }
        }
        if(defaultProp.containsKey("long-null-values")) {
            String propInfo = defaultProp.getProperty("long-null-values");
            for (String column: propInfo.split(",")) {
                long_null_values.add(Long.parseLong(column.trim()));
            }
        }
        if(defaultProp.containsKey("double-null-values")) {
            String propInfo = defaultProp.getProperty("double-null-values");
            for (String column: propInfo.split(",")) {
                double_null_values.add(Double.parseDouble(column.trim()));
            }
        }
        if(defaultProp.containsKey("float-null-values")) {
            String propInfo = defaultProp.getProperty("float-null-values");
            for (String column: propInfo.split(",")) {
                float_null_values.add(Float.parseFloat(column.trim()));
            }
        }
        if(defaultProp.containsKey("unique-strings")) {
            String propInfo = defaultProp.getProperty("unique-strings");
            uniqueMax = Integer.parseInt(propInfo);
        }
        if(defaultProp.containsKey("string-null-values")) {
            String propInfo = defaultProp.getProperty("string-null-values");
            for (String column: propInfo.split(",")) {
                string_null_values.add(column);
            }
        }
        if(defaultProp.containsKey("long-columns")) {
            String propInfo = defaultProp.getProperty("long-columns");
            for (String column: propInfo.split(",")) {
                long_columns.add(column.trim());
            }
        }
        if(defaultProp.containsKey("float-columns")) {
            String propInfo = defaultProp.getProperty("float-columns");
            for (String column: propInfo.split(",")) {
                float_columns.add(column.trim());
            }
        }
        if(defaultProp.containsKey("double-columns")) {
            String propInfo = defaultProp.getProperty("double-columns");
            for (String column: propInfo.split(",")) {
                double_columns.add(column.trim());
            }
        }
        if(defaultProp.containsKey("string-columns")) {
            String propInfo = defaultProp.getProperty("string-columns");
            for (String column: propInfo.split(",")) {
                string_columns.add(column.trim());
            }
        }
        if(defaultProp.containsKey("headers")){
            String headerInfo = defaultProp.getProperty("headers").trim();
            predefinedHeaders = true;
            String[] headerArray = headerInfo.split(",");
            this.headerArray = headerArray;
        }

        if(defaultProp.containsKey("stats-file")) {
            statsFile = defaultProp.getProperty("stats-file");
        }
        if(defaultProp.containsKey("start-time")) {
            start_time = defaultProp.getProperty("start-time");
        }
        if(defaultProp.containsKey("end-time")) {
            end_time = defaultProp.getProperty("end-time");
        }
        if(defaultProp.containsKey("timezone")){
            timeZone = defaultProp.getProperty("timezone");
        }
        for(String key: defaultProp.stringPropertyNames()){
            if(!propertySet.contains(key)){
                String propInfo = defaultProp.getProperty(key);
                List<String> nulls = new ArrayList<>();
                String[] values = propInfo.split(",");
                for(String value: values){
                    nulls.add(value);
                }
                rowNulls.put(key, nulls);
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

    private GenericData.Record convertToParquetRecord(Schema schema, InputRecord record) {
        boolean faulty = false;
        GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
        for (Schema.Field field : schema.getFields()) {
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
                                Long timeVal = timeToMillisecondsConverter(record.getString(time), timeFormatter,inputEpochInMilliseconds);
                                if(rowNulls.containsKey(field.name())){
                                    if(checkForNullLong(timeVal, field.name()) == null){
                                        throw new NullPointerException();
                                    }
                                }
                                else{
                                    if(long_null_values.contains(timeVal)){
                                        throw new NullPointerException();
                                    }
                                }
                                if(startTimeEpoch != 0L && timeVal < startTimeEpoch){
                                    timeGated += 1;
                                    faulty = true;
                                }
                                if(endTimeEpoch != 0L && timeVal > endTimeEpoch){
                                    timeGated += 1;
                                    faulty = true;
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
                                faulty = true;
                                break;
                            }
                            catch(NullPointerException e1){
                                Integer[] stat = statsTable.get("time");
                                stat[COL_NULL_VALUES] += 1;
                                statsTable.put("time", stat);
                                faulty = true;
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
                                    Long date = timeToMillisecondsConverter(record.getString(field.name()), timeFormatter,inputEpochInMilliseconds);
                                    if(rowNulls.containsKey(field.name())){
                                        if(checkForNullLong(date, field.name()) == null){
                                            throw new NullPointerException();
                                        }
                                    }
                                    else{
                                        if(long_null_values.contains(date)){
                                            throw new NullPointerException();
                                        }
                                    }
                                    genericRecordBuilder = genericRecordBuilder.set(field, date);
                                    Integer[] stat = statsTable.get(field.name());
                                    stat[COL_NON_NULL_VALUES] += 1;
                                    statsTable.put(field.name(), stat);
                                } catch (DateTimeParseException e) {
                                    try {
                                        Long number = record.getLong(field.name());
                                        if(rowNulls.containsKey(field.name())){
                                            if(checkForNullLong(number, field.name()) == null){
                                                throw new NullPointerException();
                                            }
                                        }
                                        else{
                                            if(long_null_values.contains(number)){
                                                throw new NullPointerException();
                                            }
                                        }
                                        Integer[] stat = statsTable.get(field.name());
                                        stat[COL_NON_NULL_VALUES] += 1;
                                        if (number == null || number == 0L) {
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
                                if(rowNulls.containsKey(field.name())){
                                    if(checkForNullDouble(number, field.name()) == null){
                                        throw new NullPointerException();
                                    }
                                }
                                else{
                                    if(double_null_values.contains(number)){
                                        throw new NullPointerException();
                                    }
                                }
                                if(number > 180 || number < -180){
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
                                faulty = true;
                                break;
                            }
                            catch(NullPointerException e1){
                                Integer[] stat = statsTable.get("lon");
                                stat[COL_NULL_VALUES] += 1;
                                statsTable.put("lon", stat);
                                faulty = true;
                                break;
                            }
                        }
                        else if(field.name().equals("lat")){
                                try{
                                    if(record.getString(latitude).equals("")){
                                        throw new NullPointerException();
                                    }
                                    Double number = record.getDouble(latitude);
                                    if(rowNulls.containsKey(field.name())){
                                        if(checkForNullDouble(number, field.name()) == null){
                                            throw new NullPointerException();
                                        }
                                    }
                                    else{
                                        if(double_null_values.contains(number)){
                                            throw new NullPointerException();
                                        }
                                    }
                                    if(number > 90 || number < -90){
                                        throw new NullPointerException();
                                    }
                                    genericRecordBuilder = genericRecordBuilder.set("lat", number);
                                    Integer[] stat2 = statsTable.get("lat");
                                    stat2[COL_NON_NULL_VALUES] += 1;
                                    if(number == 0.000000D){
                                        stat2[COL_ZERO_VALUES] += 1;
                                    }
                                    statsTable.put("lat", stat2);
                                }
                                catch(NumberFormatException e){
                                    Integer[] stat2 = statsTable.get("lat");
                                    stat2[COL_NON_NULL_VALUES] += 1;
                                    stat2[COL_MALFORMED_VALUES] += 1;
                                    statsTable.put("lat", stat2);
                                    faulty = true;
                                    break;
                                }
                                catch(NullPointerException e1){
                                    Integer[] stat = statsTable.get("lat");
                                    stat[COL_NULL_VALUES] += 1;
                                    statsTable.put("lat", stat);
                                    faulty = true;
                                    break;
                                }
                        }
                        else {
                            try {
                                Double number = record.getDouble(field.name());
                                if(rowNulls.containsKey(field.name())){
                                    if(checkForNullDouble(number, field.name()) == null){
                                        throw new NullPointerException();
                                    }
                                }
                                else{
                                    if(double_null_values.contains(number)){
                                        throw new NullPointerException();
                                    }
                                }
                                genericRecordBuilder = genericRecordBuilder.set(field, number);
                                Integer[] stat = statsTable.get(field.name());
                                stat[COL_NON_NULL_VALUES] += 1;
                                if(number == 0.000000D){
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
                            catch(NullPointerException e1){
                                Integer[] stat = statsTable.get(field.name());
                                stat[COL_NULL_VALUES] += 1;
                                statsTable.put(field.name(), stat);
                                break;
                            }
                        }
                        break;
                    case FLOAT:
                        try{
                            Float foundValue = record.getFloat(field.name());
                            // Max values are sometimes used to indicate missing data.
                            if(rowNulls.containsKey(field.name())){
                                if(checkForNullFloat(foundValue, field.name()) == null){
                                    throw new NullPointerException();
                                }
                            }
                            else{
                                if(float_null_values.contains(foundValue)){
                                    throw new NullPointerException();
                                }
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
                            try {
                                String answer = record.getString(field.name());
                                if (rowNulls.containsKey(field.name())) {
                                    if (checkForNullString(answer, field.name()) == null) {
                                        throw new NullPointerException();
                                    }
                                } else {
                                    if (string_null_values.contains(answer)) {
                                        throw new NullPointerException();
                                    }
                                }
                                Set<String> set = uniqueStrings.get(field.name());
                                set.add(answer);
                                uniqueStrings.put(field.name(), set);
                                genericRecordBuilder = genericRecordBuilder.set(field, answer);
                                Integer[] stat = statsTable.get(field.name());
                                stat[COL_NON_NULL_VALUES] += 1;
                                statsTable.put(field.name(), stat);

                            }
                            catch(NullPointerException e1){
                                Integer[] stat = statsTable.get(field.name());
                                stat[COL_NULL_VALUES] += 1;
                                statsTable.put(field.name(), stat);
                                genericRecordBuilder = genericRecordBuilder.set(field, null);
                            }
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
            if(!faulty) {
                totalRecords += 1;
                writtenRecords += 1;
                LocalDate localdate;
                if (timeFormatter != null) {
                ZonedDateTime date;
                try {
                    date = ZonedDateTime.parse(record.getString(time), timeFormatter);
                }
                catch(DateTimeParseException e){
                    LocalDateTime localDate = LocalDateTime.parse(record.getString(time), timeFormatter);
                    date = localDate.atZone(ZoneId.of(timeZone));
                }
                localdate = date.toLocalDate();
                } else {
                    long epoch = Long.parseLong(record.getString(time));
                    if (inputEpochInMilliseconds) {
                        Instant instant = Instant.ofEpochMilli(epoch);
                        ZonedDateTime date = ZonedDateTime.ofInstant(instant, ZoneId.of(timeZone));
                        localdate = date.toLocalDate();
                    } else {
                        Instant instant = Instant.ofEpochSecond(epoch);
                        ZonedDateTime date = ZonedDateTime.ofInstant(instant, ZoneId.of(timeZone));
                        localdate = date.toLocalDate();
                    }
                }
                if(timeData.containsKey(localdate.toString())){
                    Integer count = timeData.get(localdate.toString());
                    count += 1;
                    timeData.put(localdate.toString(), count);
                }
                else{
                    timeData.put(localdate.toString(), 1);
                }
                return genericRecordBuilder.build();
            }
            else{
                totalRecords += 1;
                return null;
            }
    }

    private void writeStatsFile(){
        statistics.append("Parquet file generation successful. \n \n \nGeneral statistics: \nFound a total of " + totalRecords + " records. ");
        statistics.append(writtenRecords + " records from " + files + " files parsed into the parquet file. " + (totalRecords - writtenRecords) + " records contained malformed lat, lon or time fields. " + timeGated + " records discarded due to time restrictions.\n");
        statistics.append("Time restrictions set:    \nStart time:  " + start_time + "    \nEnd time:  " + end_time + ". \n\n");
        statistics.append("Field statistics in the form of: field name, non-null values, null values :  \n\n");
        for(String key: uniqueStrings.keySet()){
            if(uniqueStrings.get(key).size() > uniqueMax){
                statistics.append("WARNING!!! Field " + key + " exceeded maximum allowed unique Classifiers. \n");
            }
        }
        for(String key: statsTable.keySet()){
            statistics.append(key + " , non-null: " + statsTable.get(key)[0] + " , invalid: " + statsTable.get(key)[1] + " , null: " + statsTable.get(key)[2] + " , zero-values: " + statsTable.get(key)[3] + " , type: " + typeToString(typeTable.get(key)) +  "\n");
            if(minMaxTable.containsKey(key)){
                statistics.append("     Min: " + minMaxTable.get(key)[0] + "       Max: " + minMaxTable.get(key)[1] + "\n\n");
            }
            if(uniqueStrings.containsKey(key)){
                statistics.append("     Unique string count: " + uniqueStrings.get(key).size() + "\n\n");
            }
        }
        statistics.append("Time data.\nDate followed by the number of samples found from that date\n\n\n");
        for(String key: timeData.keySet()){
            statistics.append(key + "   -   " + timeData.get(key) + "\n");
        }
        try(FileWriter fw = new FileWriter(statsFile)){
            fw.write(statistics.toString());
        }
        catch(IOException e){
            log.info("Error writing the statistics file. Error message: " + e.getMessage());
        }
    }

    private String newFileName(Integer number){
        if(outputFile.contains(".")){
            String[] pieces = outputFile.split("[.]");
            return pieces[0] + "_" + number + ".parquet";
        }
        else{
            return outputFile + "_" + number + ".parquet";
        }
    }

    private void run() {

        if(configFile.equals("")){
            throw new RuntimeException("Configuration file not set, cannot proceed.");
        }
        readConfigFile();
        List<File> csvFiles = new ArrayList<>();
        List<File> parquetFiles = new ArrayList<>();

        if (!inputFile.isDirectory()) {
            String fileName = inputFile.getName();
            if (fileName.endsWith(".csv")) {
                csvFiles.add(inputFile);
            } else if (fileName.endsWith(".parquet")) {
                parquetFiles.add(inputFile);
            } else {
                throw new RuntimeException(String.format("File {} has an unknown extension", fileName));
            }
        } else {
            try {
                for (File file : inputFile.listFiles()) {
                    String fileName = file.getName();
                    if (fileName.endsWith(".csv")) {
                        csvFiles.add(file);
                    } else if (fileName.endsWith(".parquet")) {
                        parquetFiles.add(file);
                    }
                }
            }
            catch(NullPointerException e) {
                log.info("Error finding the file/directory, please recheck the file name and make sure this file or directory exists.");
            }
        }
        if (csvFiles.size() > 0 && parquetFiles.size() > 0) {
            throw new RuntimeException("Input data can'be be in CSV and Parquet files at the same time. Input directory should contain only one of these types.");
        }
        InputParser parser;
        File exampleFile;
        List<File> inputFiles;
        if (csvFiles.size() > 0) {
            if(predefinedHeaders){
                parser = new CsvInputParser(headerArray);
                headers = Arrays.asList(headerArray);
            }
            else{
                parser = new CsvInputParser();
            }
            exampleFile = csvFiles.get(0);
            inputFiles = csvFiles;
        } else if (parquetFiles.size() > 0) {
            parser = new ParquetInputParser();
            exampleFile = parquetFiles.get(0);
            inputFiles = parquetFiles;
        } else {
            throw new RuntimeException(String.format("Input file {} doesn't have a valid input type", inputFile));
        }
        parser.beginParsing(exampleFile);
        if(!predefinedHeaders){
            InputRecord headerrow = parser.parseNextRecord();
            headerArray = headerrow.getValues();
            headers = Arrays.asList(headerArray);
        }
        InputRecord exampleRow = parser.parseNextRecord();
        String[] exampleArray = exampleRow.getValues();
        examples = Arrays.asList(exampleArray);

        parser.stopParsing();

        //Remove excluded rows and their examples
        for(String row: headers){
            if (columnsToRemoveList.contains(row)){
                headers.remove(row);
                examples.remove(headers.indexOf(row));
            }
        }

        String timeString = exampleRow.getString(time);
        timeFormatter = identifyTimeFormat(timeString, timeZone);
        if (timeFormatter == null && timeString.length() > 10) {
            inputEpochInMilliseconds = true;
        }
        JSONObject mainjson = createJSONFromCSVRecords(headers, examples);
        hashMapCounters = new long[hashColumns.size()];
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
            if (getSchemaType(field.schema()) == Schema.Type.STRING){
                uniqueStrings.put(field.name(), new HashSet<>());
            }
        }
        if(!start_time.equals("")) {
            startTimeEpoch = timeToMillisecondsConverter(start_time, timeFormatter,inputEpochInMilliseconds);
        }
        if(!end_time.equals("")){
            endTimeEpoch = timeToMillisecondsConverter(end_time, timeFormatter,inputEpochInMilliseconds);
        }

        /* CSV -> JSON -> Schema -> Record -> Parquetfile
                            |
                            |
                  attributeTranslation.json

        */

        int blockSize = 1024 * 1024 ;
        int pageSize = 1024 * 1024 * 16;

        int filenumber = 1;
        int written = 0;
        ParquetWriter<GenericData.Record> parquetWriter = null;

        try {
            parquetWriter = AvroParquetWriter
                    .<GenericData.Record>builder(new Path(newFileName(filenumber)))
                    .withSchema(avroSchema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withRowGroupSize(blockSize)
                    .withPageSize(pageSize)
                    .build();
            for (File file : inputFiles) {
                parser.beginParsing(file);
                InputRecord record;
                try {
                    while ((record = parser.parseNextRecord()) != null) {
                        GenericData.Record toWrite = convertToParquetRecord(avroSchema, record);
                        if (toWrite != null) {
                            if(parquet_size > 0 && written >= parquet_size){
                                written = 0;
                                parquetWriter.close();
                                filenumber += 1;
                                parquetWriter = AvroParquetWriter
                                        .<GenericData.Record>builder(new Path(newFileName(filenumber)))
                                        .withSchema(avroSchema)
                                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                                        .withRowGroupSize(blockSize)
                                        .withPageSize(pageSize)
                                        .build();
                            }
                            parquetWriter.write(toWrite);
                            written += 1;
                        }
                    }
                }
                catch(NullPointerException e){
                    log.info("Error. Encountered an empty file or a file with only a header row. File name is: " + file.getName());
                }
                finally{
                    files += 1;
                    parser.stopParsing();
                }
            }
        }catch(java.io.IOException e){
            log.info(String.format("Error writing parquet file %s", e.getMessage()));
        }finally{
            if(parquetWriter != null) {
                try {
                    parquetWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        writeStatsFile();
    }
}