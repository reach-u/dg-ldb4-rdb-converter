package com.demograft.ldb4rdbconverter;

import com.demograft.ldb4rdbconverter.parser.CsvInputParser;
import com.demograft.ldb4rdbconverter.parser.InputParser;
import com.demograft.ldb4rdbconverter.parser.InputRecord;
import com.demograft.ldb4rdbconverter.parser.ParquetInputParser;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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
import java.text.DecimalFormat;
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

    private final String[] propertyNames = new String[]{"input-file", "output-file", "stats-file", "latitude", "longitude",
            "time", "start-time", "end-time", "columns-to-map-long", "headers", "long-null-values", "double-null-values",
            "float-null-values", "long-columns", "float-columns", "double-columns", "string-columns", "time-columns",
            "parquet-size", "excluded", "unique-strings", "timezone","headers","retain-hashes","trajectoryID","tokenFiles"};

    private final Set<String> propertySet = new HashSet<>(Arrays.asList(propertyNames));

    private String[] headerArray;

    private String trajectory = "";


    private List<String> examples = new LinkedList<>();

    private List<String> headers = new LinkedList<>();

    private List<String> hashColumns = new ArrayList<>();

    private List<Float> floatNullValues = new ArrayList<>();

    private List<Double> doubleNullValues = new ArrayList<>();

    private List<Long> longNullValues = new ArrayList<>();

    private List<String> stringNullValues = new ArrayList<>();

    private long[] hashMapCounters;

    private List<String> longColumns = new ArrayList<>();

    private List<String> floatColumns = new ArrayList<>();

    private List<String> doubleColumns = new ArrayList<>();

    private List<String> stringColumns = new ArrayList<>();

    private List<String> tokenFiles = new LinkedList<>();

    private Map<String, String> tokenHeaders = new HashMap<>();           //    They are formed as follows:   tokenHeader -> originalHeader

    private Map<String, Map<String,String>> tokenMaps = new HashMap<>();   //    They are formed as follows:  tokenHeader -> {originalRow -> tokenRow}

    private Map<String, String> tokenTypes = new HashMap<>();

    private List<String> timeColumns = new ArrayList<>();

    private List<String> retainHashes = new LinkedList<>();

    private List<HashMap<String, Long>> hashTables = new ArrayList<>();

    private HashMap<String, Integer[]> statsTable = new HashMap<>();

    private HashMap<String, Float[]> minMaxTable = new HashMap<>();

    private HashMap<String, Set<String>> uniqueStrings = new HashMap<>();

    private int parquetSize = 0;

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

    private String startTime = "";

    private String endTime = "";

    private long startTimeEpoch = 0L;

    private long endTimeEpoch = 0L;

    private Map<String, Integer> timeData = new TreeMap<>();

    private Map<String, List<String>> rowNulls = new HashMap<>();

    private static final int COL_NON_NULL_VALUES = 0;

    private static final int COL_MALFORMED_VALUES = 1;

    private static final int COL_NULL_VALUES = 2;

    private static final int COL_ZERO_VALUES = 3;

    private static final int COL_MIN_VALUE = 0;

    private static final int COL_MAX_VALUE = 1;

    private static final String PARQUET_FILE_EXTENSION = ".parquet";

    private static String timeZone = "Z";

    private String formatName(String name){
        name = name.replace("_", " ");
        StringBuilder newName = new StringBuilder();
        for (int i = 0; i < name.length() - 2; i++) {
            if (Character.isLowerCase(name.charAt(i)) && Character.isUpperCase(name.charAt(i + 1)) && Character.isLowerCase(name.charAt(i + 2))) {
                newName.append(name.charAt(i));
                newName.append(" ");
            } else {
                newName.append(name.charAt(i));
            }
        }
        newName.append(name.charAt(name.length() - 2));
        newName.append(name.charAt(name.length() - 1));
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

    private Schema.Type getSchemaType(Schema subschema, String field) {
        if (subschema.getType() == Schema.Type.UNION) {
            if (field == null) {
                return subschema.getTypes().get(0).getType();
            } else {
                return subschema.getTypes().get(1).getType();
            }
        } else {
            return subschema.getType();
        }
    }

    private Schema.Type getSchemaType(Schema subschema) {
        if (subschema.getType() == Schema.Type.UNION) {
            return subschema.getTypes().get(1).getType();
        } else {
            return subschema.getType();
        }
    }

    private JSONObject determineType(String headername, String example) {
        JSONObject jo = new JSONObject();

        //Make all rows that get created with retainHashes as longs.

        if (headername.endsWith("_IDs")){
            jo.put("name", headername);
            JSONArray typelist = new JSONArray();
            typelist.add("null");
            typelist.add("long");
            jo.put("type", typelist);
        }
        else {

            // Try to determine type

            try {
                TimeUtils.timeToMillisecondsConverter(example, timeFormatter, inputEpochInMilliseconds, timeZone);
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add("long");
                jo.put("type", typelist);
                timeColumns.add(headername);
            } catch (DateTimeParseException e1) {
                try {
                    Float.parseFloat(example);
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

            catch (NullPointerException e1) {
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add("string");
                jo.put("type", typelist);
            }
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

            else if (longColumns.contains(headername) || timeColumns.contains(headername)) {
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add("long");
                jo.put("type", typelist);

            } else if (floatColumns.contains(headername)) {
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add("float");
                jo.put("type", typelist);

            } else if (doubleColumns.contains(headername)) {
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add("double");
                jo.put("type", typelist);

            } else if (stringColumns.contains(headername)) {
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add("string");
                jo.put("type", typelist);

            } else if (tokenTypes.keySet().contains(headername)){
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add(tokenTypes.get(headername));
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

    private void writeAttributeFile(Schema schema, String fileName) {
        JSONObject attributeTr = new JSONObject();
        JSONArray data = new JSONArray();
        for (Schema.Field field : schema.getFields()) {
            JSONObject row = new JSONObject();
            row.put("hidden", false);
            String formattedName = formatName(field.name());
            if (field.name().equals("lat")) {
                row.put("attributeName", "Latitude");
                row.put("attributeTooltip", "Refined location of the record, used by heatmapping and optionally by trajectory visualisation function" +
                        "\nGeographic WGS84 coordinate in decimal degrees." +
                        "\nIn the current version equals to " + latitude);
                row.put("group", "Geographic Location");
                row.put("unit", "degrees");
                row.put("attributeId", field.name());
                data.add(row);
            } else if (field.name().equals("lon")) {
                row.put("attributeName", "Longitude");
                row.put("attributeTooltip", "Refined location of the record, used by heatmapping and optionally by trajectory visualisation function" +
                        "\\nGeographic WGS84 coordinate in decimal degrees." +
                        "\\nIn the current version equals to " + longitude);
                row.put("group", "Geographic Location");
                row.put("unit", "degrees");
                row.put("attributeId", field.name());
                data.add(row);
            } else if (getSchemaType(field.schema()) == Schema.Type.LONG && field.name().equals("time")) {

                // guiType:dateTime

                row.put("attributeName", "Time");
                row.put("attributeTooltip", formattedName);
                row.put("group", "Generic");
                row.put("attributeId", field.name());
                row.put("guiType", "dateTime");
                data.add(row);
            } else if (getSchemaType(field.schema()) == Schema.Type.LONG) {
                row.put("attributeName", formattedName);
                row.put("attributeTooltip", formattedName);
                row.put("group", "Generic");
                row.put("attributeId", field.name());
                if (timeColumns.contains(field.name())) {
                    row.put("guiType", "dateTime");
                }
                data.add(row);
            } else {
                row.put("attributeName", formattedName);
                row.put("attributeTooltip", formattedName);
                row.put("group", "Generic");
                row.put("attributeId", field.name());
                data.add(row);
            }
        }
        if(!trajectory.equals("")){
            JSONObject traj = new JSONObject();
            traj.put("hidden", true);
            traj.put("attributeName", "Trajectory");
            traj.put("idField", trajectory);
            traj.put("lonAttributeId","lon");
            traj.put("latAttributeId","lat");
            traj.put("guiType","trajectory");
            traj.put("group","trajectories");
            JSONArray infoList = new JSONArray();
            traj.put("infoAttributes",infoList);
            traj.put("attributeId","trajectory_1");
            data.add(traj);
        }
        attributeTr.put("data", data);

        try (FileWriter fw = new FileWriter(fileName)) {
            fw.write(attributeTr.toJSONString());
        } catch (IOException e) {
            log.info("Error writing the attributeTranslation file. Error message: " + e.getMessage());
        }
    }

    private String typeToString(Schema.Type type) {
        if (type == Schema.Type.LONG) {
            return "long";
        }
        if (type == Schema.Type.FLOAT) {
            return "float";
        }
        if (type == Schema.Type.DOUBLE) {
            return "double";
        } else {
            return "string";
        }
    }

    private Float checkForNullFloat(Float value, String headerName) {
        if (!rowNulls.containsKey(headerName)) {
            return value;
        } else {
            List<String> comparables = rowNulls.get(headerName);
            for (String comparable : comparables) {
                if (Float.parseFloat(comparable) == value) {
                    return null;
                }
            }
        }
        return value;
    }

    private Double checkForNullDouble(Double value, String headerName) {
        if (!rowNulls.containsKey(headerName)) {
            return value;
        } else {
            List<String> comparables = rowNulls.get(headerName);
            for (String comparable : comparables) {
                if (Double.parseDouble(comparable) == value) {
                    return null;
                }
            }
        }
        return value;
    }

    private Long checkForNullLong(Long value, String headerName) {
        if (!rowNulls.containsKey(headerName)) {
            return value;
        } else {
            List<String> comparables = rowNulls.get(headerName);
            for (String comparable : comparables) {
                if (Long.parseLong(comparable) == value) {
                    return null;
                }
            }
        }
        return value;
    }

    private String checkForNullString(String value, String headerName) {
        if (!rowNulls.containsKey(headerName)) {
            return value;
        } else {
            List<String> comparables = rowNulls.get(headerName);
            for (String comparable : comparables) {
                if (comparable.equals(value)) {
                    return null;
                }
            }
        }
        return value;
    }

    private void createTokenMaps(List<String> tokenFiles){
        for(String file: tokenFiles){
            File tokenFile = new File(file);
            CsvParserSettings settings = new CsvParserSettings();
            settings.detectFormatAutomatically();
            CsvParser parser = new CsvParser(settings);

            parser.beginParsing(tokenFile);
            String[] headers = parser.parseNext();
            tokenHeaders.put(headers[1], headers[0]);
            tokenTypes.put(headers[1], headers[2]);
            Map<String, String> tokenMap = new HashMap<>();
            String[] record;
            int i = 0;
            while ((record = parser.parseNext()) != null) {
                tokenMap.put(record[0].toLowerCase(), record[1]);
                i++;
            }
            tokenMaps.put(headers[1], tokenMap);
            parser.stopParsing();
        }
    }


    private void checkValidity(List<String> headers) {
        for (String configValue : rowNulls.keySet()) {
            if (!headers.contains(configValue)) {
                log.error("Configuration file contains a property (" + configValue + ") that isn't a configuration property or a header in the data.");
                throw new RuntimeException("Configuration file contains a property (" + configValue + ") that isn't a configuration property or a header in the data.");
            }
        }
    }

    private void readConfigFile() {
        Properties defaultProp = new Properties();

        try (FileReader fileReader = new FileReader(configFile)) {
            defaultProp.load(fileReader);
        } catch (IOException e) {
            log.info("Error reading the configuration file, make sure your configuration file is named correctly.");
        }
        if (!defaultProp.containsKey("input-file")) {
            throw new RuntimeException("Missing input-file property in configuration file");
        }
        inputFile = new File(defaultProp.getProperty("input-file"));
        if (!defaultProp.containsKey("output-file")) {
            throw new RuntimeException("Missing output-file property in configuration file");
        }
        outputFile = defaultProp.getProperty("output-file");
        if (!defaultProp.containsKey("longitude")) {
            throw new RuntimeException("Missing longitude property in configuration file");
        }
        longitude = defaultProp.getProperty("longitude");
        if (!defaultProp.containsKey("latitude")) {
            throw new RuntimeException("Missing latitude property in configuration file");
        }
        latitude = defaultProp.getProperty("latitude");
        if (!defaultProp.containsKey("time")) {
            throw new RuntimeException("Missing time property in configuration file");
        }
        time = defaultProp.getProperty("time");
        if (defaultProp.containsKey("excluded")) {
            String propInfo = defaultProp.getProperty("excluded");
            columnsToRemove = propInfo.split(",");
            columnsToRemoveList = new ArrayList<>(Arrays.asList(columnsToRemove));
        }
        if (defaultProp.containsKey("parquet-size")) {
            parquetSize = Integer.parseInt(defaultProp.getProperty("parquet-size"));
        }

        if(defaultProp.containsKey("columns-to-map-long")) {
            String propInfo = defaultProp.getProperty("columns-to-map-long");
            for (String column : propInfo.split(",")) {
                hashColumns.add(column.trim());
            }
        }
        if (defaultProp.containsKey("time-columns")) {
            String propInfo = defaultProp.getProperty("time-columns");
            for (String column : propInfo.split(",")) {
                timeColumns.add(column.trim());
            }
        }
        if (defaultProp.containsKey("long-null-values")) {
            String propInfo = defaultProp.getProperty("long-null-values");
            for (String column : propInfo.split(",")) {
                longNullValues.add(Long.parseLong(column.trim()));
            }
        }
        if (defaultProp.containsKey("double-null-values")) {
            String propInfo = defaultProp.getProperty("double-null-values");
            for (String column : propInfo.split(",")) {
                doubleNullValues.add(Double.parseDouble(column.trim()));
            }
        }
        if (defaultProp.containsKey("float-null-values")) {
            String propInfo = defaultProp.getProperty("float-null-values");
            for (String column : propInfo.split(",")) {
                floatNullValues.add(Float.parseFloat(column.trim()));
            }
        }
        if (defaultProp.containsKey("unique-strings")) {
            String propInfo = defaultProp.getProperty("unique-strings");
            uniqueMax = Integer.parseInt(propInfo);
        }
        if (defaultProp.containsKey("string-null-values")) {
            String propInfo = defaultProp.getProperty("string-null-values");
            for (String column : propInfo.split(",")) {
                stringNullValues.add(column);
            }
        }
        if (defaultProp.containsKey("long-columns")) {
            String propInfo = defaultProp.getProperty("long-columns");
            for (String column : propInfo.split(",")) {
                longColumns.add(column.trim());
            }
        }
        if (defaultProp.containsKey("float-columns")) {
            String propInfo = defaultProp.getProperty("float-columns");
            for (String column : propInfo.split(",")) {
                floatColumns.add(column.trim());
            }
        }
        if (defaultProp.containsKey("double-columns")) {
            String propInfo = defaultProp.getProperty("double-columns");
            for (String column : propInfo.split(",")) {
                doubleColumns.add(column.trim());
            }
        }
        if (defaultProp.containsKey("tokenFiles")) {
            String propInfo = defaultProp.getProperty("tokenFiles");
            for (String column : propInfo.split(",")){
                tokenFiles.add(column.trim());
            }
        }
        if (defaultProp.containsKey("string-columns")) {
            String propInfo = defaultProp.getProperty("string-columns");
            for (String column : propInfo.split(",")) {
                stringColumns.add(column.trim());
            }
        }
        if (defaultProp.containsKey("headers")) {
            String headerInfo = defaultProp.getProperty("headers").trim();
            predefinedHeaders = true;
            String[] headerArray = headerInfo.split(",");
            this.headerArray = headerArray;
        }

        if (defaultProp.containsKey("retain-hashes")) {
            String headerInfo = defaultProp.getProperty("retain-hashes").trim();
            String[] retainHashesArray = headerInfo.split(",");
            this.retainHashes = new LinkedList<>(Arrays.asList(retainHashesArray));
        }

        if (defaultProp.containsKey("stats-file")) {
            statsFile = defaultProp.getProperty("stats-file");
        }
        if (defaultProp.containsKey("start-time")) {
            startTime = defaultProp.getProperty("start-time");
        }
        if (defaultProp.containsKey("end-time")) {
            endTime = defaultProp.getProperty("end-time");
        }
        if (defaultProp.containsKey("timezone")) {
            timeZone = defaultProp.getProperty("timezone");
        }
        if (defaultProp.containsKey("trajectoryID")) {
            trajectory = defaultProp.getProperty("trajectoryID");
        }

        for (String key : defaultProp.stringPropertyNames()) {
            if (!propertySet.contains(key)) {
                String propInfo = defaultProp.getProperty(key);
                String[] values = propInfo.split(",");
                List<String> nulls = new ArrayList<>(Arrays.asList(values));
                rowNulls.put(key, nulls);
            }
        }
    }

    private Long generateNewHash(String hash, Integer index) {
        if (hashTables.get(index).containsKey(hash)) {
            return hashTables.get(index).get(hash);
        }
        Long hashNumber = hashMapCounters[index] + 1;
        hashTables.get(index).put(hash, hashNumber);
        hashMapCounters[index] = hashNumber;
        return hashNumber;
    }

    private GenericData.Record convertToParquetRecord(Schema schema, InputRecord record) {
        FieldConversionResult fieldConversionResult = new FieldConversionResult(schema);
        for (Schema.Field field : schema.getFields()) {
            Schema subschema = field.schema();


            Schema.Type subSchemaType = getSchemaType(record, field, subschema);

            switch (subSchemaType) {
                case LONG:
                    // Long currently only used for dateTimes and hashes
                    convertLong(record, fieldConversionResult, field);
                    break;
                case DOUBLE:
                    convertDouble(record, fieldConversionResult, field);
                    break;
                case FLOAT:
                    convertFloat(record, fieldConversionResult, field);
                    break;
                case STRING:
                    convertString(record, fieldConversionResult, field);
                    break;
                case NULL:
                    //Union data types always have 2 types, first is null and the second is the needed type.
                    handleNullOrEmpty(fieldConversionResult, field);
                    break;
                default:
                    // TODO: Should we log a warning or throw an exception here?
                    break;
            }
        }

        if (!fieldConversionResult.isFaulty()) {
            totalRecords += 1;
            writtenRecords += 1;
            LocalDate localdate;
            if (timeFormatter != null) {
                ZonedDateTime date;
                try {
                    date = ZonedDateTime.parse(record.getString(time), timeFormatter);
                } catch (DateTimeParseException e) {
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
            if (timeData.containsKey(localdate.toString())) {
                Integer count = timeData.get(localdate.toString());
                count += 1;
                timeData.put(localdate.toString(), count);
            } else {
                timeData.put(localdate.toString(), 1);
            }
            return fieldConversionResult.getGenericRecordBuilder().build();
        } else {
            totalRecords += 1;
            return null;
        }
    }

    private void handleNullOrEmpty(FieldConversionResult fieldConversionResult, Schema.Field field) {
        Integer[] stat2 = statsTable.get(field.name());
        stat2[COL_NULL_VALUES] += 1;
        statsTable.put(field.name(), stat2);
        fieldConversionResult.updateGenericRecordBuilder(field, null);
    }

    private void convertString(InputRecord record, FieldConversionResult fieldConversionResult, Schema.Field field) {
        if (record.getString(field.name()).equals("")) {
            handleNullOrEmpty(fieldConversionResult, field);
        } else {
            try {
                String answer;
                String fieldName = field.name();
                if(tokenHeaders.keySet().contains(fieldName)){
                    String preToken = record.getString(tokenHeaders.get(fieldName));
                    String nr = tokenMaps.get(fieldName).get(preToken.toLowerCase());
                    if (nr == null){
                        fieldConversionResult.updateGenericRecordBuilder(field, null);
                        throw new NullPointerException();
                    }
                    answer = nr;
                }
                else{
                    answer = record.getString(field.name());
                }
                checkFieldValidity(field, checkForNullString(answer, fieldName) == null, stringNullValues.contains(answer));
                Set<String> set = uniqueStrings.get(fieldName);
                set.add(answer);
                uniqueStrings.put(fieldName, set);
                fieldConversionResult.updateGenericRecordBuilder(field, answer);
                Integer[] stat = statsTable.get(fieldName);
                stat[COL_NON_NULL_VALUES] += 1;
                statsTable.put(fieldName, stat);

            } catch (NullPointerException e1) {
                handleNullOrEmpty(fieldConversionResult, field);
            }
        }
    }

    private void convertFloat(InputRecord record, FieldConversionResult fieldConversionResult, Schema.Field field) {
        try {
            Float foundValue;
            String fieldName = field.name();
            if(tokenHeaders.keySet().contains(field.name())){
                String preToken = record.getString(tokenHeaders.get(fieldName));
                String nr = tokenMaps.get(fieldName).get(preToken.toLowerCase());
                if (nr == null){
                    fieldConversionResult.updateGenericRecordBuilder(field, null);
                    throw new NullPointerException();
                }
                foundValue = Float.parseFloat(nr);
            } else {
                foundValue = record.getFloat(fieldName);
            }
            // Max values are sometimes used to indicate missing data.
            checkFieldValidity(field, checkForNullFloat(foundValue, fieldName) == null, floatNullValues.contains(foundValue));
            fieldConversionResult.updateGenericRecordBuilder(field, foundValue);
            Integer[] stat = statsTable.get(fieldName);
            Float[] minmax = minMaxTable.get(fieldName);
            stat[COL_NON_NULL_VALUES] += 1;
            if (foundValue == 0.0F) {
                stat[COL_ZERO_VALUES] += 1;
            }
            statsTable.put(field.name(), stat);
            if (minmax[COL_MIN_VALUE] > foundValue) {
                minmax[COL_MIN_VALUE] = foundValue;
            }
            if (minmax[COL_MAX_VALUE] < foundValue) {
                minmax[COL_MAX_VALUE] = foundValue;
            }
            minMaxTable.put(field.name(), minmax);
        } catch (NumberFormatException e1) {
            Integer[] stat2 = statsTable.get(field.name());
            stat2[COL_NON_NULL_VALUES] += 1;
            stat2[COL_MALFORMED_VALUES] += 1;
            statsTable.put(field.name(), stat2);
            fieldConversionResult.updateGenericRecordBuilder(field, null);
        } catch (NullPointerException e) {
            handleNullOrEmpty(fieldConversionResult, field);
        }
    }

    private void convertDouble(InputRecord record, FieldConversionResult fieldConversionResult, Schema.Field field) {
        if (field.name().equals("lon")) {
            try {
                Double number = getValidatedLatitudeLongitude(record, field, longitude);
                if (number > 180 || number < -180) {
                    throw new NullPointerException();
                }
                fieldConversionResult.updateGenericRecordBuilder("lon", number);
                Float[] minmax = minMaxTable.get(field.name());
                Integer[] stat2 = statsTable.get("lon");
                stat2[COL_NON_NULL_VALUES] += 1;
                if (number == 0.0D) {
                    stat2[COL_ZERO_VALUES] += 1;
                }
                statsTable.put("lon", stat2);
                if (minmax[COL_MIN_VALUE] > number) {
                    minmax[COL_MIN_VALUE] = number.floatValue();
                }
                if (minmax[COL_MAX_VALUE] < number) {
                    minmax[COL_MAX_VALUE] = number.floatValue();
                }
                minMaxTable.put(field.name(), minmax);
            } catch (NumberFormatException e) {
                Integer[] stat2 = statsTable.get("lon");
                stat2[COL_NON_NULL_VALUES] += 1;
                stat2[COL_MALFORMED_VALUES] += 1;
                statsTable.put("lon", stat2);
                fieldConversionResult.setFaulty(true);
            } catch (NullPointerException e1) {
                Integer[] stat = statsTable.get("lon");
                stat[COL_NULL_VALUES] += 1;
                statsTable.put("lon", stat);
                fieldConversionResult.setFaulty(true);
            }
        } else if (field.name().equals("lat")) {
            try {
                Double number = getValidatedLatitudeLongitude(record, field, latitude);
                if (number > 90 || number < -90) {
                    throw new NullPointerException();
                }
                fieldConversionResult.updateGenericRecordBuilder("lat", number);
                Integer[] stat2 = statsTable.get("lat");
                Float[] minmax = minMaxTable.get(field.name());
                stat2[COL_NON_NULL_VALUES] += 1;
                if (number == 0.000000D) {
                    stat2[COL_ZERO_VALUES] += 1;
                }
                statsTable.put("lat", stat2);
                if (minmax[COL_MIN_VALUE] > number) {
                    minmax[COL_MIN_VALUE] = number.floatValue();
                }
                if (minmax[COL_MAX_VALUE] < number) {
                    minmax[COL_MAX_VALUE] = number.floatValue();
                }
                minMaxTable.put(field.name(), minmax);
            } catch (NumberFormatException e) {
                Integer[] stat2 = statsTable.get("lat");
                stat2[COL_NON_NULL_VALUES] += 1;
                stat2[COL_MALFORMED_VALUES] += 1;
                statsTable.put("lat", stat2);
                fieldConversionResult.setFaulty(true);
            } catch (NullPointerException e1) {
                Integer[] stat = statsTable.get("lat");
                stat[COL_NULL_VALUES] += 1;
                statsTable.put("lat", stat);
                fieldConversionResult.setFaulty(true);
            }
        } else if (tokenHeaders.keySet().contains(field.name())){
            try {
                String fieldName = field.name();
                String preToken = record.getString(tokenHeaders.get(fieldName));
                String nr = tokenMaps.get(fieldName).get(preToken.toLowerCase());
                if (nr == null){
                    fieldConversionResult.updateGenericRecordBuilder(field, nr);
                    throw new NullPointerException();
                }
                Double number = Double.parseDouble(nr);
                fieldConversionResult.updateGenericRecordBuilder(field, number);
                Integer[] stat = statsTable.get(fieldName);
                stat[COL_NON_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
                Float[] minmax = minMaxTable.get(field.name());
                if (minmax[COL_MIN_VALUE] > number) {
                    minmax[COL_MIN_VALUE] = number.floatValue();
                }
                if (minmax[COL_MAX_VALUE] < number) {
                    minmax[COL_MAX_VALUE] = number.floatValue();
                }
            } catch (NullPointerException e) {
                Integer[] stat = statsTable.get(field.name());
                stat[COL_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            }
        } else {
            try {
                Double number = record.getDouble(field.name());
                checkFieldValidity(field, checkForNullDouble(number, field.name()) == null, doubleNullValues.contains(number));
                fieldConversionResult.updateGenericRecordBuilder(field, number);
                Integer[] stat = statsTable.get(field.name());
                Float[] minmax = minMaxTable.get(field.name());
                stat[COL_NON_NULL_VALUES] += 1;
                if (number == 0.000000D) {
                    stat[COL_ZERO_VALUES] += 1;
                }
                statsTable.put(field.name(), stat);
                if (minmax[COL_MIN_VALUE] > number) {
                    minmax[COL_MIN_VALUE] = number.floatValue();
                }
                if (minmax[COL_MAX_VALUE] < number) {
                    minmax[COL_MAX_VALUE] = number.floatValue();
                }
            } catch (NumberFormatException e) {
                handleNullOrEmpty(fieldConversionResult, field);
            } catch (NullPointerException e1) {
                Integer[] stat = statsTable.get(field.name());
                stat[COL_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            }
        }
    }

    private void convertLong(InputRecord record, FieldConversionResult fieldConversionResult, Schema.Field field) {
        if (field.name().equals("time")) {
            try {
                if (record.getString(time).equals("")) {
                    throw new NullPointerException();
                }
                Long timeVal = TimeUtils.timeToMillisecondsConverter(record.getString(time), timeFormatter, inputEpochInMilliseconds, timeZone);
                checkFieldValidity(field, checkForNullLong(timeVal, field.name()) == null, longNullValues.contains(timeVal));
                if (startTimeEpoch != 0L && timeVal < startTimeEpoch) {
                    timeGated += 1;
                    fieldConversionResult.setFaulty(true);
                }
                if (endTimeEpoch != 0L && timeVal > endTimeEpoch) {
                    timeGated += 1;
                    fieldConversionResult.setFaulty(true);
                }
                fieldConversionResult.updateGenericRecordBuilder("time", timeVal);
                Integer[] stat2 = statsTable.get("time");
                stat2[COL_NON_NULL_VALUES] += 1;
                statsTable.put("time", stat2);
            } catch (DateTimeParseException e) {
                Integer[] stat2 = statsTable.get("time");
                stat2[COL_NON_NULL_VALUES] += 1;
                stat2[COL_MALFORMED_VALUES] += 1;
                statsTable.put("time", stat2);
                fieldConversionResult.setFaulty(true);
            } catch (NullPointerException e1) {
                Integer[] stat = statsTable.get("time");
                stat[COL_NULL_VALUES] += 1;
                statsTable.put("time", stat);
                fieldConversionResult.setFaulty(true);
            }
        } else if (hashColumns.contains(field.name())) {
            String fieldName = field.name();
            Integer index = hashColumns.indexOf(fieldName);
            try {
                Long hashValue = generateNewHash(record.getString(fieldName), index);
                fieldConversionResult.updateGenericRecordBuilder(field, hashValue);
                Integer[] stat = statsTable.get(field.name());
                stat[COL_NON_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            } catch (NullPointerException e) {
                Integer[] stat = statsTable.get(field.name());
                stat[COL_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            }
        }
        // check wether it is the newly generated field for retainHashes by checking it's final 4 symbols
        else if(field.name().substring(field.name().length() - 4).equals("_IDs")){
            String fieldName = field.name();
            Integer index = hashColumns.size() + retainHashes.indexOf(field.name().substring(0, field.name().length() - 4));
            try {
                Long hashValue = generateNewHash(record.getString(fieldName.substring(0,fieldName.length() - 4)), index);
                fieldConversionResult.updateGenericRecordBuilder(field, hashValue);
                Integer[] stat = statsTable.get(fieldName);
                stat[COL_NON_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            } catch (NullPointerException e) {
                Integer[] stat = statsTable.get(field.name());
                stat[COL_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            }
        }
        // Check wether it a generated row by one of the token Files.
        else if (tokenHeaders.keySet().contains(field.name())){
            try{
                String fieldName = field.name();
                String preToken = record.getString(tokenHeaders.get(fieldName));
                String nr = tokenMaps.get(fieldName).get(preToken.toLowerCase());
                if (nr == null){
                    fieldConversionResult.updateGenericRecordBuilder(field, null);
                    throw new NullPointerException();
                }
                Long number = Long.parseLong(nr);
                fieldConversionResult.updateGenericRecordBuilder(field, number);
                Integer[] stat = statsTable.get(fieldName);
                stat[COL_NON_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            } catch (NullPointerException e) {
                Integer[] stat = statsTable.get(field.name());
                stat[COL_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            }
        }
        else {
            // Could be null or long
            try {
                Long date = TimeUtils.timeToMillisecondsConverter(record.getString(field.name()), timeFormatter, inputEpochInMilliseconds, timeZone);
                checkFieldValidity(field, checkForNullLong(date, field.name()) == null, longNullValues.contains(date));
                fieldConversionResult.updateGenericRecordBuilder(field, date);
                Integer[] stat = statsTable.get(field.name());
                stat[COL_NON_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            } catch (DateTimeParseException e) {
                try {
                    Long number = record.getLong(field.name());
                    checkFieldValidity(field, checkForNullLong(number, field.name()) == null, longNullValues.contains(number));
                    Integer[] stat = statsTable.get(field.name());
                    stat[COL_NON_NULL_VALUES] += 1;
                    if (number == null || number == 0L) {
                        stat[COL_ZERO_VALUES] += 1;
                    }
                    statsTable.put(field.name(), stat);
                    fieldConversionResult.updateGenericRecordBuilder(field, number);
                } catch (NumberFormatException e1) {
                    Integer[] stat2 = statsTable.get(field.name());
                    stat2[COL_NON_NULL_VALUES] += 1;
                    stat2[COL_MALFORMED_VALUES] += 1;
                    statsTable.put("lon", stat2);
                }
            } catch (NullPointerException e2) {
                Integer[] stat = statsTable.get(field.name());
                stat[COL_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            }
        }
    }

    private void checkFieldValidity(Schema.Field field, boolean isNullCheckNull, boolean containsNullValues) {
        if (rowNulls.containsKey(field.name())) {
            if (isNullCheckNull) {
                throw new NullPointerException();
            }
        } else {
            if (containsNullValues) {
                throw new NullPointerException();
            }
        }
    }

    private Double getValidatedLatitudeLongitude(InputRecord record, Schema.Field field, String value) {
        if (record.getString(value).equals("")) {
            throw new NullPointerException();
        }
        Double number = record.getDouble(value);
        checkFieldValidity(field, checkForNullDouble(number, field.name()) == null, doubleNullValues.contains(number));
        return number;
    }

    private Schema.Type getSchemaType(InputRecord record, Schema.Field field, Schema subschema) {
        String targetName;
        if (field.name().equals("lat")) {
            targetName = latitude;
        } else if (field.name().equals("lon")) {
            targetName = longitude;
        } else if (field.name().equals("time")) {
            targetName = time;
        } else if (field.name().substring(field.name().length() - 4).equals("_IDs")){   // Deal with created hash rows
            return Schema.Type.LONG;
        } else if (tokenHeaders.keySet().contains(field.name())){
            switch(tokenTypes.get(field.name())){
                case "long": {
                    return Schema.Type.LONG;
                }
                case "double": {
                    return Schema.Type.DOUBLE;
                }
                case "string": {
                    return Schema.Type.STRING;
                }
                case "float": {
                    return Schema.Type.FLOAT;
                }
                default:{
                    log.info("This should not be possible");
                    return null;
                }
            }
        }
        else {
            targetName = field.name();
        }
        return getSchemaType(subschema, record.getString(targetName));
    }

    private void writeStatsFile() {
        statistics.append("Parquet file generation successful. \n \n \nGeneral statistics: \nFound a total of " + totalRecords + " records. ");
        statistics.append(writtenRecords + " records from " + files + " files parsed into the parquet file. " + (totalRecords - writtenRecords) + " records contained malformed lat, lon or time fields. " + timeGated + " records discarded due to time restrictions.\n");
        statistics.append("Time restrictions set:    \nStart time:  " + startTime + "    \nEnd time:  " + endTime + ". \n\n");
        statistics.append("Field statistics in the form of: field name, non-null values, null values :  \n\n");
        for (Map.Entry<String,Set<String>> entry : uniqueStrings.entrySet()) {
            if (entry.getValue().size() > uniqueMax) {
                statistics.append("WARNING!!! Field " + entry.getKey() + " exceeded maximum allowed unique Classifiers. \n");
            }
        }
        for (Map.Entry<String,Integer[]> entry  : statsTable.entrySet()) {
            String key = entry.getKey();
            Integer[] value = entry.getValue();
            DecimalFormat df = new DecimalFormat();

            // Statistics are shown in a comfortable format.

            df.applyPattern("###.###");
            statistics.append(key + " , non-null: " + value[0] + " , invalid: " + value[1] + " , null: " + value[2] + " , zero-values: " + value[3] + " , type: " + typeToString(typeTable.get(key)) + "\n");
            if (minMaxTable.containsKey(key)) {
                statistics.append("     Min: " + df.format(minMaxTable.get(key)[0]) + "       Max: " + df.format(minMaxTable.get(key)[1]) + "\n\n");
            }
            if (uniqueStrings.containsKey(key)) {
                statistics.append("     Unique string count: " + uniqueStrings.get(key).size() + "\n\n");
            }
        }
        statistics.append("Time data.\nDate followed by the number of samples found from that date\n\n\n");
        for (Map.Entry<String,Integer> entry : timeData.entrySet()) {
            statistics.append(entry.getKey() + "   -   " + entry.getValue() + "\n");
        }
        try (FileWriter fw = new FileWriter(statsFile)) {
            fw.write(statistics.toString());
        } catch (IOException e) {
            log.info("Error writing the statistics file. Error message: " + e.getMessage());
        }
    }

    private String newFileName(Integer number) {
        if (outputFile.contains(".")) {
            String[] pieces = outputFile.split("[.]");
            return pieces[0] + "_" + number + PARQUET_FILE_EXTENSION;
        } else {
            return outputFile + "_" + number + PARQUET_FILE_EXTENSION;
        }
    }

    private void run() {

        if (configFile.equals("")) {
            throw new RuntimeException("Configuration file not set, cannot proceed.");
        }
        readConfigFile();
        List<File> csvFiles = new ArrayList<>();
        List<File> parquetFiles = new ArrayList<>();

        if (!inputFile.isDirectory()) {
            String fileName = inputFile.getName();
            if (fileName.endsWith(".csv") || fileName.endsWith(".gz") || fileName.endsWith(".zip")) {
                csvFiles.add(inputFile);
            } else if (fileName.endsWith(PARQUET_FILE_EXTENSION)) {
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
                    } else if (fileName.endsWith(PARQUET_FILE_EXTENSION)) {
                        parquetFiles.add(file);
                    }
                }
            } catch (NullPointerException e) {
                log.info("Error finding the file/directory, please recheck the file name and make sure this file or directory exists.");
            }
        }
        if (!csvFiles.isEmpty() && !parquetFiles.isEmpty()) {
            throw new RuntimeException("Input data can'be be in CSV and Parquet files at the same time. Input directory should contain only one of these types.");
        }
        InputParser parser;
        File exampleFile;
        List<File> inputFiles;
        if (csvFiles.size() > 0) {
            if(predefinedHeaders){
                parser = new CsvInputParser(headerArray);
                headers = new LinkedList<>(Arrays.asList(headerArray));
            }
            else{
                parser = new CsvInputParser();
            }
            exampleFile = csvFiles.get(0);
            inputFiles = csvFiles;
        } else if (!parquetFiles.isEmpty()) {
            parser = new ParquetInputParser();
            exampleFile = parquetFiles.get(0);
            inputFiles = parquetFiles;
        } else {
            throw new RuntimeException(String.format("Input file {} doesn't have a valid input type", inputFile));
        }
        parser.beginParsing(exampleFile);
        if(!predefinedHeaders){
            headerArray = parser.getHeader();
            headers = new LinkedList<>(Arrays.asList(headerArray));
        }
        InputRecord exampleRow = parser.parseNextRecord();
        String[] exampleArray = exampleRow.getValues();
        examples = new LinkedList<>(Arrays.asList(exampleArray));

        parser.stopParsing();


        // Otherwise throws concurrentmodificationexception
        List<String> iterHeaders = new LinkedList<>(headers);

        //Remove excluded rows and their examples
        for(String row: iterHeaders){
            if (columnsToRemoveList.contains(row)){
                examples.remove(headers.indexOf(row));
                headers.remove(row);
            }
        }

        String timeString = exampleRow.getString(time);
        timeFormatter = TimeUtils.identifyTimeFormat(timeString, timeZone);
        if (timeFormatter == null && timeString.length() > 10) {
            inputEpochInMilliseconds = true;
        }
        for(String row: retainHashes){
            headers.add(row + "_IDs");
            examples.add("0");
        }
        createTokenMaps(tokenFiles);
        for(String row: tokenMaps.keySet()){
            headers.add(row);
            examples.add("0");   // This does not count as token rows are given types based on the entries in tokenTypes
        }
        JSONObject mainjson = createJSONFromCSVRecords(headers, examples);
        hashMapCounters = new long[hashColumns.size() + retainHashes.size()];
        for (int i = 0; i < (hashColumns.size() + retainHashes.size()); i++) {
            HashMap<String, Long> toAdd = new HashMap<>();
            hashTables.add(toAdd);
        }


        Arrays.fill(hashMapCounters, 0L);

        Schema avroSchema = new Schema.Parser().parse(mainjson.toString());
        writeAttributeFile(avroSchema, "attributeTranslation.json");

        for (Schema.Field field : avroSchema.getFields()) {
            statsTable.put(field.name(), new Integer[]{0, 0, 0, 0});
            typeTable.put(field.name(), getSchemaType(field.schema()));
            if (getSchemaType(field.schema()) == Schema.Type.FLOAT || getSchemaType(field.schema()) == Schema.Type.DOUBLE) {
                minMaxTable.put(field.name(), new Float[]{Float.MAX_VALUE, Float.MIN_VALUE});
            }
            if (getSchemaType(field.schema()) == Schema.Type.STRING) {
                uniqueStrings.put(field.name(), new HashSet<>());
            }
        }
        if (!startTime.equals("")) {
            startTimeEpoch = TimeUtils.timeToMillisecondsConverter(startTime, timeFormatter, inputEpochInMilliseconds, timeZone);
        }
        if (!endTime.equals("")) {
            endTimeEpoch = TimeUtils.timeToMillisecondsConverter(endTime, timeFormatter, inputEpochInMilliseconds, timeZone);
        }

        /* CSV -> JSON -> Schema -> Record -> Parquetfile
                            |
                            |
                  attributeTranslation.json

        */

        int blockSize = 1024 * 1024;
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
                            if (parquetSize > 0 && written >= parquetSize) {
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
                } catch (NullPointerException e) {
                    log.info("Error. Encountered an empty file or a file with only a header row. File name is: " + file.getName());
                } finally {
                    files += 1;
                    parser.stopParsing();
                }
            }
        } catch (java.io.IOException e) {
            log.info(String.format("Error writing parquet file %s", e.getMessage()));
        } finally {
            if (parquetWriter != null) {
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