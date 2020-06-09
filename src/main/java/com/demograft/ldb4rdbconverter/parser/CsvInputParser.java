package com.demograft.ldb4rdbconverter.parser;

import com.univocity.parsers.common.fields.FieldSet;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.lang.reflect.Field;
import java.util.List;

@Slf4j
public class CsvInputParser implements InputParser {

    CsvParser parser;
    String[] header;

    public CsvInputParser(String[] excluded) {
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        settings.setMaxColumns(1000);
        settings.excludeFields(excluded);
        parser = new CsvParser(settings);
    }

    public CsvInputParser(String[] headers, String[] excluded){
        CsvParserSettings settings = new CsvParserSettings();
        settings.setFormat(new CsvFormat());
        settings.getFormat().setLineSeparator("\n");
        settings.setMaxColumns(1000);
        settings.setHeaders(headers);
        settings.excludeFields(excluded);
        parser = new CsvParser(settings);
        header = headers;
    }

    @Override
    public void beginParsing(File file) {
        log.info("Using csv reader for {}", file);
        parser.beginParsing(file);
        if(header == null) {
            header = parser.parseNextRecord().getValues();
        }
    }

    @Override
    public String[] getHeader() {
        return header.clone();
    }

    @Override
    public InputRecord parseNextRecord() {
        Record record = parser.parseNextRecord();
        if (record == null) {
            return null;
        }
        return new CsvInputRecord(record);
    }


    @Override
    public void stopParsing() {
        parser.stopParsing();

    }
}
