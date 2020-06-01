package com.demograft.ldb4rdbconverter.parser;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.extern.slf4j.Slf4j;

import java.io.File;

@Slf4j
public class CsvInputParser implements InputParser {

    CsvParser parser;
    String[] header;

    public CsvInputParser() {
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        settings.setMaxColumns(1000);
        parser = new CsvParser(settings);

    }

    @Override
    public void beginParsing(File file) {
        log.info("Using csv reader for {}", file);
        parser.beginParsing(file);
        header = parser.parseNextRecord().getValues();
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
