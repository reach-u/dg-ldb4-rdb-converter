package com.demograft.ldb4rdbconverter.parser;

import java.io.File;

public interface InputParser {


    void beginParsing(File file);

    String[] getHeader();

    InputRecord parseNextRecord();

    void stopParsing();
}
