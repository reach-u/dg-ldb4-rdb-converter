package com.demograft.ldb4rdbconverter;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class GUIController {

    private Stage stage;
    private Parent root;


    @FXML
    Button loadButton;
    @FXML
    Button newButton;
    @FXML
    Button selectFileButton;
    @FXML
    Button newFileNextButton;
    @FXML
    Label newFileError;
    @FXML
    Label chosenFileName;
    @FXML
    Label previousFile;
    @FXML
    Button selectPreviousButton;
    @FXML
    Button loadExistingNext;
    @FXML
    Label previousFileError;
    @FXML
    Button selectPreviousBack;
    @FXML
    Button selectNewBack;
    @FXML
    Label previousData;
    @FXML
    Button selectPreviousDataButton;

    private void determineInitialTypes(List<String> headers, List<String> examples){
        HashMap<String, String> map = AppData.getTypeMap();
        for(int i = 0; i < headers.size(); i++){
            String example = examples.get(i);
            try {
                if (example.equals("")) {
                    map.put(headers.get(i), "Undetermined");
                } else {
                    try {
                        Float.parseFloat(example);
                        map.put(headers.get(i), "Float");
                    } catch (NumberFormatException e) {
                        map.put(headers.get(i), "String");
                    }
                }
            }
            catch(NullPointerException e){
                map.put(headers.get(i), "Undetermined");
            }
        }
        HashMap<String, String> baseMap = new HashMap<>(map);
        AppData.setBaseTypes(baseMap);
        AppData.setTypeMap(map);
    }

    @FXML
    private void loadExistingClicked() throws Exception{
        stage = (Stage) loadButton.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("loadExisting.fxml"));
        Scene scene = new Scene(root);
        stage.setScene(scene);
        stage.show();
    }
    @FXML
    private void createNewClicked() throws Exception{
        stage = (Stage) newButton.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("newFileSelect.fxml"));
        Scene scene = new Scene(root);
        stage.setScene(scene);
        stage.show();
    }
    @FXML
    private void selectNewFileClicked(){
        FileChooser fileChooser = new FileChooser();
        Stage secondStage = new Stage();
        File inputFile = fileChooser.showOpenDialog(secondStage);
        AppData.setInputFile(inputFile);
        if(inputFile != null && inputFile.getName().endsWith(".csv")) {
            chosenFileName.setText("File:" + inputFile.getName());
            newFileError.setVisible(false);
        }
        else if(inputFile != null ){
            chosenFileName.setText("No file selected");
            newFileError.setText("Wrong file format. Please select a .csv file");
            newFileError.setVisible(true);
            AppData.setInputFile(null);
        }
        else{
            AppData.setInputFile(null);
            chosenFileName.setText("No file selected");
        }
    }

    @FXML
    private void selectPreviousDataClicked(){
        FileChooser fileChooser = new FileChooser();
        Stage secondStage = new Stage();
        File inputFile = fileChooser.showOpenDialog(secondStage);
        AppData.setPreviousData(inputFile);
        if(inputFile != null) {
            previousData.setText("File:" + inputFile.getName());
        }
        else{
            AppData.setPreviousData(null);
            previousData.setText("No file selected");
        }
    }

    @FXML
    private void selectPreviousBackClicked() throws Exception{
        stage = (Stage) selectPreviousBack.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("mainScreen.fxml"));
        Scene scene = new Scene(root);
        stage.setScene(scene);
        stage.show();
    }
    @FXML
    private void newFileNextClicked() throws Exception{
        if(AppData.getInputFile() == null){
            newFileError.setText("Data file is required");
            newFileError.setVisible(true);
        }
        else{
            CsvParserSettings settings = new CsvParserSettings();
            settings.getFormat().setLineSeparator("\n");
            settings.setMaxColumns(1_000);
            CsvParser parser = new CsvParser(settings);
            parser.beginParsing(AppData.getInputFile());
            String[] headerArray = parser.parseNextRecord().getValues();
            int headerNr = headerArray.length;
            List<String> headerList = new ArrayList<>(Arrays.asList(headerArray));
            AppData.setHeaderList(headerList);
            List<ArrayList<String>> examples = new ArrayList<>();
            for(int i = 0; i < 3; i++){
                String[] example = parser.parseNextRecord().getValues();
                ArrayList<String> row = new ArrayList<>(Arrays.asList(example));
                examples.add(row);
            }
            AppData.setExamples(examples);
            parser.stopParsing();
            determineInitialTypes(headerList, examples.get(0));
            List<DataRow> GUIexamples = new ArrayList<>();
            for(int i = 0; i < headerNr; i++){
                StringBuilder sb = new StringBuilder();
                for(int j = 0; j < 3; j++){
                    sb.append(examples.get(j).get(i) + "; ");
                }
                DataRow row = new DataRow(headerList.get(i),AppData.getTypeMap().get(headerList.get(i)), sb.toString());
                GUIexamples.add(row);
            }
            AppData.setGUIexamples(GUIexamples);
            stage = (Stage) newFileNextButton.getScene().getWindow();
            root = FXMLLoader.load(getClass().getClassLoader().getResource("determineMainAttributes.fxml"));
            Scene scene = new Scene(root, 700, 600);
            stage.setScene(scene);
            stage.show();
        }
    }
    @FXML
    private void selectNewBackClicked() throws Exception{
        stage = (Stage) selectNewBack.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("mainScreen.fxml"));
        Scene scene = new Scene(root);
        stage.setScene(scene);
        stage.show();
    }
    @FXML
    private void selectPreviousFileClicked(){
        FileChooser fileChooser = new FileChooser();
        Stage secondStage = new Stage();
        File inputFile = fileChooser.showOpenDialog(secondStage);
        AppData.setPreviousConfig(inputFile);
        if(inputFile != null) {
            previousFile.setText("File:" + inputFile.getName());
        }
        else{
            AppData.setPreviousConfig(null);
            previousFile.setText("No file selected");
        }
    }
    @FXML
    private void previousFileNextClicked() throws Exception {
        if (AppData.getPreviousConfig() == null || AppData.getPreviousData() == null) {
            previousFileError.setVisible(true);
        } else {
            CsvParserSettings settings = new CsvParserSettings();
            settings.getFormat().setLineSeparator("\n");
            settings.setMaxColumns(1000);
            CsvParser parser = new CsvParser(settings);
            parser.beginParsing(AppData.getPreviousData());
            String[] headerArray = parser.parseNextRecord().getValues();
            int headerNr = headerArray.length;

            stage = (Stage) loadExistingNext.getScene().getWindow();
            root = FXMLLoader.load(getClass().getClassLoader().getResource("determineMainAttributes.fxml"));
            Scene scene = new Scene(root, 700, 600);
            stage.setScene(scene);
            stage.show();
        }
    }
}