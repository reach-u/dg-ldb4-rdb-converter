package com.demograft.ldb4rdbconverter;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class GUIController {

    private Stage stage;
    private Parent root;


    @FXML
    TextField newConfigFileName;
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
    TextField unusedRows;
    @FXML
    TableView<DataRow> mainTable;
    @FXML
    Button mainRowsButton;
    @FXML
    Button mainAttributesNext;
    @FXML
    Button populateMain;
    @FXML
    TextField longitude;
    @FXML
    TextField latitude;
    @FXML
    TextField time;
    @FXML
    Label longitudeError;
    @FXML
    Label latitudeError;
    @FXML
    Label timeError;
    @FXML
    Label mainAttrError;

    private void updateTable(){
        List<DataRow> newList = new ArrayList<>();
        for(int i = 0; i < AppData.getHeaderList().size(); i++){
            List<String> headers = AppData.getHeaderList();
            StringBuilder sb = new StringBuilder();
            for(int j = 0; j < 3; j++){
                sb.append(AppData.getExamples().get(j).get(i) + "; ");
            }
            DataRow row = new DataRow(headers.get(i),AppData.getTypeMap().get(headers.get(i)), sb.toString());
            newList.add(row);
        }
        AppData.setGUIexamples(newList);
        ObservableList<DataRow> olist = FXCollections.observableArrayList(newList);
        mainTable.setItems(olist);
    }

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
        AppData.setBaseTypes(map);
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
    private void selectPreviousBackClicked() throws Exception{
        stage = (Stage) selectPreviousBack.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("mainScreen.fxml"));
        Scene scene = new Scene(root);
        stage.setScene(scene);
        stage.show();
    }
    @FXML
    private void newFileNextClicked() throws Exception{
        if(newConfigFileName.getText().equals("") || AppData.getInputFile() == null){
            newFileError.setText("All inputs are required");
            newFileError.setVisible(true);
        }
        else{
            AppData.setConfigFile(new File(newConfigFileName.getText()));
            CsvParserSettings settings = new CsvParserSettings();
            settings.getFormat().setLineSeparator("\n");
            CsvParser parser = new CsvParser(settings);
            parser.beginParsing(AppData.getInputFile());
            String[] headerArray = parser.parseNextRecord().getValues();
            int headerNr = headerArray.length;
            List<String> headerList = Arrays.asList(headerArray);
            AppData.setHeaderList(headerList);
            List<List<String>> examples = new ArrayList<>();
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
        File configFile = fileChooser.showOpenDialog(secondStage);
        AppData.setConfigFile(configFile);
        if(configFile != null) {
            previousFile.setText("File:" + configFile.getName());
        }
    }
    @FXML
    private void previousFileNextClicked() throws Exception {
        if (AppData.getConfigFile() == null) {
            previousFileError.setVisible(true);
        } else {
            stage = (Stage) loadExistingNext.getScene().getWindow();
            root = FXMLLoader.load(getClass().getClassLoader().getResource("determineMainAttributes.fxml"));
            Scene scene = new Scene(root, 700, 600);
            stage.setScene(scene);
            stage.show();
        }
    }
    @FXML
    private void removeRows() throws Exception{

    }
    @FXML
    private void populateMainRows(){
        ObservableList<DataRow> olist = FXCollections.observableArrayList(AppData.getGUIexamples());
        mainTable.setItems(olist);
        populateMain.setVisible(false);
    }
    @FXML
    private void updateMainRows(){
        String lon = longitude.getText();
        String lat = latitude.getText();
        String tim = time.getText();
        HashMap<String, String> map = AppData.getTypeMap();
        if(!lon.equals("") && AppData.getHeaderList().contains(lon)){
            if(!AppData.getLongitude().equals("")){
                String previous = AppData.getLongitude();
                System.out.println("EELMINE:  " + AppData.getBaseTypes().get(previous));
                map.put(previous, AppData.getBaseTypes().get(previous));
                System.out.println("NÜÜD: " + map.get(previous));

            }
            map.put(lon, "Double");
            AppData.setLongitude(lon);
            longitudeError.setVisible(false);
        }
        else if(!lon.equals("") && !AppData.getHeaderList().contains(lon)){
            longitudeError.setVisible(true);
        }
        if(!lat.equals("") && AppData.getHeaderList().contains(lat)){
            if(!AppData.getLatitude().equals("")){
                String previous = AppData.getLatitude();
                map.put(previous, AppData.getBaseTypes().get(previous));
            }
            map.put(lat, "Double");
            AppData.setLatitude(lat);
            latitudeError.setVisible(false);
        }
        else if(!lat.equals("") && !AppData.getHeaderList().contains(lat)){
            latitudeError.setVisible(true);
        }
        if(!tim.equals("") && AppData.getHeaderList().contains(tim)){
            if(!AppData.getTime().equals("")){
                String previous = AppData.getTime();
                map.put(previous, AppData.getBaseTypes().get(previous));
            }
            map.put(tim, "Long");
            AppData.setTime(tim);
            timeError.setVisible(false);
        }
        else if(!tim.equals("") && !AppData.getHeaderList().contains(lon)){
            timeError.setVisible(true);
        }
        AppData.setTypeMap(map);
        updateTable();
    }
    @FXML
    private void mainAttributesNextClicked() throws IOException{
        if(!AppData.mainSet()){
            mainAttrError.setVisible(true);
        }
        else {
            mainAttrError.setVisible(false);
            stage = (Stage) mainAttributesNext.getScene().getWindow();
            root = FXMLLoader.load(getClass().getClassLoader().getResource("removeColumns.fxml"));
            Scene scene = new Scene(root, 700, 600);
            stage.setScene(scene);
            stage.show();
        }
    }
}