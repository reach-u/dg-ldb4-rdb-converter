package com.demograft.ldb4rdbconverter;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.stage.Stage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RowTypesController {

    private Stage stage;
    private Parent root;

    private HashMap<String, String> backupMap;
    private List<String> backupHeaders;
    private List<ArrayList<String>> backupExamples;

    @FXML
    private TableView<DataRow> mainTable;
    @FXML
    private Button nextButton;
    @FXML
    private Button backButton;
    @FXML
    private TextField floatRows;
    @FXML
    private TextField doubleRows;
    @FXML
    private TextField stringRows;
    @FXML
    private Label floatError;
    @FXML
    private Label doubleError;
    @FXML
    private Label stringError;
    @FXML
    private Button resetButton;
    @FXML
    private Button updateRowsButton;

    public void initialize() {
        backupMap = new HashMap<>(AppData.getTypeMap());
        backupHeaders = new ArrayList<>(AppData.getHeaderList());
        backupExamples = new ArrayList<>();
        // Implementation of deep copy. Copy all of the innerlists to break their reference to the original.
        for(ArrayList<String> innerList: AppData.getExamples()) {
            backupExamples.add(new ArrayList<>(innerList));
        }
        updateTable();
    }

    @FXML
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
    @FXML
    private void updateRows(){
        List<String> headers = AppData.getHeaderList();
        HashMap<String, String> types = AppData.getTypeMap();
        String floats = floatRows.getText();
        String[] floatrows = floats.split(",");
        for(String row: floatrows){
            if(row.equals(AppData.getLatitude()) || row.equals(AppData.getLongitude()) || row.equals(AppData.getTime())){
                floatError.setText("Cannot change the type of main attribute.");
                floatError.setVisible(true);
            }
            else if(!row.equals("") && !headers.contains(row)){
                floatError.setText("Encountered an invalid header name: " + row);
                floatError.setVisible(true);
                return;
            }
            else if(!row.equals("") && headers.contains(row)){
                types.put(row, "Float");
            }
        }
        String doubles = doubleRows.getText();
        String[] doubleRows = doubles.split(",");
        for(String row: doubleRows){
            if(row.equals(AppData.getLatitude()) || row.equals(AppData.getLongitude()) || row.equals(AppData.getTime())){
                doubleError.setText("Cannot change the type of main attribute.");
                doubleError.setVisible(true);
            }
            else if(!row.equals("") && !headers.contains(row)){
                doubleError.setText("Encountered an invalid header name: " + row);
                doubleError.setVisible(true);
                return;
            }
            else if(!row.equals("") && headers.contains(row)){
                types.put(row, "Double");
            }
        }
        String strings = stringRows.getText();
        String[] stringsRows = strings.split(",");
        for(String row: stringsRows){
            if(row.equals(AppData.getLatitude()) || row.equals(AppData.getLongitude()) || row.equals(AppData.getTime())){
                stringError.setText("Cannot change the type of main attribute.");
                stringError.setVisible(true);
            }
            else if(!row.equals("") && !headers.contains(row)){
                stringError.setText("Encountered an invalid header name: " + row);
                stringError.setVisible(true);
                return;
            }
            else if(!row.equals("") && headers.contains(row)){
                types.put(row, "String");
            }
        }
        updateTable();
    }
    @FXML
    private void resetRows(){
        AppData.setTypeMap(new HashMap<>(backupMap));
        AppData.setHeaderList(new ArrayList<>(backupHeaders));

        // Deep copy.

        List<ArrayList<String>> oldData = new ArrayList<>();
        for(ArrayList<String> innerList: backupExamples){
            oldData.add(new ArrayList<>(innerList));
        }
        AppData.setExamples(oldData);
        updateTable();
    }

    @FXML
    private void rowTypesBackClicked() throws IOException {
        stage = (Stage) backButton.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("defineNullValues.fxml"));
        Scene scene = new Scene(root, 700, 600);
        stage.setScene(scene);
        stage.show();
    }
    @FXML
    private void rowTypesNextClicked() throws IOException{
        stage = (Stage) backButton.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("specialRows.fxml"));
        Scene scene = new Scene(root, 700, 600);
        stage.setScene(scene);
        stage.show();
    }
}