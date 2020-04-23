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
import org.apache.avro.generic.GenericData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RemoveColumnsController {

    private Stage stage;
    private Parent root;


    private HashMap<String, String> backupMap;
    private List<String> backupHeaders;
    private List<ArrayList<String>> backupExamples;

    @FXML
    Button removeRowsButton;
    @FXML
    TextField toBeRemoved;
    @FXML
    Label removeError;
    @FXML
    Button removeRowsNext;
    @FXML
    Button resetButton;
    @FXML
    Button removeRowsBack;
    @FXML
    TableView<DataRow> mainTable;

    public void initialize() {
        backupMap = new HashMap<>(AppData.getTypeMap());
        backupHeaders = new ArrayList<>(AppData.getHeaderList());
        backupExamples = new ArrayList<>();
        // Implementation of deep copy. Copy all of the innerlists to break their reference to the original.
        for(ArrayList<String> innerList: AppData.getExamples()){
            backupExamples.add(new ArrayList<>(innerList));
        }

        updateTable();
    }

    @FXML
    private void updateTable(){
        List<DataRow> newList = new ArrayList<>();
        for(int i = 0; i < AppData.getHeaderList().size() - 1; i++){
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
    private void removeRowsBackClicked() throws IOException{
        stage = (Stage) removeRowsNext.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("determineMainAttributes.fxml"));
        Scene scene = new Scene(root, 700, 600);
        stage.setScene(scene);
        stage.show();
    }

    @FXML
    private void removeRows(){
        List<String> headers = AppData.getHeaderList();
        HashMap<String, String> types = AppData.getTypeMap();
        List<ArrayList<String>> examples = AppData.getExamples();
        String rowString = toBeRemoved.getText();
        String[] rows = rowString.split(",");
        for(String row: rows){
            if(row.equals(AppData.getLatitude()) || row.equals(AppData.getLongitude()) || row.equals(AppData.getTime())){
                removeError.setVisible(true);
                removeError.setText("Cannot remove one of the main attributes");
                return;
            }
            else if(!row.equals("") && !headers.contains(row)){
                removeError.setText("Encountered an invalid header name: " + row);
                removeError.setVisible(true);
                return;
            }
            else if(!row.equals("") && headers.contains(row)){
                for(int i = 0; i < 3; i++){
                    examples.get(i).remove(headers.indexOf(row));
                }
                types.remove(row);
                headers.remove(row);
                removeError.setVisible(false);
            }
        }
        updateTable();
    }
    @FXML
    private void resetRows(){
        AppData.setTypeMap(new HashMap<>(backupMap));
        AppData.setHeaderList(new ArrayList<>(backupHeaders));
        List<ArrayList<String>> oldData = new ArrayList<>();

        // Deep copy.

        for(ArrayList<String> innerList: backupExamples){
            oldData.add(new ArrayList<>(innerList));
        }
        AppData.setExamples(oldData);
        updateTable();
}

    @FXML
    private void removeRowsNextClicked() throws IOException {
        stage = (Stage) removeRowsNext.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("defineNullValues.fxml"));
        Scene scene = new Scene(root, 700, 600);
        stage.setScene(scene);
        stage.show();
    }
}
