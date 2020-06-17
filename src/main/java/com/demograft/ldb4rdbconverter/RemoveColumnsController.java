package com.demograft.ldb4rdbconverter;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.stage.Stage;
import org.apache.avro.generic.GenericData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.demograft.ldb4rdbconverter.Helpers.copySelectionToClipboard;

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
    TextField defaultType;
    @FXML
    TableView<DataRow> mainTable;
    @FXML
    TextField searchBar;
    @FXML
    Label typeError;
    private FilteredList<DataRow> filteredList;
    private ObservableList<DataRow> dataList;

    public void initialize() {
        backupMap = new HashMap<>(AppData.getTypeMap());
        backupHeaders = new ArrayList<>(AppData.getHeaderList());
        backupExamples = new ArrayList<>();
        // Implementation of deep copy. Copy all of the innerlists to break their reference to the original.
        for(ArrayList<String> innerList: AppData.getExamples()) {
            backupExamples.add(new ArrayList<>(innerList));
        }
        mainTable.getSelectionModel().setCellSelectionEnabled(true);
        mainTable.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);

        // Implements ctrl + c copying from tableview. Allows selection of multiple rows.

        final KeyCodeCombination keyCodeCopy = new KeyCodeCombination(KeyCode.C, KeyCombination.CONTROL_ANY);
        mainTable.setOnKeyPressed(event -> {
            if (keyCodeCopy.match(event)) {
                copySelectionToClipboard(mainTable);
            }
        });
        dataList = FXCollections.observableArrayList(AppData.getGUIexamples());
        filteredList = new FilteredList<>(dataList, p -> true);
        searchBar.setOnKeyReleased(keyEvent -> {
            filteredList.setPredicate(p -> p.getHeader().toLowerCase().contains(searchBar.getText().toLowerCase().trim()));
        });
        mainTable.setItems(filteredList);
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
        dataList.clear();
        dataList.addAll(newList);
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
                AppData.addRemoved(row);
                removeError.setVisible(false);
            }
        }
        if(!defaultType.getText().equals("")){
            if(defaultType.getText().equals("double") || defaultType.getText().equals("string") || defaultType.getText().equals("float")){
                AppData.setDefaultType(defaultType.getText());
                typeError.setVisible(false);
            }
            else{
                typeError.setVisible(true);
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
        AppData.setRemoved(new ArrayList<>());
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
