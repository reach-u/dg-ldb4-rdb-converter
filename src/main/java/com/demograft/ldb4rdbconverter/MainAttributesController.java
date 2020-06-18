package com.demograft.ldb4rdbconverter;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.input.*;
import javafx.stage.Stage;

import java.io.IOException;
import java.util.*;

import static com.demograft.ldb4rdbconverter.Helpers.copySelectionToClipboard;

public class MainAttributesController {

    private Stage stage;
    private Parent root;

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
    @FXML
    TableView<DataRow> mainTable;
    @FXML
    Button mainRowsButton;
    @FXML
    Button mainAttributesNext;
    @FXML
    TextField searchBar;
    @FXML
    private FilteredList<DataRow> filteredList;
    private ObservableList<DataRow> dataList;


    public void initialize() {
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
    private void updateMainRows(){
        String lon = longitude.getText();
        String lat = latitude.getText();
        String tim = time.getText();
        HashMap<String, String> map = AppData.getTypeMap();
        if(!lon.equals("") && AppData.getHeaderList().contains(lon)){
            if(!AppData.getLongitude().equals("")){
                String previous = AppData.getLongitude();
                map.put(previous, AppData.getBaseTypes().get(previous));
            }
            map.put(lon, "Double(lon)");
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
            map.put(lat, "Double(lat)");
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
            map.put(tim, "Long(Time)");
            AppData.setTime(tim);
            timeError.setVisible(false);
        }
        else if(!tim.equals("") && !AppData.getHeaderList().contains(lon)){
            timeError.setVisible(true);
        }
        updateTable();
    }
    @FXML
    private void mainAttributesNextClicked() throws IOException {
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
