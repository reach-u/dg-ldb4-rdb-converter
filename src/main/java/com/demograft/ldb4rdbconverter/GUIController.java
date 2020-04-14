package com.demograft.ldb4rdbconverter;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javafx.stage.Window;

import java.io.File;
import java.util.ArrayList;

public class GUIController {

    private Stage stage;
    private Parent root;
    private File inputFile = null;
    private File configFile = null;


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
        inputFile = fileChooser.showOpenDialog(secondStage);
        if(inputFile != null) {
            chosenFileName.setText("File:" + inputFile.getName());
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
        if(newConfigFileName.getText().equals("") || inputFile == null){
            newFileError.setVisible(true);
        }
        else{
            stage = (Stage) newFileNextButton.getScene().getWindow();
            root = FXMLLoader.load(getClass().getClassLoader().getResource("determineUnusedRows.fxml"));
            Scene scene = new Scene(root);
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
        configFile = fileChooser.showOpenDialog(secondStage);
        if(configFile != null) {
            previousFile.setText("File:" + configFile.getName());
        }
    }
    @FXML
    private void previousFileNextClicked() throws Exception {
        if (configFile == null) {
            previousFileError.setVisible(true);
        } else {
            stage = (Stage) loadExistingNext.getScene().getWindow();
            root = FXMLLoader.load(getClass().getClassLoader().getResource("determineUnusedRows.fxml"));
            Scene scene = new Scene(root);
            stage.setScene(scene);
            stage.show();
        }
    }
}