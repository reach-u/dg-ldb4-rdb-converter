package com.demograft.ldb4rdbconverter;

import javafx.fxml.FXML;
import javafx.scene.Parent;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.stage.Stage;

public class FinalScreenController {

    private Stage stage;
    private Parent root;


    @FXML
    Button closingButton;
    @FXML
    Label closingMessage;

    public void initialize() {
        closingMessage.setText("Your configuration file " + AppData.getConfigName() + " has now been created.\nYou can find it in the running catalogue");
    }

    @FXML
    private void CloseProgram(){
        System.exit(0);
    }
}