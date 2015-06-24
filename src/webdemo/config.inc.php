<?php

error_reporting(E_NOTICE);
ini_set("display_errors", "On");
putenv("JAVA_HOME=/data/software/jdk8");

$F = dirname(__FILE__);
$eso_path = "$F/ESO.owl";
$rdfpro_path = "/data/software/bin/rdfpro";
// $java_path = "/data/software/bin/java";
// $html_jar_path = "$F/eso-reasoner-1.0-SNAPSHOT.jar";

// $rdfp_command = "$rdfpro_path @read %s @esoreasoner $eso_path @write %s";
// $html_command = "$java_path -Xmx10G -cp $html_jar_path eu.fbk.eso.ReasonerAPI -i %s -w %s";

class UploadException extends Exception {
    public function __construct($code) { 
        $message = $this->codeToMessage($code); 
        parent::__construct($message, $code); 
    } 

    private function codeToMessage($code) 
    { 
        switch ($code) { 
            case UPLOAD_ERR_INI_SIZE: 
                $message = "The uploaded file exceeds the upload_max_filesize directive in php.ini"; 
                break; 
            case UPLOAD_ERR_FORM_SIZE: 
                $message = "The uploaded file exceeds the MAX_FILE_SIZE directive that was specified in the HTML form";
                break; 
            case UPLOAD_ERR_PARTIAL: 
                $message = "The uploaded file was only partially uploaded"; 
                break; 
            case UPLOAD_ERR_NO_FILE: 
                $message = "No file was uploaded"; 
                break; 
            case UPLOAD_ERR_NO_TMP_DIR: 
                $message = "Missing a temporary folder"; 
                break; 
            case UPLOAD_ERR_CANT_WRITE: 
                $message = "Failed to write file to disk"; 
                break; 
            case UPLOAD_ERR_EXTENSION: 
                $message = "File upload stopped by extension"; 
                break; 

            default: 
                $message = "Unknown upload error"; 
                break; 
        } 
        return $message; 
    } 
} 
