<?php

$rdfpro_path = "/data/software/bin/rdfpro-demo";
$java_home = "/data/software/jdk8";

$additionalFileNo = 4;

$inputFormat = array('rdf', 'rj', 'jsonld', 'nt', 'nq', 'trix', 'trig', 'tql', 'ttl', 'n3', 'brf');
$outputFormat = $inputFormat;
$compressionFormat = array('gz', 'bz2', 'xz', '7z');

$inputFormatDefault = "trig";
$outputFormatDefault = "trig";

$allowedCommands = array('count', 'esoreasoner', 'groovy', 'prefix', 'rdfs', 'rules', 'smush', 'stats', 'tbox', 'transform', 'unique');

// $inputExample = "abox10k.tql.gz";
$inputExample = "1000soccerPlayersDBpedia_10000.ttl";
$inputDescription = "10K triples extracted from DBpedia 2014";

$exampleList = array(
    "@transform '+p rdfs:label' @unique" => "Extracts rdfs:labels and remove duplicate quads",
    "@transform groovy:#grul @rules -r #rdfs #dbo" => "Converts string used in rdfs:labels assertions to uppercase and materialises RDFS inferred triples (via the internal rule engine)",
);

$customFiles = array(
    "rdfs" => "rdfs.ttl",
    "owl2rl" => "owl2rl.ttl",
    "dbo" => "dbpedia_2014.owl",
    "grul" => "uppercase_labels.groovy",
);

$customFilesDesc = array(
    "rdfs" => "RDFS ruleset",
    "owl2rl" => "OWL 2 RL ruleset",
    "dbo" => "DBpedia ontology (TBox)",
    "grul" => "Groovy script to convert uppercase RDFS labels",
);

// ---

// error_reporting(E_NOTICE);
// ini_set("display_errors", "On");
putenv("JAVA_HOME=$java_home");

$F = dirname(__FILE__);
$customFolder = "$F/custom";

