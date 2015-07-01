<?php

header("Content-Type: text/javascript");

echo "// Some variables built using php\n";

require "commons.inc.php";

$fileList = array_keys($customFiles);
for ($i = 1; $i <= $additionalFileNo; $i++) {
	$fileList[] = "file$i";
}
$fileList = addQuotes($fileList);

echo "var fileList = [".implode(", ", $fileList)."];\n";
echo "var allowedCommands = ['".implode("', '", $allowedCommands)."'];\n";
