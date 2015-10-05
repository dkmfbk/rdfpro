<div class='page-header'>
    <h1>RDFpro web interface</h1>
</div>

---------------------------------------

### Screenshot

<a href="https://knowledgestore2.fbk.eu/rdfpro-demo/"><img id='demo-img' src='images/demo.png' /></a>

### Description

RDFpro 0.4 comes with a web interface, written in [php](http://www.php.net/).
To install it in your webserver (for example [Apache](http://httpd.apache.org/)), just copy the `src/webdemo` folder in the distributed tar.gz package and copy it to the webserver root folder.

The main options of the web interface can be set by editing the `config.inc.php` file.

* `$rdfpro_path` is the path to RDFpro executable on your machine.
* `$java_home` is the Java home path.
* `$additionalFileNo` is the number of slots for uploading files.
* `$inputFormat`, `$outputFormat` and `$compressionFormat` tell the web interface which extensions should be accepted.
* `$inputFormatDefault` and `$outputFormatDefault` are the default values for the corresponding select menus.
* `$allowedCommands` is the list of allowed commands to pass to RDFpro.
* `$inputExample` is the input file to be used as default (third radio button in the input part).
* `$inputDescription` is the description of `$inputExample`.
* `$exampleList` is a list of predefined examples.
* `$customFiles` is a list of predefined files, that can be used without uploading them every time.
    The keys of the array is the codename, while the value is the filename (to be copied in the `custom` folder).
* `$customFilesDesc` is the list of descriptions for the `$customFiles`. The array keys must match.
