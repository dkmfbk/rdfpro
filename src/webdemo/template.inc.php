<!DOCTYPE html>
<html>
<head>
	<title>RDFpro web interface</title>
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css">
    <link href="style.css" rel="stylesheet">

    <!-- HTML5 shim and Respond.js for IE8 support of HTML5 elements and media queries -->
    <!-- WARNING: Respond.js doesn't work if you view the page via file:// -->
    <!--[if lt IE 9]>
      <script src="https://oss.maxcdn.com/html5shiv/3.7.2/html5shiv.min.js"></script>
      <script src="https://oss.maxcdn.com/respond/1.4.2/respond.min.js"></script>
    <![endif]-->

</head>
<body>

	<div class="page-header">
		<div class='pull-right logos'>
			<a href="http://dkm.fbk.eu/"><img src="images/fbkdkm.png"/></a>&nbsp;&nbsp;
			<a href="http://www.newsreader-project.eu/"><img src="images/newsreader.png"/></a>
		</div>
		<h1>
			<img src='images/rdfpro.png' id='logo' />
			<a href='http://rdfpro.fbk.eu'>RDF<sub>pro</sub> <small>The Swiss-Army tool for RDF and Named Graph manipulation</small></a>
		</h1>
	</div>

	<div class='container-fluid'>
		<?php if (isset($Text) && $Text): ?>
			<div class="row">
				<div class="col-md-12">
					<?php echo $Text; ?>
				</div>
			</div>
		<?php endif; ?>

		<p>
			<a href="https://www.youtube.com/watch?v=Vd2FCVRL8fk">Video tutorial</a>
			| <a href="https://www.youtube.com/watch?v=Vd2FCVRL8fk&amp;t=3m13s">Example 1 video</a>
			| <a href="https://www.youtube.com/watch?v=Vd2FCVRL8fk&amp;t=4m25s">Example 2 video</a>
		</p>
		
		<div class="row">
			<div class="col-md-5" id="form-components">
				<form enctype="multipart/form-data" method="POST" id="fileForm" target="_blank">
					<h4>
						<span class="label label-success">1</span>
						Select how to provide the input
					</h4>

					<div id="input-radio">
						<div class="radio">
							<label>
								<input type="radio" name="inputRadio" id="inputRadio_file" value="file" checked="checked" />
								Upload an input RDF file in any popular serialization format
							</label>
							<div class="form-group form-radio">
								<input id="upload_readFile" type="file" name="readFile" id="readFile" />
								<p class="help-block">
									File must be in one of the following format: <?php echo implode(", ", $inputFormat); ?>.<br />
									File may be compressed using: <?php echo implode(", ", $compressionFormat); ?>.
								</p>
							</div>
						</div>
						<div class="radio">
							<label>
								<input type="radio" name="inputRadio" id="inputRadio_text" value="text" />
								Insert data manually
							</label>
							<div class="form-group form-radio">
								<textarea class="form-control" rows="3" name="readText" id="readText"></textarea>
								<div class="form-inline" id="form-format">
									<label for="readTextType">Format</label>
									<select class="form-control" name="readTextType" id="readTextType">
										<?php echo implode("\n", optionList($inputFormat, true, $inputFormatDefault)); ?>
									</select>
								</div>
							</div>
						</div>
						<div class="radio">
							<label>
								<input type="radio" name="inputRadio" id="inputRadio_example" value="example">
								Use an example file (<?php echo $inputDescription; ?>)
							</label>
						</div>
					</div>

					<hr />

					<h4>
						<span class="label label-success">2</span>
						Combine processors
					</h4>
					<div class="form-group">
						<textarea class="form-control" rows="3" name="commands" id="commandsArea"></textarea>
						<p class="help-block">
							Insert here the commands you want to use for the RDFpro processing.
							The list of the available commands is documented in the right side window.
							<a href="#" data-toggle="modal" data-target="#myModal2">Load an example.</a>
						</p>
					</div>

<!-- 					<h5>Pick a pre-loaded example</h5>
					<div class="form-group">
						<select class="form-control" id="exampleList">
							<option value=''>[select]</option>
							<?php echo implode("\n", optionList($exampleList, false)); ?>
						</select>
					</div>
 -->
					<h5>Additional files</h5>
					<div class="form-inline" id="form-additional-file">
						<?php

						for ($i = 1; $i <= $additionalFileNo; $i++) {
							?>
							<div class="form-group">
								<label for="upload_additionalFile1">
									#file<?php echo $i; ?>
								</label>
								<input id="upload_additionalFile1" type="file" name="additionalFile<?php echo $i; ?>" id="additionalFile<?php echo $i; ?>" />
								<a href="#" class="cancel-file"><span class="badge"><span class="glyphicon glyphicon-remove"></span></span></a>
							</div>
							<?php
						}
						?>
						<p class="help-block">
							These additional files can be included in the commands using the labels #file1, #file2, #file3, #file4.<br />
							Some other pre-loaded files are available, too (<a href="#" data-toggle="modal" data-target="#myModal">see list</a>).
						</p>
					</div>

					<hr />

					<h4>
						<span class="label label-success">3</span>
						Select how to get the output
					</h4>
					<div class="form-inline" id="form-output">
						<div class="form-group">
							<label for="fileType">Format</label>
							<select class="form-control" name="fileType" id="fileType">
								<?php echo implode("\n", optionList($outputFormat, true, $outputFormatDefault)); ?>
							</select>

							<label for="fileCompression">Compression</label>
							<select class="form-control" name="fileCompression" id="fileCompression">
								<option value=''>[none]</option>
								<?php echo implode("\n", optionList($compressionFormat, true)); ?>
							</select>

							<label>
								<input id="check_showResults" name="showResults" type="checkbox"> Show results in the browser
							</label>
						</div>
					</div>

					<hr />

					<input class='btn btn-primary' type="submit" value="Send" id="submitButton" data-loading-text="Loading...">
				</form>
			</div>

			<div class="col-md-7" id="documentation">
				<p class="text-right">
					<a class="btn btn-default btn-hide" target="_blank" href="http://rdfpro.fbk.eu/usage.html">
						<span class="glyphicon glyphicon-info-sign"></span>
						Extended documentation
					</a>
					<a class="btn btn-default btn-hide" target="_blank" href="http://rdfpro.fbk.eu/example.html">
						<span class="glyphicon glyphicon-blackboard"></span>
						Usage examples
					</a>

					<button type="button" class="btn btn-default" id="show-hide-button">
						<span class="glyphicon glyphicon-resize-small"></span>
						<span id="show-hide-button-text">Hide documentation panel</span>
					</button>
				</p>

				<pre><?php echo htmlentities(file_get_contents("doc.txt")); ?></pre>
			</div>
		</div>

		<hr />

		<div class="row">
			<div class="col-md-12">
				<p class="text-center">
					RDF<sub>pro</sub> is public domain software
 					|
					<a href='http://rdfpro.fbk.eu/'>Official website</a>
<!--					|
					<a href='http://rdfpro.fbk.eu/usage.html'>Documentation</a> |
					<a href='http://rdfpro.fbk.eu/example.html'>Examples of use</a> |
					<a href='http://rdfpro.fbk.eu/examples_sac.html'>More examples (SAC 2015 conference paper)</a> -->
				</p>
			</div>
		</div>
	</div>

	<div class="modal fade" id="myModal" tabindex="-1" role="dialog" aria-labelledby="myModalLabel">
		<div class="modal-dialog" role="document">
			<div class="modal-content">
				<div class="modal-header">
					<button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
					<h4 class="modal-title" id="myModalLabel">List of pre-loaded files</h4>
				</div>

				<div class="modal-body">
					<ul>
					<?php
						foreach ($customFilesDesc as $key => $value) {
							$filename = $customFiles[$key];
							echo "<li><strong><a target='_blank' href='custom/$filename'>#$key</a></strong> - $value</li>\n";
						}
					?>
					</ul>
				</div>

				<div class="modal-footer">
					<button type="button" class="btn btn-primary" data-dismiss="modal">Close</button>
				</div>
			</div>
		</div>
	</div>

	<div class="modal fade" id="myModal2" tabindex="-1" role="dialog" aria-labelledby="myModalLabel2">
		<div class="modal-dialog" role="document">
			<div class="modal-content">
				<div class="modal-header">
					<button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
					<h4 class="modal-title" id="myModalLabel2">List of examples</h4>
				</div>

				<div class="modal-body">
					<ul>
					<?php
						foreach ($exampleList as $key => $value) {
							echo "<li><strong><a href='#'>$key</a></strong> - $value</li>\n";
						}
						// foreach ($customFilesDesc as $key => $value) {
						// 	$filename = $customFiles[$key];
						// 	echo "<li><strong><a target='_blank' href='custom/$filename'>#$key</a></strong> - $value</li>\n";
						// }
					?>
					</ul>
				</div>

				<div class="modal-footer">
					<button type="button" class="btn btn-primary" data-dismiss="modal">Close</button>
				</div>
			</div>
		</div>
	</div>


    <script type="text/javascript" src="https://ajax.googleapis.com/ajax/libs/jquery/1.11.1/jquery.min.js"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js"></script>

    <script type="text/javascript" src="js/jquery.overlay.min.js"></script>
    <script type="text/javascript" src="js/jquery.textcomplete.min.js"></script>

    <script type="text/javascript" src="js.php"></script>
    <script type="text/javascript" src="js/custom.js"></script>

</body>
</html>