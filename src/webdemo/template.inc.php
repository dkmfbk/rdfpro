<!DOCTYPE html>
<html>
<head>

    <!-- Bootstrap -->
    <link href="js/bootstrap/css/bootstrap.min.css" rel="stylesheet">
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
		<h1>RDF<sub>PRO</sub> <small>The Swiss-Army tool for RDF/NG manipulation</small></h1>
	</div>

	<div class='container-fluid'>
		<div class="row">
			<div class="col-md-12">
				<?php echo $Text; ?>
			</div>
		</div>
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
								<input type="radio" name="inputRadio" id="inputRadio_file" value="file" checked="checked">
								Upload an input RDF file in any popular serialization format
							</label>
							<div class="form-group form-radio">
								<input id="upload_readFile" type="file" name="readFile" id="readFile">
								<p class="help-block">
									File must be in one of the following format: rdf, rj, jsonld, nt, nq, trix, trig, tql, ttl, n3, brf.<br />
									File may be compressed using: gz, bz2, xz, 7z.
								</p>
							</div>
						</div>
						<div class="radio">
							<label>
								<input type="radio" name="inputRadio" id="inputRadio_text" value="text">
								Insert data manually
							</label>
							<div class="form-group form-radio">
								<textarea class="form-control" rows="3" name="readText" id="readText"></textarea>
								<div class="form-inline" id="form-format">
									<label for="readTextType">Format</label>
									<select class="form-control" name="readTextType" id="readTextType">
										<option value='rdf'>rdf</option>
										<option value='rj'>rj</option>
										<option value='jsonld'>jsonld</option>
										<option value='nt'>nt</option>
										<option value='nq'>nq</option>
										<option value='trix'>trix</option>
										<option value='trig' selected="selected">trig</option>
										<option value='tql'>tql</option>
										<option value='ttl'>ttl</option>
										<option value='n3'>n3</option>
										<option value='brf'>brf</option>
									</select>
								</div>
							</div>
						</div>
						<div class="radio">
							<label>
								<input type="radio" name="inputRadio" id="inputRadio_example" value="example">
								Use an example file (10K triples extracted from DBpedia 2014)
							</label>
						</div>
					</div>

					<hr />

					<h4>
						<span class="label label-success">2</span>
						Combine processors from the right side window
					</h4>
					<div class="form-group">
						<textarea class="form-control" rows="3" name="commands" id="commandsArea"></textarea>
						<p class="help-block">
							Insert here the commands you want to use for the RDFpro processing.
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
								<option value='rdf'>rdf</option>
								<option value='rj'>rj</option>
								<option value='jsonld'>jsonld</option>
								<option value='nt'>nt</option>
								<option value='nq'>nq</option>
								<option value='trix'>trix</option>
								<option value='trig' selected="selected">trig</option>
								<option value='tql'>tql</option>
								<option value='ttl'>ttl</option>
								<option value='n3'>n3</option>
								<option value='brf'>brf</option>
							</select>

							<label for="fileCompression">Compression</label>
							<select class="form-control" name="fileCompression" id="fileCompression">
								<option value=''>[none]</option>
								<option value='gz'>gz</option>
								<option value='bz2'>bz2</option>
								<option value='xz'>xz</option>
								<option value='7z'>7z</option>
							</select>

							<label>
								<input id="check_showResults" name="showResults" type="checkbox"> Show results as output
							</label>
						</div>
					</div>

					<hr />

					<input class='btn btn-primary' type="submit" value="Send" id="submitButton" data-loading-text="Loading...">
				</form>
			</div>

			<div class="col-md-7" id="documentation">
				<p class="text-right">
					<button type="button" class="btn btn-default" id="show-hide-button">
						<span class="glyphicon glyphicon-resize-small"></span>
						<span id="show-hide-button-text">Hide documentation</span>
					</button>
				</p>

				<pre>
AVAILABLE PROCESSORS:

@count          Counts the number of triples for each distinct subject
  [-p] URI      the predicate URI denoting the number of triples (def. void:triples)
  [-c] URI      the context URI where to emit count triples (def. sesame:nil)

@groovy         Transform the stream using a user-supplied Groovy script
  [-p]          use multiple script instances in parallel (script pooling)
  SCRIPT        either the script expression or the name of the script file
  [ARG...]      optional arguments to be passed to the script

@prefix|@p      Adds missing prefix-to-namespace bindings
  [-f FILE]     use prefixes from FILE instead of prefix.cc

@rdfs           Emits the RDFS closure of input quads
  [-e RULES]    exclude RULES in comma-separated list (default: no exclusions)
  [-d]          decompose OWL axioms to RDFS (e.g. equivalentClass -&gt; subClass)
  [-t]          drop uninformative &lt;x rdf:type _:b&gt; statements (default: keep)
  [-C | -c URI] emits closed TBox to default graph [-C] or graph URI [-c]
  [-b URI][-w]  use base URI [-b] and optional BNode rewriting [-w] to load TBox
  [FILE...]     load TBox from FILE...

@rules          Emits the closure of input quads using a set of rules
  [-r RULESETS] use comma separated list of RULESETs (rdfs, owl2rl, custom filename)
  [-B BINDINGS] use comma separated list of var=value BINDINGs to customize rules
  [-p] MODE     set partitioning MODE: 'none' (default), 'entity', 'graph', 'rules'
  [-g] MODE     set graph inference MODE: 'none' (default), 'global', 'separate', 'star'
  [-G] URI      set global graph URI for inference modes 'global' and 'star'
  [-t]          drop uninformative &lt;x rdf:type _:b&gt; statements (default: keep)
  [-C | -c URI] emits closure of static data to original graphs [-C] or graph URI [-c]
  [-b URI][-w]  use base URI [-b] and optional BNode rewriting [-w] to load static data
  [FILE...]     load static data (e.g., TBox) from FILE...

@smush          Performs smushing, using a single URI for each sameAs cluster
  URI...        use ranked namespace URIs to select canonical URIs

@stats          Emits VOID structural statistics for its input
  [-n URI]      use namespace URI to mint URIs for VOID dataset instances
  [-p URI]      create a dataset for graphs linked to a source via property URI
  [-c URI]      look for graph-to-source quads in graph URI
  [-t NUM]      emits only VOID partitions with at least NUM entities or triples
  [-o]          enable computation of void:classes and void:properties (costly)

@tbox           Emits only quads belonging to RDFS or OWL TBox axioms.

@transform|@t   Discards/replaces quads based on matching and replace exp.
  [EXP]         sequence of rules on quad components X (values: s, p, o, c):
                  +X value list =&gt; quad dropped if X not belongs to list
                  -X vlaue list =&gt; quad dropped if X belongs to list
                  =X value =&gt; component X replaced with value (after filters)
                values are in Turtle and include following wildcard values: &lt;*&gt;
                =&gt; any URI; _:* =&gt; any BNode; * =&gt; any plain literal; *@* =&gt; any
                lang literal; *^^* =&gt; any typed literal; *@xyz =&gt; literals lang
                xyz; *^^&lt;uri&gt; =&gt; literals type &lt;uri&gt;; *^^ns:iri =&gt; literals type
                ns:iri; *^^ns:* =&gt; literals any type with prefix ns; ns:* =&gt; any
                uri with prefix ns; &lt;ns*&gt; =&gt; any uri in namespace ns

@unique|@u      Discards duplicates in the input stream
  [-m]          merges quads with same &lt;s,p,o&gt; and different graphs in a unique
                quad, put in a graph described with quads of all source graphs

QUICK SYNTAX:

CMD ::=
  @p args1                          builtin processor (see below)
  @p1 args1 ... @pN argsN           sequence composition
  { @p1 args1 , ... , @pN argsN }S  parallel composition, set operator S

S ::=
  a   multiset sum of @p1 .. @pn outputs, keep duplicates (fast, default)
  u   set union of @p1 .. @pn outputs
  U   multiset union of @p1 .. @pn outputs (with duplicates)
  i   set intersection of @p1 .. @pn outputs
  I   multiset intersection of @p1 .. @pn outputs (with duplicates)
  d   set difference @p1 \ (union of @p2 .. @pN)
  D   multiset difference @p1 \ (union of @p2 .. @pN) (with duplicates)
  s   symmetric set difference: quads in at least a @pi but not all of them
  S   multiset symmetric set difference
  n+  emits quads with at least n (number) occurrences (no duplicates out) 
  n-  emits quads with at most n (number) occurrences (no duplicates out)

				</pre>
			</div>
		</div>

		<hr />

		<div class="row">
			<div class="col-md-12">
				<p class="text-center">
					RDF<sub>PRO</sub> is public domain software |
					<a href='http://rdfpro.fbk.eu/'>Official website</a>
				</p>
			</div>
		</div>
	</div>

	<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.11.2/jquery.min.js"></script>
	<script type="text/javascript" src="js/bootstrap/js/bootstrap.min.js"></script>
    <script type="text/javascript" src="js/custom.js"></script>

</body>
</html>