// Imports
var RDF = Java.type("org.eclipse.rdf4j.model.vocabulary.RDF");
var RDFS = Java.type("org.eclipse.rdf4j.model.vocabulary.RDFS");
var OWL = Java.type("org.eclipse.rdf4j.model.vocabulary.OWL");
var XMLSchema = Java.type("org.eclipse.rdf4j.model.vocabulary.XMLSchema");
var URI = Java.type("org.eclipse.rdf4j.model.URI");
var BNode = Java.type("org.eclipse.rdf4j.model.BNode");
var Literal = Java.type("org.eclipse.rdf4j.model.Literal");
var Resource = Java.type("org.eclipse.rdf4j.model.Resource");
var Value = Java.type("org.eclipse.rdf4j.model.Value");

// Global objects
var vf = org.eclipse.rdf4j.model.impl.SimpleValueFactory.getInstance();

// SPARQL functions for classifying nodes
function isiri(x) { return x instanceof URI; }
function isblank(x) { return x instanceof BNode; }
function isliteral(x) { return x instanceof Literal; }
function isnumeric(x) { return x instanceof Literal && x.datatype != null &&
	org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil.isNumericDatatype(x.datatype); }

// SPARQL functions for extracting node components
function str(x) { if (x instanceof BNode) throw "str() called with bnode " + x; return x.toString(); }
function lang(x) { if (x instanceof Literal) return x.language; throw "not a literal: " + x; }
function datatype(x) {
	if (!(x instanceof Literal)) throw "not a literal: " + x;
	if (x.datatype != null) return x.datatype; 
	if (x.language != null) return RDF.LANGSTRING;
	return XMLSchema.STRING;
}

// SPARQL functions for creating nodes
function iri(x) { return x instanceof URI ? x : vf.createURI(x.toString()); } 
function bnode(x) { return (typeof b == 'undefined') ? vf.createBNode()
		: x instanceof BNode ? x : vf.createBNode(x.toString()); }
function strdt(x, y) { return vf.createLiteral(x.toString(), iri(y)); }
function strlang(x, y) { return vf.createLiteral(x.toString(), y.toString()); }
function uuid() { return vf.createURI("urn:uuid:" + java.util.UUID.randomUUID().toString()); }
function struuid() { return java.util.UUID.randomUUID().toString(); }

// SPARQL functions for manipulating strings
function strlen(x) {}
function substr(x, y, z) {}
function ucase(x) {}
function lcase(x) {}
function strstarts(x, y) {}
function strends(x, y) {}
function contains(x, y) {}
function strbefore(x, y) {}
function strafter(x, y) {}
function encode_for_uri(x) {}
function concat() {}
function langmatches(x, y) {}
function regex(x, y, z) {}
function replace(x, y, z, w) {}

// SPARQL functions for manipulating numbers
function abs(x) {}
function round(x) {}
function ceil(x) {}
function floor(x) {}
function rand() {}

// SPARQL functions for manipulating dates
function now() {}
function year(x) {}
function month(x) {}
function day(x) {}
function hours(x) {}
function minutes(x) {}
function seconds(x) {}
function timezone(x) {}
function tz(x) {}

// SPARQL hash functions
function md5(x) {}
function sha1(x) {}
function sha256(x) {}
function sha384(x) {}
function sha512(x) {}

// Other cast operators
function bool(x) {}
function dbl(x) {}
function flt(x) {}
function dec(x) {}
function integer(x) {}
function dt(x) {}
