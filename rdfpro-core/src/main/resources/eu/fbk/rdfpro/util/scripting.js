// Imports
var RDF = Java.type("org.openrdf.model.vocabulary.RDF");
var RDFS = Java.type("org.openrdf.model.vocabulary.RDFS");
var OWL = Java.type("org.openrdf.model.vocabulary.OWL");
var XMLSchema = Java.type("org.openrdf.model.vocabulary.XMLSchema");
var URI = Java.type("org.openrdf.model.URI");
var BNode = Java.type("org.openrdf.model.BNode");
var Literal = Java.type("org.openrdf.model.Literal");
var Resource = Java.type("org.openrdf.model.Resource");
var Value = Java.type("org.openrdf.model.Value");

// Global objects
var vf = org.openrdf.model.impl.ValueFactoryImpl.getInstance();

// Custom functions for dealing with broken equality
function eq(x,y) { return x == null && y == null || x != null && x.equals(y); }

// Custom utility functions for dealing with Sesame model objects
function subj(x) { return x.getSubject(); }
function pred(x) { return x.getPredicate(); }
function obj(x)  { return x.getObject(); }
function ctx(x)  { return x.getContext(); }
function quad(s, p, o, c) {
	var ov = o instanceof Value ? o : vf.createLiteral(o.toString());
	return vf.createStatement(s, p, ov, (typeof c == 'undefined') ? null : c); }
function emit(h, s, p, o, c) {
	if (typeof p == 'undefined') h.handleStatement(s);
	else h.handleStatement(quad(s, p, o, c)); }

// SPARQL functions for classifying nodes
function isiri(x) { return x instanceof URI; }
function isblank(x) { return x instanceof BNode; }
function isliteral(x) { return x instanceof Literal; }
function isnumeric(x) { return x instanceof Literal && x.datatype != null &&
	org.openrdf.model.datatypes.XMLDatatypeUtil.isNumericDatatype(x.datatype); }

// SPARQL functions for extracting node components
function str(x) { if (x instanceof BNode) throw "str() called with bnode " + x; return x.toString(); }
function lang(x) { if (x instanceof Literal) return x.language; throw "not a literal: " + x; }
function datatype(x) {
	if (!(x instanceof Literal)) throw "not a literal: " + x;
	if (x.datatype != null) return x.datatype; 
	if (x.language != null) return RDF.LANGSTRING;
	return XMLSchema.STRING; }

// SPARQL functions for creating nodes
function iri(x) { return x instanceof URI ? x : vf.createURI(x.toString()); } 
function bnode(x) { return (typeof b == 'undefined') ? vf.createBNode()
		: x instanceof BNode ? x : vf.createBNode(x.toString()); }
function strdt(x, y) { return vf.createLiteral(x.toString(), iri(y)); }
function strlang(x, y) { return vf.createLiteral(x.toString(), y.toString()); }
function uuid() { return vf.createURI("urn:uuid:" + java.util.UUID.randomUUID().toString()); }
function struuid() { return java.util.UUID.randomUUID().toString(); }

// SPARQL functions for manipulating strings
function strlen(x) {
	if (x == null || x instanceof Resource) throw "not a literal: " + x;
	return x.toString().length(); }
function substr(x, y, z) {
	if (x == null || x instanceof Resource) throw "not a literal: " + x;
	if (typeof z == 'undefined') return x.toString().substring(integer(y));
	return x.toString().substring(integer(y), integer(z)); }
function ucase(x) {
	if (x == null || x instanceof Resource) throw "not a literal: " + x;
	return x.toString().toUpperCase(); }
function lcase(x) {
	if (x == null || x instanceof Resource) throw "not a literal: " + x;
	return x.toString().toLowerCase(); }
function strstarts(x, y) {
	if (x == null || x instanceof Resource) throw "not a literal: " + x;
	if (y == null || y instanceof Resource) throw "not a literal: " + y;
	return x.toString().startsWith(y.toString()); }
function strends(x, y) {
	if (x == null || x instanceof Resource) throw "not a literal: " + x;
	if (y == null || y instanceof Resource) throw "not a literal: " + y;
	return x.toString().endsWith(y.toString()); }
function contains(x, y) {
	if (x == null || x instanceof Resource) throw "not a literal: " + x;
	if (y == null || y instanceof Resource) throw "not a literal: " + y;
	return x.toString().contains(y.toString()); }
function strbefore(x, y) {
	if (x == null || x instanceof Resource) throw "not a literal: " + x;
	if (y == null || y instanceof Resource) throw "not a literal: " + y;
	var index = x.toString().indexOf(y.toString());
	return index <= 0 ? "" : x.toString().substring(0, index); }
function strafter(x, y) {
	if (x == null || x instanceof Resource) throw "not a literal: " + x;
	if (y == null || y instanceof Resource) throw "not a literal: " + y;
	var index = x.toString().indexOf(y.toString());
	return index <= 0 ? "" : x.toString().substring(index + y.toString.length()); }
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
function integer(x) {
	if (x instanceof Resource) throw "not a literal: " + x;
	if (x instanceof Literal) return x.intValue();
	return Integer.parseInt(x.toString());
}
function dt(x) {}
