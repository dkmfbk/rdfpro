import org.openrdf.model.Value;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.BNode;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.*;

// Global objects
vf = ValueFactoryImpl.getInstance();

// Custom functions for dealing with broken equality
def eq(x,y) { return x == null && y == null || x != null && x.equals(y); }

// Custom utility functions for dealing with Sesame model objects
def subj(x) { return x.getSubject(); }
def pred(x) { return x.getPredicate(); }
def obj(x)  { return x.getObject(); }
def ctx(x)  { return x.getContext(); }
def quad(s, p, o) {
    ov = o instanceof Value ? o : vf.createLiteral(o.toString());
    return vf.createStatement(s, p, ov); }
def quad(s, p, o, c) {
    ov = o instanceof Value ? o : vf.createLiteral(o.toString());
    return vf.createStatement(s, p, ov, c); }
def emit(h, q) {
    h.handleStatement(q); }
def emit(h, s, p, o, c) {
    h.handleStatement(quad(s, p, o, c)); }

// SPARQL functions for classifying nodes
def isiri(x) { return x instanceof URI; }
def isblank(x) { return x instanceof BNode; }
def isliteral(x) { return x instanceof Literal; }
def isnumeric(x) { return x instanceof Literal && x.getDatatype() != null &&
    org.openrdf.model.datatypes.XMLDatatypeUtil.isNumericDatatype(x.getDatatype()); }

def str(x) {
    if (x instanceof BNode) throw "str() called with bnode " + x;
    return x instanceof Value ? x.stringValue() : x.toString(); }
def ucase(x) {
    if (x == null || x instanceof Resource) throw new IllegalArgumentException("not a literal: " + x);
    return str(x).toUpperCase(); }
    