import org.openrdf.model.*;
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
    