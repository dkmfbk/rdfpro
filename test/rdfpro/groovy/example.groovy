def init(args) { println "Initialized with " + args; }
def start(x)   { println "Begin pass " + x; num = 0 }
def end(x)     { println "End pass " + x + ", " + num + " triples"; emit(iri("ex:stats"), iri("ex:numtriples"), num, null); }

emitIf(p == rdf:type);
++ num;
