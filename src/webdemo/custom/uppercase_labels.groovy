def transform(quad, handler) {
    if (isiri(subj(quad)) && pred(quad) == rdfs:label) {
        label = str(obj(quad));
        emit(handler, subj(quad), rdfs:label, ucase(label), ctx(quad));
    } else {
        emit(handler, quad);
    }
}