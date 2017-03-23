(function (collection, indexes) {
    for (var i = 0; i < indexes.length; i++) collection.createIndex(indexes[i]);
})(db.claims_new_835, [
    {"acn": 1},
    {"procDate": -1},
    {"svc.cpt": 1},
    {"svc.adj.adjReason": 1},
    {"prn": 1},
    {"status": 1},
    {"clmAsk": 1},
    {"sys": 1},
    {"svc.cptAsk": 1},
    {"ref": 1}
]);