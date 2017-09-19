(function (collection, indexes) {
    for (var i = 0; i < indexes.length; i++) collection.createIndex(indexes[i]);
})(db.claims_835c, [
    {"acn": 1},
    {"sys": 1},
    {"ref": 1},
    {"procDate": -1},
    {"prn": 1},
    {"prid": 1},
    {"status": 1},
    {"clmAsk": 1},
    {"svc.cpt": 1},
    {"svc.cptAsk": 1},
    {"svc.adj.adjGrp": 1},
    {"svc.adj.adjReason": 1},
    {"clmPayTotal": 1},
    {"clmPay": 1},
    {"ptnId": 1},
    // +837
    {"sendDate": -1},
    {"dx": 1},
    {"npi": 1},
    {"prn": 1, "svc.cpt": 1}
]);
