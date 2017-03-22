(function (collection, indexes) {
    for (var i = 0; i < indexes.length; i++) collection.createIndex(indexes[i]);
})(db.claims_837, [
    {"acn": 1},
    {"sendDate": -1}
]);