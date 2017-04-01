db.apiJobs.remove({_id: 'debug'});
db.apiJobs.insertOne({
    _id: 'debug',
    type: 'Import',
    status: 'NEW',
    options: {
        sourceDir: '/claim-db/sourceFiles',
        after: ISODate('2014-01-01'),
        claimsCollectionFormat: 'claims_new_%s_debug'
    }
});
