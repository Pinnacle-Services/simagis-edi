db.apiJobs.remove({_id: 'debug'});
db.apiJobs.insertOne({
    _id: 'debug',
    type: 'Debug',
    status: 'RUNNING',
    options: {
        sourceDir: '/claim-db/sourceFiles',
        after: ISODate('2014-01-01'),
        claimTypes: {
            "835": {temp: "claims_835.temp", target: "claims_835", createIndexes: true},
            "837": {temp: "claims_837.temp", target: "claims_837", createIndexes: true}
        },
        archive: {
            "835a": {temp: "claims_835a.temp", target: "claims_835a.target", createIndexes: true},
            "837a": {temp: "claims_837a.temp", target: "claims_837a.target", createIndexes: true}
        },
        build835c: {
            "835c": {temp: "claims_835c.temp", target: "claims_835c.target", createIndexes: true},
            clients: "clientid"
        }
    }
});
