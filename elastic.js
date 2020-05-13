const { Client } = require('@elastic/elasticsearch')
const client = new Client({ node: 'http://localhost:9200' })

client.search({
    index: 'my_locations',
    type: '_doc',
    body: {
        "query": {
            "bool" : {
                "must" : {
                    "match_all" : {}
                },
                "filter" : {
                    "geo_distance" : {
                        "distance" : "100000km",
                        "pin.location" : {
                            "lat" : 21.0228571,
                            "lon" : 105.793895
                        }
                    }
                }
            }
        }
    }
}).then(function (resp) {
    let rs = resp.body.hits.hits.map(hit => hit._source.pin.location)
    console.log(rs)
}).catch(function (error) {
    console.log(error);
});
