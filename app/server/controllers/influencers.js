const hbase = require('hbase');
const client = hbase({
  host: '127.0.0.1',
  port: 8080
});




exports.getInfluencers = (req, res) => {
    const {hashtags} = req.query;
    console.log(hashtags);
    client
    .table('seb-mat-topkinfluencers')
    .scan({
    }, (error, cells) => {
        let mapHashtags= new Map();
      cells.forEach(line => {
        if (line.column === 'users:infos') {
          let value = mapHashtags.get(line.key);
          if (value === undefined) {
            value = { infos: line['$'] }
          }
          mapHashtags.set(line.key, value);
        }
      });
      let list =[]; 
      
      mapHashtags.forEach((value,key)=>{
        list.push({user: key, infos:value});
      });
      res.json(list);
      
    });
};

exports.getTriplets = (req, res) => {
    const {hashtags} = req.query;
    console.log(hashtags);
    client
    .table('seb-mat-triplets')
    .scan({
        filter: {
            "op": "EQUAL",
            "type": "RowFilter",
            "comparator": { "value": `.*${hashtags}.*`, "type": "RegexStringComparator" }
          }
    }, (error, cells) => {
        let mapHashtags= new Map();
      cells.forEach(line => {
        if (line.column === 'users:infos') {
          let value = mapHashtags.get(line.key);
          if (value === undefined) {
            value = { infos: line['$'] }
          }
          mapHashtags.set(line.key, value);
        }
      });
      let list =[]; 
      let i =0;
      for(let entry of mapHashtags.entries()){
        list.push({hashtags: entry[0], infos: entry[1]});
        if (i===20) break
        i++;
      }
    //   mapHashtags.forEach((value,key)=>{
    //     list.push({hashtags: key, infos:value});
    //   });
    console.log(list);
      res.json(list);
      
    });
};