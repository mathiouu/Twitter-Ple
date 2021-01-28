const hbase = require('hbase');
const client = hbase({
  host: '127.0.0.1',
  port: 8080
});




exports.getTopKHashtags = (req, res) => {
  const { day, start, end } = req.query;
  console.log(req.query);
  if (day !== 'all') {
    client
      .table('seb-mat-tophashtagsbyday')
      .row(`${day}*`).get('hashtags', function (err, cell) {
        let mapTopKHashtagsByDay = new Map();
        cell.forEach(line => {
          if (line.column === 'hashtags:hashtags') {
            let value = mapTopKHashtagsByDay.get(line.key);
            if (value === undefined) {
              value = { hashtag: line['$'] }
            }
            else {
              value.hashtag = line['$'];
            }
            mapTopKHashtagsByDay.set(line.key, value);
          }
          if (line.column === 'hashtags:times') {
            let value = mapTopKHashtagsByDay.get(line.key);
            if (value === undefined) {
              value = { times: line['$'] }
            }
            else {
              value.times = line['$'];
            }
            mapTopKHashtagsByDay.set(line.key, value);
          }
        });
        let list = [];
        for (let i = start - 1; i < end; i++) {
          const key = `${day}-${i}`;
          list.push(mapTopKHashtagsByDay.get(key));
        }
        res.json(list);
      })
  }
  else {
    client.table('seb-mat-topkhashtags').row('row*').get('hashtags', function (err, cell) {
      let mapHashtags = new Map();
      cell.forEach(line => {
        if (line.column === 'hashtags:hashtag') {
          let value = mapHashtags.get(line.key);
          if (value === undefined) {
            value = { hashtag: line['$'] }
          }
          else {
            value.hashtag = line['$'];
          }
          mapHashtags.set(line.key, value);
        }
        if (line.column === 'hashtags:times') {
          let value = mapHashtags.get(line.key);

          value.times = line['$'];
          mapHashtags.set(line.key, value);
        }
      });
      let list = [];
      for(let i = start-1 ; i < end; i ++){
        list.push(mapHashtags.get(`row${i}`));
      }
      res.json(list);
    });


   
  }
};

exports.getHashtag = (req, res) => {
  const {hashtag} = req.query;
  console.log(hashtag);
  client
    .table('seb-mat-hashtags_by_users')
    .scan({
      filter: {
        "op": "EQUAL",
        "type": "RowFilter",
        "comparator": { "value": `${hashtag}.*`, "type": "RegexStringComparator" }
      }
    }, (error, cells) => {
      let mapHashtags= new Map();
      cells.forEach(line => {
        if (line.column === 'hashtags:times') {
          let value = mapHashtags.get(line.key);
          if (value === undefined) {
            value = { times: line['$'] }
          }
          else {
            value.times = line['$'];
          }
          mapHashtags.set(line.key, value);
        }
        if (line.column === 'hashtags:users') {
          let value = mapHashtags.get(line.key);

          value.users = line['$'];
          mapHashtags.set(line.key, value);
        }
      });
      let list =[]; 
      mapHashtags.forEach((value,key)=>{
        list.push({hashtag: key, infos:value});
      });
      res.json(list);
      
    });
}