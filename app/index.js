const hbase = require('hbase');
const client = hbase({
    host: '127.0.0.1',
    port: 8080
  });

const containsUser = ([k,v]) =>{
  return v.users.some((tuple)=> tuple._1 === "x03040x" ) ;
};
let mapUser = new Map();

client.table('testSmelezan' ).row('row*').get('hashtags', function(err, cell) 
  {
    cell.forEach(line =>{
      let currentHashtag ="";
      if(line.column=== 'hashtags:hashtags'){
        let value = mapUser.get(line.key);
        if(value===undefined){
          value = {hashtags: line['$']}
        }
        else{
          value.hashtags = line['$'];
        }
        mapUser.set(line.key, value);
      }
      if(line.column=== 'hashtags:users'){
        let value = mapUser.get(line.key);
        let tmp = line['$'];
        value.users = JSON.parse(tmp);
        mapUser.set(line.key, value);
      }
    });
    const res = [...mapUser].filter(containsUser);
    res.forEach(([k,v]) => {
      console.log(v.users);
    })
    console.log();
  }
);
