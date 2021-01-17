const hbase = require('hbase');
const client = hbase({
    host: '127.0.0.1',
    port: 8080
  });

// const containsUser = ([k,v]) =>{
//   return v.users.some((tuple)=> tuple._1 === "x03040x" ) ;
// };
// let mapHashtags = new Map();

let mapHashtags = new Map();
client.table('testSmelezan' ).row('row*').get('hashtags', function(err, cell) 
  {
    cell.forEach(line =>{
      if(line.column=== 'hashtags:hashtag'){
        let value = mapHashtags.get(line.key);
        if(value===undefined){
          value = {hashtag: line['$']}
        }
        else{
          value.hashtag = line['$'];
        }
        mapHashtags.set(line.key, value);
      }
      if(line.column=== 'hashtags:times'){
        let value = mapHashtags.get(line.key);
        
        value.times =  line['$'];
        mapHashtags.set(line.key, value);
      }
    });
    
    //res = [...mapHashtags].slice(0,10);
  }
);

exports.getTopKHashtags = (start,end)=>{
  let res = [];
    for(let i = start-1 ; i < end; i ++){
      res.push(mapHashtags.get(`row${i}`));
    }

    return res;
};
