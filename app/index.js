const hbase = require('hbase');
const client = hbase({
    host: '127.0.0.1',
    port: 8080
  });

client.table('seb-mat-hashtagsTriplets' ).row('row1').get('hashtags', function(err, cell) 
  {
    console.log(cell);
    console.log();
  }
);
