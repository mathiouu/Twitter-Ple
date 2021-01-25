const hbase = require('hbase');
const client = hbase({
  host: '127.0.0.1',
  port: 8080
});

client
  .table('seb-mat-tophashtagsbyday')
  .scan({
    filter: {
      "op": "EQUAL",
      "type": "RowFilter",
      "comparator": { "value": "03_03_2020-.+", "type": "RegexStringComparator" }
    }
  }, (error, cells) => {
    console.log(cells);
  });