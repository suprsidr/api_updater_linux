/**
 * @author wpatterson
 * Step2 coalesces the data collected in step1 does final cleanup and inserts into local MongoDB
 */
var ItemProvider = require('./itemprovider-mongodb').ItemProvider,
    itemProvider = new ItemProvider(),
    path = require('path'),
    LOCALAPPDATA = path.join(process.env.HOME, 'api_updater'),
    _ = require('lodash'),
    merge = require('merge'),
    hasPartsListing = require(path.join(LOCALAPPDATA, './HasPartsListing')).HasPartsListing,
    demandRank = require(path.join(LOCALAPPDATA, './DemandRank')).DemandRank,
    price = require(path.join(LOCALAPPDATA, './Price')).Price,
    listPrice = require(path.join(LOCALAPPDATA, './ListPrice')).ListPrice,
    partsList = require(path.join(LOCALAPPDATA, './PartsList')).PartsList,
    attributes = require(path.join(LOCALAPPDATA, './Attributes')).Attributes,
    categories = require(path.join(LOCALAPPDATA, './Categories')).Categories,
    completionGuides = require(path.join(LOCALAPPDATA, './CompletionGuides')).CompletionGuides,
    data = require(path.join(LOCALAPPDATA, './Products')).Products,
    timestamp = Date.now();

console.log = function(d) {
  process.stdout.write(d + '\n');
};

// a little data cleanup/formatting
Array.prototype.clean=function(t){for(var r=0;r<this.length;r++)this[r]==t&&(this.splice(r,1),r--);return this};
for(var i = 0;i<data.length;i++) {

  if(data[i].Keywords) {
    data[i].Keywords = data[i].Keywords.replace(/ /g, '').split(',').clean('');
  }

  data[i]['LastUpdate'] = timestamp;
  data[i]['HasPartsListing'] = hasPartsListing.indexOf(data[i].ProdID) > -1 ? 1 : 0;
  data[i]['DemandRank'] = demandRank[data[i].ProdID] || 0;
  data[i]['Price'] = price[data[i].ProdID] || 0;
  data[i]['ListPrice'] = listPrice[data[i].ProdID] || 0;
  data[i]['PartsList'] = partsList[data[i].ProdID] || null;
  data[i]['Categories'] = categories[data[i].ProdID] || null;
  data[i]['CompletionGuides'] = completionGuides[data[i].ProdID] || null;
  data[i]['Attributes'] = attributes[data[i].ProdID] || null;

  //if(i == data.length - 1) {process.exit()}
}

var i = 0, l = data.length;

// iterate over our product data and insert into MongoDB
function saveToDb() {
  itemProvider.findOne({collection: 'products', query: {ProdID: data[i].ProdID}}, function(err, item) {
    if(err){
      console.log(err);
      return i<data.length-1?(i++,void saveToDb()):void process.exit();
    }
    if(item === null) {
      itemProvider.save({collection: 'products'}, data[i], function(err) {
        if(err) console.log(err, data[i]);
        console.log('saved: ' + data[i].ProdID, i + '/' + l);
        return i<data.length-1?(i++,void saveToDb()):void process.exit();
      });
    } else {
      // document exists, lets update it.
      var merged = merge.recursive(true, item, data[i]);
      delete merged._id;
      itemProvider.updateItem(
          {
            collection: 'products',
            query: {
              ProdID: data[i].ProdID
            },
            action: {
              '$set' : merged
            }
          }, function(err) {
            if(err) console.log(err, data[i]);
            console.log('updated: ' + data[i].ProdID, i + '/' + l);
            return i<data.length-1?(i++,void saveToDb()):void process.exit();
          });
    }
  });
}

/**
 * Get type of variable
 * @param mixed input
 * @return string
 *
 * @see http://jsperf.com/typeofvar
 */
function typeOf(t){var e={}.toString.call(t);return"[object Object]"===e?"object":"[object Array]"===e?"array":"[object String]"===e?"string":"[object Number]"===e?"number":"[object Function]"===e?"function":"[object Null]"===e?"null":"undefined"}
Array.prototype.unique=function(){for(var r=this.concat(),t=0;t<r.length;++t)for(var n=t+1;n<r.length;++n)r[t]===r[n]&&r.splice(n--,1);return r};

// open DB -> insert items into DB
itemProvider.open(function(){
  
});
// open DB -> remove all items -> insert items into DB
itemProvider.open(function(){
  itemProvider.deleteItem({collection: 'products', query: {}}, function(err, item) {
    if(err) {
      console.log(err);
    } else {
      saveToDb();
    }
  });
});
