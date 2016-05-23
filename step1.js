/**
 * @author wpatterson
 * Collect product data for coalescence in step2
 * nine queries all pretty self explanatory
 * each query maps the data in a predictable way for Document storage in MongoDB
 */
var ibmdb = require('ibm_db'),
    path = require('path'),
    fs = require('fs');
fs.exists(path.join(process.env.HOME, 'api_updater'), function(exists) {
  if(!exists) {
    fs.mkdirSync(path.join(process.env.HOME, 'api_updater'));
  }
});
    LOCALAPPDATA = path.join(process.env.HOME, 'api_updater'),
    _ = require('lodash'),
    async = require('async'),
    file = require('./file'),
    categories = require('./CatMaps').AllowedCategories,
    catMap = require('./CatMaps').CatMap,
    catArr = Object.keys(catMap),
    fs = require('fs');

// pass console.log to standard out
console.log = function(msg) {
  process.stdout.write(msg + '\n');
};

function getHasPartsListing(cb) {
  console.log('Querying for HasPartsListing');
  ibmdb.open("DRIVER={DB2};DATABASE=STG01DB;HOSTNAME=cmp02-ws-stg-db01;UID=wcdbuser;PWD=h0r1z0n;PORT=50000;PROTOCOL=TCPIP", function (err,conn) {
    if (err) cb(err);
    var q = "select distinct (b.mfpartnumber) from massoccece as a inner join catentry as b on a.catentry_id_from = b.catentry_id inner join catentdesc as c on b.catentry_id = c.catentry_id where a.MASSOCTYPE_ID in ('PART_LISTING') and b.field4 in ('3DS','BLH','BTX','CLN','DYN','ECX','EFL','EVO','FMM','FUG','FPV','FSV','GWS','HAN','HBZ','IBS','INT','KXS','IRL','LOS','MPU','PKZ','PRB','PRO','PTA','RC4','RTM','SAI','SEA','SPM','STX','TAM','TLR','TRA','VNR','VTR','YUN','ZEN') order by b.mfpartnumber asc with ur";
    conn.query(q, [], function (err, data) {
      if (err) {
        cb(err);
      }

      //console.log(data);
      var data = _.map(data, function(a) {
        return a.MFPARTNUMBER
      });
      file.save(path.join(LOCALAPPDATA, './HasPartsListing.js'), 'exports.HasPartsListing = ' + JSON.stringify(data), function(err) {
        if(err) cb(err);
        conn.close(function () {
          console.log('done');
          cb(null, 'step 1 done');
        });
      });
    });
  });
}

function getPrice(cb) {
  console.log('Querying for Price');
  ibmdb.open("DRIVER={DB2};DATABASE=STG01DB;HOSTNAME=cmp02-ws-stg-db01;UID=wcdbuser;PWD=h0r1z0n;PORT=50000;PROTOCOL=TCPIP", function (err,conn) {
    if (err) cb(err);
    var q = "select a.mfpartnumber as \"ProdID\", c.price as \"Price\" from catentry as a inner join offer as b on a.catentry_id = b.catentry_id inner join offerprice as c on b.offer_id = c.offer_id where a.catenttype_id = 'ProductBean' and b.tradeposcn_id = 4000000000000000001 and b.precedence = 15.0 and a.buyable = 1 and a.field4 in ('3DS','BLH','BTX','CLN','DYN','ECX','EFL','EVO','FMM','FUG','FPV','FSV','GWS','HAN','HBZ','IBS','INT','KXS','IRL','LOS','MPU','PKZ','PRB','PRO','PTA','RC4','RTM','SAI','SEA','SPM','STX','TAM','TLR','TRA','VNR','VTR','YUN','ZEN','HPS') order by a.mfpartnumber asc with ur";
    conn.query(q, [], function (err, data) {
      if (err) cb(err);

      //console.log(data);
      var mapped = {};
      _.each(data, function(a) {
        mapped[a.ProdID] = parseFloat(a.Price);
      })
      file.save(path.join(LOCALAPPDATA, './Price.js'), 'exports.Price = ' + JSON.stringify(mapped), function(err) {
        if(err) cb(err);
        conn.close(function () {
          console.log('done');
          cb(null, 'step 2 done');
        });
      });
    });
  });
}

function getListPrice(cb) {
  console.log('Querying for ListPrice');
  ibmdb.open("DRIVER={DB2};DATABASE=STG01DB;HOSTNAME=cmp02-ws-stg-db01;UID=wcdbuser;PWD=h0r1z0n;PORT=50000;PROTOCOL=TCPIP", function (err,conn) {
    if (err) cb(err);
    var q = "select a.mfpartnumber as \"ProdID\", c.price as \"ListPrice\" from catentry as a inner join offer as b on a.catentry_id = b.catentry_id inner join offerprice as c on b.offer_id = c.offer_id where a.catenttype_id = 'ProductBean' and b.tradeposcn_id = 4000000000000000002 and b.precedence = 15.0 and a.buyable = 1 and a.field4 in ('3DS','BLH','BTX','CLN','DYN','ECX','EFL','EVO','FMM','FUG','FPV','FSV','GWS','HAN','HBZ','IBS','INT','KXS','IRL','LOS','MPU','PKZ','PRB','PRO','PTA','RC4','RTM','SAI','SEA','SPM','STX','TAM','TLR','TRA','VNR','VTR','YUN','ZEN','HPS') order by a.mfpartnumber asc with ur";
    conn.query(q, [], function (err, data) {
      if (err) cb(err);

      //console.log(data);
      var mapped = {};
      _.each(data, function(a) {
        mapped[a.ProdID] = parseFloat(a.ListPrice);
      })
      file.save(path.join(LOCALAPPDATA, './ListPrice.js'), 'exports.ListPrice = ' + JSON.stringify(mapped), function(err) {
        if(err) cb(err);
        conn.close(function () {
          console.log('done');
          cb(null, 'step 3 done');
        });
      });
    });
  });
}

function getDemandRank(cb) {
  console.log('Querying for DemandRank');
  ibmdb.open("DRIVER={DB2};DATABASE=STG01DB;HOSTNAME=cmp02-ws-stg-db01;UID=wcdbuser;PWD=h0r1z0n;PORT=50000;PROTOCOL=TCPIP", function (err,conn) {
    if (err) cb(err);
    var q = "select a.mfpartnumber as \"ProdID\", x.totalamount as \"DemandRank\" from catentry a inner join x_bestseller x on a.catentry_id = x.catentry_id where a.catenttype_id = 'ProductBean' and a.field4 in ('3DS','BLH','BTX','CLN','DYN','ECX','EFL','EVO','FMM','FUG','FPV','FSV','GWS','HAN','HBZ','IBS','INT','KXS','IRL','LOS','MPU','PKZ','PRB','PRO','PTA','RC4','RTM','SAI','SEA','SPM','STX','TAM','TLR','TRA','VNR','VTR','YUN','ZEN') order by a.mfpartnumber asc with ur";
    conn.query(q, [], function (err, data) {
      if (err) cb(err);

      //console.log(data);
      var mapped = {};
      _.each(data, function(a) {
        mapped[a.ProdID] = parseInt(a.DemandRank);
      })
      file.save(path.join(LOCALAPPDATA, './DemandRank.js'), 'exports.DemandRank = ' + JSON.stringify(mapped), function(err) {
        if(err) cb(err);
        conn.close(function () {
          console.log('done');
          cb(null, 'step 4 done');
        });
      });
    });
  });
}

function getPartsList(cb) {
  console.log('Querying for PartsList');
  ibmdb.open("DRIVER={DB2};DATABASE=STG01DB;HOSTNAME=cmp02-ws-stg-db01;UID=wcdbuser;PWD=h0r1z0n;PORT=50000;PROTOCOL=TCPIP", function (err,conn) {
    if (err) cb(err);
    var q = "select distinct a.mfpartnumber  as \"PartNO\", c.mfpartnumber  as \"ProdID\", z.description, a.buyable \"Buyable\", e.published as \"Displayable\" from catentry as a inner join MASSOCCECE as b on a.catentry_id = b.catentry_id_to inner join catentry as c on b.catentry_id_from = c.catentry_id inner join catgpenrel as d on c.catentry_id = d.catentry_id inner join catentdesc as e on a.catentry_id = e.catentry_id inner join massoctype as z on b.massoctype_id = z.massoctype_id where a.catenttype_id = 'ProductBean' and z.description in ('Optional Accessories', 'Part Listing', 'Hop Ups') and a.field4 in ('3DS','BLH','BTX','CLN','DYN','ECX','EFL','EVO','FMM','FUG','FPV','FSV','GWS','HAN','HBZ','IBS','INT','KXS','IRL','LOS','MPU','PKZ','PRB','PRO','PTA','RC4','RTM','SAI','SEA','SPM','STX','TAM','TLR','TRA','VNR','VTR','YUN','ZEN') and d.catalog_id = '10051' and e.published = 1  order by a.mfpartnumber asc with ur;";
    conn.query(q, [], function (err, data) {
      if (err) cb(err);

      //console.log(data);
      var mapped = {};
      _.each(data, function(a) {
        try{
          mapped[a.ProdID].push(a.PartNO)
        } catch(e) {
          mapped[a.ProdID] = [a.PartNO];
        }
      });
      file.save(path.join(LOCALAPPDATA, './PartsList.js'), 'exports.PartsList = ' + JSON.stringify(mapped), function(err) {
        if(err) cb(err);
        conn.close(function () {
          console.log('done');
          cb(null, 'step 5 done');
        });
      });
    });
  });
}

function getAttributeValues(cb) {
  console.log('Querying for Attribute Values');
  ibmdb.open("DRIVER={DB2};DATABASE=STG01DB;HOSTNAME=cmp02-ws-stg-db01;UID=wcdbuser;PWD=h0r1z0n;PORT=50000;PROTOCOL=TCPIP", function (err,conn) {
    if (err) cb(err);
    var q = "select attrval_id \"ID\", value \"Value\" from ATTRVALDESC with ur;";
    conn.query(q, [], function (err, data) {
      if (err) cb(err);

      //console.log(data);
      var mapped = {};
      _.each(data, function(a) {
        mapped[a.ID] = a.Value;
      })
      file.save(path.join(LOCALAPPDATA, './AttributeValues.js'), 'exports.AttributeValues = ' + JSON.stringify(mapped), function(err) {
        if(err) cb(err);
        conn.close(function () {
          console.log('done');
          cb(null, 'step 6 done');
        });
      });
    });
  });
}

function getAttributes(cb) {
  var values = require(path.join(LOCALAPPDATA, './AttributeValues')).AttributeValues;
  console.log('Querying for Attributes');
  ibmdb.open("DRIVER={DB2};DATABASE=STG01DB;HOSTNAME=cmp02-ws-stg-db01;UID=wcdbuser;PWD=h0r1z0n;PORT=50000;PROTOCOL=TCPIP", function (err,conn) {
    if (err) cb(err);
    var q = "select a.mfpartnumber \"ProdID\",d.identifier \"SpecID\",c.identifier \"SpecName\" from catentry as a inner join catentryattr as b on a.catentry_id = b.catentry_id inner join attrval as c on b.attrval_id = c.attrval_id inner join attr as d on c.attr_id = d.attr_id where a.catenttype_id = 'ProductBean' and d.storeent_id = 10051 and a.field4 in ('3DS','BLH','BTX','CLN','DYN','ECX','EFL','EVO','FMM','FUG','FPV','FSV','GWS','HAN','HBZ','IBS','INT','KXS','IRL','LOS','MPU','PKZ','PRB','PRO','PTA','RC4','RTM','SAI','SEA','SPM','STX','TAM','TLR','TRA','VNR','VTR','YUN','ZEN') and d.identifier not in ('0','ABCCode','ProductClass','Oversize','RoadName','RoadID') order by a.mfpartnumber asc with ur;";
    conn.query(q, [], function (err, data) {
      if (err) cb(err);

      //we need to merge multiple results for same item
      var items = {};
      _.each(data, function(a) {
        if(a.SpecName.match(/^700000000/)) {
          a.SpecName = values[a.SpecName];
        }
        try {
          if(items[a.ProdID][a.SpecID]) {
            try {
              items[a.ProdID][a.SpecID].push(a.SpecName);
            } catch(e) {
              items[a.ProdID][a.SpecID] = [items[a.ProdID][a.SpecID]];
              items[a.ProdID][a.SpecID].push(a.SpecName);
            }
          } else {
            try {
              items[a.ProdID][a.SpecID] = a.SpecName;
            } catch(e) {
              items[a.ProdID] = {};
              items[a.ProdID][a.SpecID] = a.SpecName;
            }
          }
        } catch(e) {
          items[a.ProdID] = {};
          items[a.ProdID][a.SpecID] = a.SpecName;
        }
      });
      //map items for proper data structure
      var mapped = {};
      _.forIn(items, function(value, key) {
        mapped[key] = [];
        _.forIn(value, function(v, k) {
          mapped[key].push({ID: k, Name: v})
        });
      });
      file.save(path.join(LOCALAPPDATA, './Attributes.js'), 'exports.Attributes = ' + JSON.stringify(mapped), function(err) {
        if(err) cb(err);
        conn.close(function () {
          console.log('done');
          cb(null, 'step 7 done');
        });
      });
    });
  });
}

function getCategories(cb) {
  console.log('Querying for Categories');
  ibmdb.open("DRIVER={DB2};DATABASE=STG01DB;HOSTNAME=cmp02-ws-stg-db01;UID=wcdbuser;PWD=h0r1z0n;PORT=50000;PROTOCOL=TCPIP", function (err,conn) {
    if (err) cb(err);
    var q = "select a.mfpartnumber as \"ProdID\", trim(e.identifier) as \"CatID\", trim(d.name) as \"CatName\" from catentry as a inner join catgpenrel as c on a.catentry_id = c.catentry_id inner join catgrpdesc as d on c.catgroup_id = d.catgroup_id inner join catgroup as e on d.catgroup_id = e.catgroup_id where a.catenttype_id = 'ProductBean' and a.field4 in ('3DS','BLH','BTX','CLN','DYN','ECX','EFL','EVO','FMM','FUG','FPV','FSV','GWS','HAN','HBZ','IBS','INT','KXS','IRL','LOS','MPU','PKZ','PRB','PRO','PTA','RC4','RTM','SAI','SEA','SPM','STX','TAM','TLR','TRA','VNR','VTR','YUN','ZEN') and c.catalog_id = '10051' order by a.mfpartnumber asc with ur";
    conn.query(q, [], function (err, data) {
      if (err) cb(err);

      //map items for proper data structure
      var mapped = {};
      _.each(data, function(a) {
        if(catArr.indexOf(a.CatID) > -1) {
          a.CatName = catMap[a.CatID]
        }

        if(categories.indexOf(a.CatID) > -1) {
          try{
            mapped[a.ProdID].push({ID: a.CatID, Name: a.CatName});
          } catch(e) {
            mapped[a.ProdID] = [];
            mapped[a.ProdID].push({ID: a.CatID, Name: a.CatName});
          }
        }

      })
      file.save(path.join(LOCALAPPDATA, './Categories.js'), 'exports.Categories = ' + JSON.stringify(mapped), function(err) {
        if(err) cb(err);
        conn.close(function () {
          console.log('done');
          cb(null, 'step 8 done');
        });
      });
    });
  });
}

function getProducts(cb) {
  console.log('Querying for Products');
  ibmdb.open("DRIVER={DB2};DATABASE=STG01DB;HOSTNAME=cmp02-ws-stg-db01;UID=wcdbuser;PWD=h0r1z0n;PORT=50000;PROTOCOL=TCPIP", function (err,conn) {
    if (err) cb(err);
    var q = "select distinct a.mfpartnumber as \"ProdID\", a.field4 as \"BrandID\", a.mfname as \"BrandName\", trim(b.name) as \"Name\", a.buyable \"Buyable\", b.published as \"Displayable\", a.field1 as \"ProductStatus\", a.field2 as \"RatingCount\", a.field3 as \"RatingAverage\", trim(b.keyword) as \"Keywords\", trim(b.shortdescription) as \"Desc\", trim(CAST (b.longdescription as VARCHAR(10000))) as \"LongDesc\"from catentry as a inner join catentdesc as b on a.catentry_id = b.catentry_id inner join catgpenrel as c on b.catentry_id = c.catentry_id where a.catenttype_id = 'ProductBean' and a.field4 in ('3DS','BLH','BTX','CLN','DYN','ECX','EFL','EVO','FMM','FUG','FPV','FSV','GWS','HAN','HBZ','IBS','INT','KXS','IRL','LOS','MPU','PKZ','PRB','PRO','PTA','RC4','RTM','SAI','SEA','SPM','STX','TAM','TLR','TRA','VNR','VTR','YUN','ZEN') and c.catalog_id = '10051' order by a.mfpartnumber asc with ur;";
    var q2 = "select distinct a.mfpartnumber as \"ProdID\", a.field4 as \"BrandID\", a.mfname as \"BrandName\", trim(b.name) as \"Name\", a.buyable \"Buyable\", b.published as \"Displayable\", a.field1 as \"ProductStatus\", a.field2 as \"RatingCount\", a.field3 as \"RatingAverage\", trim(b.keyword) as \"Keywords\", trim(b.shortdescription) as \"Desc\", trim(CAST (b.longdescription as VARCHAR(10000))) as \"LongDesc\"from catentry as a inner join catentdesc as b on a.catentry_id = b.catentry_id inner join catgpenrel as c on b.catentry_id = c.catentry_id where a.catenttype_id = 'ProductBean' and a.mfpartnumber like 'HPS%' order by a.mfpartnumber asc with ur;";
    conn.query(q, [], function (err, data) {
      if (err) cb(err);

      conn.query(q2, [], function (err, hpsdata) {
      if (err) cb(err);

      if(hpsdata.length > 0) {
        hpsdata.forEach(function(item) {
          data.push(item);
        })
      }

      file.save(path.join(LOCALAPPDATA, './Products.js'), 'exports.Products = ' + JSON.stringify(data), function(err) {
          if(err) cb(err);

          conn.close(function () {
            fs.stat(path.join(LOCALAPPDATA, './Products.js'), function(e, f) {
              if (f.size === 0) {
                console.log('Products.js filesize is 0, retrying query');
                getProducts(cb);
              } else {
                console.log('done');
                cb(null, 'step 9 done');
              }
            });
          });
        });
      });
    });
  });
}

// run queries in series
async.series([
  getHasPartsListing,
  getPrice,
  getListPrice,
  getDemandRank,
  getPartsList,
  getAttributeValues,
  getAttributes,
  getCategories,
  getProducts
], function(err, res){
  err && process.exit(err);
  process.exit();
});


Array.prototype.unique=function(){for(var r=this.concat(),t=0;t<r.length;++t)for(var n=t+1;n<r.length;++n)r[t]===r[n]&&r.splice(n--,1);return r};
