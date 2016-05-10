# api_updater_linux
Linux version of my api_updater for both HH.com and ForceRC.com

#### Installation
```
npm install
```
#### Execute the app
```
node nightlyProcess
```
##### Or run the steps separately
```
node step1

node step2

node step3

node step4
```

## Hints:

After initial run - future runs will be much quicker if you create a unique index on ProdID
```
mongo
```

followed by:

```
use hhproducts

db.products.createIndex( { "ProdID": 1 }, { unique: true } )

db.forceproducts.createIndex( { "ProdID": 1 }, { unique: true } )
```

# License

The MIT License (MIT)

Copyright (c) 2015 Wayne Patterson

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
