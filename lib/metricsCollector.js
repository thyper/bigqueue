var log = require("winston"),
    fs = require("fs")
function MetricCollector(metricName, buckets, evalFunction, statsFile, saveInterval){
    var self = this
    this.metricName = metricName
    this.collector = {"total":0}
    this.buckets = ["total"].concat(buckets)
    this.evalFunction = evalFunction
    this.statsFile = statsFile || "/tmp/bigqueueStats.log"
    this.saveInterval = saveInterval || -1
    if(this.saveInterval > 0){
        this.interval = setInterval(function(){
            self.saveMetrics()
        },this.saveInterval)
    }
    for(var i in this.buckets)
        this.collector[this.buckets[i]] = 0
}

MetricCollector.prototype.collect = function(val){
    try{
        this.collector["total"]++
        var bucket = this.evalFunction(val)
        this.collector[bucket]++
    }catch(e){
        log.error(e)
    }
}

MetricCollector.prototype.reset = function(){
    for(var i in this.collector)
        this.collector[i] = 0
}

MetricCollector.prototype.stats = function(){
    return this.collector
}

MetricCollector.prototype.getTime = function(){
    return new Date().getTime() 
}

MetricCollector.prototype.saveMetrics = function(){
    var logLine = this.getTime()+"\t"+this.metricName+"\t"
    for(var i in this.buckets){
        logLine += this.collector[this.buckets[i]]+"\t"
    }
    this.reset()
    this.getStatsFile(function(file){
        file.write(logLine+"\n")
    })
}


MetricCollector.prototype.getStatsFile = function(cb){
    var self = this
    if(!this.file){
        this.file = fs.createWriteStream(this.statsFile,{
            "flags":'a'
        })
    }else{
        cb(this.file)
    }
}

function Timer(){
    this.start = new Date().getTime()
}
Timer.prototype.lap = function(){
    return new Date().getTime() - this.start
}

MetricCollector.prototype.timer = function(){
    return new Timer()
}

exports.createCollector = function(metricName,buckets, evalFunction,statsFile,statsInterval){
    return new MetricCollector(metricName,buckets,evalFunction,statsFile,statsInterval)
}


