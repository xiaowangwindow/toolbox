// get all fields of collection from mongoDB
var map = function() {
    var join_key = function(pre_key, key){
        if (pre_key.length==0) {return key;}
        else {return pre_key+'.'+key;}
    }
    var add_key = function(key_dict, key) {
        if (key in key_dict){key_dict[key] += 1}
        else {key_dict[key] = 1}
    }
    var get_key = function(pre_key, obj, key_dict){
        if (obj instanceof ObjectId) {
            add_key(key_dict, pre_key)
        }else if (obj instanceof Array){
            for (var i=0, len=obj.length; i<len; i++){
                get_key(join_key(pre_key, '[]'), obj[i], key_dict);
            }
        }else if (obj instanceof Object){
            for (k in obj) {
                get_key(join_key(pre_key, k), obj[k], key_dict)
            }
        }else{
            add_key(key_dict, pre_key)
        }
    }

    var key_dict = {}
    get_key('', this, key_dict)
    for (key in key_dict){
        emit(key, key_dict[key]);
    }
};

var reduce = function(key, value) {
    print(value)
    return Array.sum(value)
}

db.runCommand({
    "mapreduce" : "europages-company",
    "map" : map,
    "reduce" : reduce,
    "limit":100000, //　too large data will cause execute timeout
    "out": {inline: 1} // print to console
    // "out": "field_analysis", // save to collection
})
