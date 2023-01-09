// query 8: Find the city average friend count per user using MapReduce
// Using the same terminology in query6, we are asking you to write the mapper,
// reducer and finalizer to find the average friend count for each city.


var city_average_friendcount_mapper = function() {
  // implement the Map function of average friend count
  emit(this.hometown.city, {count: 1, x: this.friends.length});
};

var city_average_friendcount_reducer = function(key, values) {
  // implement the reduce function of average friend count
  var pair = {count: 0, x: 0};
  for (var idx = 0; idx < values.length; idx++) {
  	pair.count = pair.count + values[idx].count;
  	pair.x = pair.x + values[idx].x;
  }
  return pair;
};

var city_average_friendcount_finalizer = function(key, reduceVal) {
  // We've implemented a simple forwarding finalize function. This implementation 
  // is naive: it just forwards the reduceVal to the output collection.
  // Feel free to change it if needed.
  var red = reduceVal.x / reduceVal.count;
  return red;
}
