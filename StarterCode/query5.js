// find the oldest friend for each user who has a friend. 
// For simplicity, use only year of birth to determine age, if there is a tie, use the one with smallest user_id
// return a javascript object : key is the user_id and the value is the oldest_friend id
// You may find query 2 and query 3 helpful. You can create selections if you want. Do not modify users collection.
//
//You should return something like this:(order does not matter)
//{user1:userx1, user2:userx2, user3:userx3,...}

function oldest_friend(dbname){
  db = db.getSiblingDB(dbname);
  var result = {};
  var years = {};
  var age = {};
  for (var i = 0; i < 800; i ++) {
  	result[i] = -1;
  	years[i] = -1;
  }

  // TODO: implement oldest friends
  // return an javascript object described above
  db.users.aggregate([{$project: {_id :0, user_id :1, friends :1}}, {$unwind: "$friends"}, {$out: "flat_users"}]);
  db.users.aggregate([{$project: {_id :0, user_id :1, YOB :1}}, {$out: "user_year"}]);


  db.user_year.find().forEach(function(A) {
  	age[A.user_id] = A.YOB
  })

  db.flat_users.find().forEach(function(A) {
  
  	var A_age = age[A.user_id];
  	var B_age = age[A.friends];

  	// B as A's friend
  	if (result[A.user_id] == -1 || years[A.user_id] > B_age) {
  		result[A.user_id] = A.friends;
  		years[A.user_id] = B_age;
  	}

  	if (years[A.user_id] == B_age && A.friends < result[A.user_id]) {
  		result[A.user_id] = A.friends;
  	}

  	// A as B's friend
  	if (result[A.friends] == -1 || years[A.friends] > A_age) {
  		result[A.friends] = A.user_id;
  		years[A.friends] = A_age;
  	}

  	if (years[A.friends] == A_age && A.user_id < result[A.friends]) {
  		result[A.friends] = A.user_id;
  	}

  })

  results = {};
  for (var i = 0; i < 800; i ++) {
  	if (result[i] != -1) {
  		results[i] = result[i];
  	}
  }



  return results;
}
