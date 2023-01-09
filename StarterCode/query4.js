
// query 4: find user pairs (A,B) that meet the following constraints:
// i) user A is male and user B is female
// ii) their Year_Of_Birth difference is less than year_diff
// iii) user A and B are not friends
// iv) user A and B are from the same hometown city
// The following is the schema for output pairs:
// [
//      [user_id1, user_id2],
//      [user_id1, user_id3],
//      [user_id4, user_id2],
//      ...
//  ]
// user_id is the field from the users collection. Do not use the _id field in users.
  
function suggest_friends(year_diff, dbname) {
    db = db.getSiblingDB(dbname);
    var pairs = [];
    // TODO: implement suggest friends
    // Return an array of arrays.
    // db.users.aggregate([ {$match: {gender: "male"}}, 
    // 				    {$lookup:{ 	from: "users", 
    //  							  	let: {user_B: "$user_id", yob_B: "$YOB", friends_B: "$friends", gender: "$gender", city: "$current.city"},
    //  								pipeline: [
    //  									{ $match: 
    //  										{ $expr:
    //  											{ $and : 
    //  												[
    //  													{$not: {$eq: ["$$gender", "$gender"]}},
    //  													{$gt: [year_diff, {$abs: {$subtract: ["$YOB", "$$yob_B"]}}]},
    //  													{$not: {$in: ["$$user_B", "$friends"]}},
    //  													{$not: {$in: ["$user_id", "$$friends_B"]}},
    //  													{$eq: ["$current.city", "$$city"]}
    //  												]
    //  											}
    //  										}
    //  									},
    //  									{ $project: {_id: 0, user_id: 1}}
    //  								],
    //  								as: "user_B"
    //  							}
    //  					},
    //  					{$out: "result"}
    //  				]);
    db.users.aggregate([{$project: {_id :0, user_id :1, friends :1}}, {$unwind: "$friends"}, {$out: "flat_users"}]);
    db.users.find().forEach(function(A) {
    	if (A.gender == "male") {
    		db.users.find().forEach(function(B) {
    			if (B.gender == "female") {
    				if (Math.abs(A.YOB - B.YOB) < year_diff) {
    					if (A.hometown.city == B.hometown.city) {
    						if (A.user_id < B.user_id) {
    							if (!db.flat_users.find({"user_id":A.user_id, "friends":B.user_id}).limit(1).size()) {
    								var pair = [];
									pair.push(A.user_id);
									pair.push(B.user_id);
									pairs.push(pair);
								}
							}
							if (B.user_id < A.user_id) {
    							if (!db.flat_users.find({"user_id":B.user_id, "friends":A.user_id}).limit(1).size()) {
    								var pair = [];
									pair.push(B.user_id);
									pair.push(A.user_id);
									pairs.push(pair);
								}
							}
    						
    					}
    				}
    			}
    		})
    	}
    })
    


    return pairs;
}
