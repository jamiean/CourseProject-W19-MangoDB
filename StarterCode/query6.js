// query6 : Find the Average friend count per user for users
//
// Return a decimal variable as the average user friend count of all users
// in the users document.

function find_average_friendcount(dbname){
  db = db.getSiblingDB(dbname)
  // TODO: return a decimal number of average friend count
  db.users.aggregate([{$project: {_id :0, user_id :1, friends :1}}, {$unwind: "$friends"}, {$out: "flat_users"}]);
  return db.flat_users.count()/db.users.count();
}
